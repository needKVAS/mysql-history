import MySQLdb
import uuid
from collections import namedtuple
import sys
import re
import json


def table_names(config):
    config.cursor.execute(
        '''SELECT table_name FROM information_schema.tables
                             WHERE table_schema=%s
                             AND table_name not like %s
                             AND TABLE_TYPE="BASE TABLE"''',
        (config.database, history_table_template(config)),
    )
    names = [i["TABLE_NAME"] for i in cursor]
    includes = config.includes
    if includes:
        names = [
            name for name in names if any([re.match(incl, name) for incl in includes])
        ]
    excludes = config.excludes
    if excludes:
        names = [
            name
            for name in names
            if not any([re.match(excl, name) for excl in excludes])
        ]
    return names


def table_exists(config, table_name):
    return config.cursor.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_name = %s
        AND table_schema = %s""",
        (table_name, config.database),
    )


def columns(config, table_name):
    columns_query = """SELECT column_name, column_type, character_set_name, collation_name, column_default
                        FROM information_schema.columns
                        WHERE table_schema=%s
                        AND table_name=%s
                        ORDER BY ORDINAL_POSITION ASC"""
    config.cursor.execute(columns_query, (config.database, table_name))
    return config.cursor.fetchall()


def copy_table(config, table_from, table_to):
    print("CREATE TABLE", table_to)
    cols = columns(config, table_from)
    columns_sql = [
        "`%(name)s` %(type)s %(charset)s %(collation)s"
        % {
            "name": col["COLUMN_NAME"],
            "type": col["COLUMN_TYPE"],
            "charset": ("CHARACTER SET " + col["CHARACTER_SET_NAME"])
            if col["CHARACTER_SET_NAME"]
            else "",
            "collation": ("COLLATE " + col["COLLATION_NAME"])
            if col["COLLATION_NAME"]
            else "",
        }
        for col in cols
    ]

    create_sql = """CREATE TABLE %(schema)s.%(table_to)s (
    %(key_field)s varchar(36) PRIMARY KEY,
    %(date_field)s datetime,
    %(type_field)s varchar(2),
    %(columns)s
    )
    """ % {
        "schema": config.database,
        "table_to": table_to,
        "columns": ",\n ".join(columns_sql),
        "key_field": config.key_field,
        "date_field": config.date_field,
        "type_field": config.type_field,
    }

    config.cursor.execute(create_sql)


def update_table(config, table_from, table_to):
    cols_from = columns(config, table_from)
    cols_to = columns(config, table_to)
    from_dict = {col["COLUMN_NAME"]: col for col in cols_from}
    to_dict = {col["COLUMN_NAME"]: col for col in cols_to}
    new_column_names = set(from_dict.keys()).difference(set(to_dict.keys()))
    new_columns = {name: from_dict[name] for name in new_column_names}
    changed_type = []
    for name, col in from_dict.items():
        if name not in to_dict:
            continue
        if col["COLUMN_TYPE"] != to_dict[name]["COLUMN_TYPE"]:
            changed_type.append(col)

    print(
        "UPDATING COLUMNS IN TABLE",
        table_to,
        len(new_columns),
        "new",
        len(changed_type),
        "changed",
    )

    new_columns_sql = [
        "ADD COLUMN `%(name)s` %(type)s %(charset)s %(collation)s"
        % {
            "name": col["COLUMN_NAME"],
            "type": col["COLUMN_TYPE"],
            "charset": ("CHARACTER SET " + col["CHARACTER_SET_NAME"])
            if col["CHARACTER_SET_NAME"]
            else "",
            "collation": ("COLLATE " + col["COLLATION_NAME"])
            if col["COLLATION_NAME"]
            else "",
        }
        for name, col in new_columns.items()
    ]

    changed_columns_sql = [
        "MODIFY COLUMN `%(name)s` %(type)s %(charset)s %(collation)s"
        % {
            "name": col["COLUMN_NAME"],
            "type": col["COLUMN_TYPE"],
            "charset": ("CHARACTER SET " + col["CHARACTER_SET_NAME"])
            if col["CHARACTER_SET_NAME"]
            else "",
            "collation": ("COLLATE " + col["COLLATION_NAME"])
            if col["COLLATION_NAME"]
            else "",
        }
        for col in changed_type
    ]

    for sql in new_columns_sql + changed_columns_sql:
        config.cursor.execute(
            """ALTER TABLE %(schema)s.%(table)s %(sql)s"""
            % {"schema": config.database, "table": table_to, "sql": sql}
        )


def create_or_update_h_table(config, table_name):
    name = (
        config.h_prefix
        + table_name[: 64 - len(config.h_prefix) - len(config.h_postfix)]
        + config.h_postfix
    )
    exists = table_exists(config, name)
    if not exists:
        copy_table(config, table_name, name)
    else:
        update_table(config, table_name, name)
    return name


def drop_triggers(config, table_name):
    config.cursor.execute(
        """SELECT trigger_name
                             FROM information_schema.triggers
                             WHERE event_object_table=%s
                             AND trigger_schema = %s
                             AND trigger_name like %s""",
        (table_name, config.database, "%s%%" % escape_underscore(config.ht_prefix)),
    )
    names = [i["TRIGGER_NAME"] for i in config.cursor]
    for name in names:
        print("Dropping trigger", name)
        cursor.execute("DROP TRIGGER %s.%s" % (config.database, name))


def create_triggers(config, table_name, h_table):
    print("CREATING TRIGGERS FOR", h_table, "ON", table_name)
    id = uuid.uuid4().hex
    cols = columns(config, table_name)
    col_names = [col["COLUMN_NAME"] for col in cols]
    values = ", ".join(["NEW.`%s`" % col for col in col_names])

    # INSERT
    ins_trigger = """
    CREATE TRIGGER %(tprefix)s%(id)s
    AFTER INSERT ON %(schema)s.%(table)s FOR EACH ROW
    BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    END;
    INSERT INTO %(schema)s.%(h_table)s (`%(key_field)s`, `%(date_field)s`, `%(type_field)s`, `%(columns)s`)
    VALUES (UUID(), SYSDATE(), 'I', %(values)s);
    END""" % {
        "schema": config.database,
        "table": table_name,
        "h_table": h_table,
        "key_field": config.key_field,
        "date_field": config.date_field,
        "type_field": config.type_field,
        "columns": "`,`".join(col_names),
        "values": values,
        "tprefix": config.ht_prefix,
        "id": id,
    }

    config.cursor.execute(ins_trigger)

    # UPDATE
    id = uuid.uuid4().hex
    up_trigger = """
    CREATE TRIGGER %(tprefix)s%(id)s
    AFTER UPDATE ON %(schema)s.%(table)s FOR EACH ROW
    BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    END;
    INSERT INTO %(schema)s.%(h_table)s (`%(key_field)s`, `%(date_field)s`, `%(type_field)s`, `%(columns)s`)
    VALUES (UUID(), SYSDATE(), 'U', %(values)s);
    END""" % {
        "schema": config.database,
        "table": table_name,
        "h_table": h_table,
        "key_field": config.key_field,
        "date_field": config.date_field,
        "type_field": config.type_field,
        "columns": "`,`".join(col_names),
        "values": values,
        "tprefix": config.ht_prefix,
        "id": id,
    }

    config.cursor.execute(up_trigger)

    # DELETE
    id = uuid.uuid4().hex
    del_values = ",".join(["OLD.`%s`" % col for col in col_names])
    del_trigger = """
    CREATE TRIGGER %(tprefix)s%(id)s
    AFTER DELETE ON %(schema)s.%(table)s FOR EACH ROW
    BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
    END;
    INSERT INTO %(schema)s.%(h_table)s (`%(key_field)s`, `%(date_field)s`, `%(type_field)s`, `%(columns)s`)
    VALUES (UUID(), SYSDATE(), 'D', %(values)s);
    END""" % {
        "schema": config.database,
        "table": table_name,
        "h_table": h_table,
        "key_field": config.key_field,
        "date_field": config.date_field,
        "type_field": config.type_field,
        "columns": "`,`".join(col_names),
        "values": del_values,
        "tprefix": config.ht_prefix,
        "id": id,
    }

    config.cursor.execute(del_trigger)


def escape_underscore(text):
    return text.replace("_", "\_")


def history_table_template(config):
    return "%s%%%s" % (
        escape_underscore(config.h_prefix),
        escape_underscore(config.h_postfix),
    )


def drop_history_tables(config):
    cursor = config.cursor

    cursor.execute(
        """SELECT table_name
                      FROM information_schema.tables
                      WHERE table_name like %s
                      AND table_schema = %s
                   """,
        (history_table_template(config), config.database),
    )
    h_names = [i["TABLE_NAME"] for i in cursor]
    for name in h_names:
        print("Dropping", name)

        base_table_part = name[
            len(config.h_prefix) : -len(config.h_postfix)
            if len(config.h_postfix)
            else None
        ]
        cursor.execute(
            """SELECT table_name
                      FROM information_schema.tables
                      WHERE table_name like %s
                      AND table_schema = %s 
                      ORDER BY table_name ASC
                      """,
            ("%s%%" % escape_underscore(base_table_part), config.database),
        )

        base_table = cursor.fetchone()
        if base_table:
            base_table = base_table["TABLE_NAME"]
            drop_triggers(config, base_table)

        cursor.execute("DROP TABLE %s" % name)


Config = namedtuple(
    "Config",
    [
        "cursor",
        "database",
        "h_prefix",
        "h_postfix",
        "ht_prefix",
        "includes",
        "excludes",
        "key_field",
        "date_field",
        "type_field",
    ],
)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("USAGE: %s <config.json> [DROP]" % sys.argv[0])
        sys.exit(1)

    config = json.load(open(sys.argv[1], "r"))

    cnx = MySQLdb.connect(
        host=config["host"],
        user=config["user"],
        passwd=config["password"],
        db=config["database"],
    )
    cursor = cnx.cursor(MySQLdb.cursors.DictCursor)
    app_config = Config(
        cursor=cursor,
        database=config["database"],
        h_prefix=config["history_table_prefix"],
        h_postfix=config.get("history_table_postfix", ""),
        ht_prefix=config.get("history_trigger_prefix", "HST_"),
        includes=config.get("includes", [".*"]),
        excludes=config.get("excludes", []),
        key_field=config.get("key_field", "hst_id"),
        date_field=config.get("date_field", "hst_modified_date"),
        type_field=config.get("type_field", "hst_type"),
    )

    if len(sys.argv) > 2:
        if sys.argv[2].lower() == "drop":
            drop_history_tables(app_config)
            sys.exit(0)

    tables = table_names(app_config)
    for table in tables:
        h_name = create_or_update_h_table(app_config, table)
        drop_triggers(app_config, table)
        create_triggers(app_config, table, h_name)

    cnx.commit()
    cursor.close()
