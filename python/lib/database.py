import csv
import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_handle():
    return psycopg2.connect(
        dbname="visionzero",
        user="vz",
        password="vz",
        host="db",
        cursor_factory=RealDictCursor,
    )


def create_schemata(db):
    schemata = [
        "cris_facts",
        "visionzero_facts",
        "cris_lookup",
        "visionzero_lookup",
    ]
    with db.cursor() as cursor:
        for schema in schemata:
            sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
            print(sql)
            cursor.execute(sql)
    db.commit()


def drop_schemata_except(db):
    keep_schemata = [
        "public",
        "tiger",
        "tiger_data",
        "topology",
        "information_schema",
        "pg_catalog",
        "pg_toast",
    ]
    with db.cursor() as cursor:
        cursor.execute("SELECT schema_name FROM information_schema.schemata;")
        all_schemata = [row["schema_name"] for row in cursor.fetchall()]
        # print(f"all_schemata: {all_schemata}")
        for schema in all_schemata:
            if schema not in keep_schemata:
                sql = f"DROP SCHEMA IF EXISTS {schema} CASCADE;"
                print(sql)
                cursor.execute(sql)
    db.commit()


def create_lookup_tables(db):
    sql_commands = [
        """
CREATE SEQUENCE cris_lookup.road_types_sequence;
        """,
        """
CREATE TABLE cris_lookup.road_types (
    id INTEGER PRIMARY KEY DEFAULT nextval('cris_lookup.road_types_sequence'),
    description TEXT
    );
        """,
        """
CREATE TABLE visionzero_lookup.road_types (
    id INTEGER PRIMARY KEY DEFAULT nextval('cris_lookup.road_types_sequence'),
    description TEXT
    );
        """,
        """
CREATE SEQUENCE cris_lookup.unit_types_sequence;
        """,
        """
CREATE TABLE cris_lookup.unit_types (
    id INTEGER PRIMARY KEY DEFAULT nextval('cris_lookup.unit_types_sequence'),
    description TEXT
    );
        """,
        """
CREATE TABLE visionzero_lookup.unit_types (
    id INTEGER PRIMARY KEY DEFAULT nextval('cris_lookup.unit_types_sequence'),
    description TEXT
    );
        """,
    ]
    with db.cursor() as cursor:
        for sql in sql_commands:
            sql = sql.strip()
            print(sql)
            cursor.execute(sql)
    db.commit()


def populate_lookup_tables(db):
    with open("seeds/road_types.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row
        with db.cursor() as cursor:
            for row in reader:
                sql = "INSERT INTO cris_lookup.road_types (id, description) VALUES (%s, %s) RETURNING id;"
                cursor.execute(sql, (row[0], row[1]))
                returned_id = cursor.fetchone()["id"]
                print((sql % (row[0], f"'{row[1]}'")) + f" --> {returned_id}")
            db.commit()
    with open("seeds/unit_types.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row
        with db.cursor() as cursor:
            for row in reader:
                sql = "INSERT INTO cris_lookup.unit_types (id, description) VALUES (%s, %s) RETURNING id;"
                cursor.execute(sql, (row[0], row[1]))
                returned_id = cursor.fetchone()["id"]
                print((sql % (row[0], f"'{row[1]}'")) + f" --> {returned_id}")
            db.commit()


def set_lookup_sequences(db):
    pairs = [
        (
            "cris_lookup.road_types",
            "visionzero_lookup.road_types",
            "cris_lookup.road_types_sequence",
        ),
        (
            "cris_lookup.unit_types",
            "visionzero_lookup.unit_types",
            "cris_lookup.unit_types_sequence",
        ),
    ]

    with db.cursor() as cursor:
        for table1, table2, sequence in pairs:
            sql = f"SELECT MAX(id) as max FROM {table1};"
            cursor.execute(sql)
            max_id1 = cursor.fetchone()["max"] or 0
            print(sql + " --> " + str(max_id1))

            sql = f"SELECT MAX(id) as max FROM {table2};"
            cursor.execute(sql)
            max_id2 = cursor.fetchone()["max"] or 0
            print(sql + " --> " + str(max_id2))

            next_seq_val = max(max_id1, max_id2) + 1

            sql = f"ALTER SEQUENCE {sequence} RESTART WITH {next_seq_val};"
            print(sql)
            cursor.execute(sql)

        db.commit()
