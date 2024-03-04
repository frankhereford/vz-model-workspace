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


def create_schemata(db, schemata):
    with db.cursor() as cursor:
        for schema in schemata:
            sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
            print(sql)
            cursor.execute(sql)
    db.commit()


def drop_schemata_except(db, keep_schemata):
    additional_exceptions = [
        "public",
        "tiger",
        "tiger_data",
        "topology",
        "information_schema",
        "pg_catalog",
        "pg_toast",
    ]
    keep_schemata.extend(additional_exceptions)
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
CREATE TABLE cris_lookup.road_types (
    id SERIAL PRIMARY KEY,
    description TEXT
    );
        """,
        """
CREATE TABLE cris_lookup.unit_types (
    id SERIAL PRIMARY KEY,
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


def create_fact_tables(db):
    sql = " "
