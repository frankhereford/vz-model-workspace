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
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    db.commit()


def drop_schemata_except(db, keep_schemata):
    with db.cursor() as cursor:
        cursor.execute("SELECT schema_name FROM information_schema.schemata;")
        all_schemata = [row["schema_name"] for row in cursor.fetchall()]
        for schema in all_schemata:
            if schema not in keep_schemata:
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
    db.commit()
