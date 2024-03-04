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
    upstream_id INTEGER,
    description TEXT
    );
        """,
        """
CREATE TABLE visionzero_lookup.road_types (
    id INTEGER PRIMARY KEY DEFAULT nextval('cris_lookup.road_types_sequence'),
    upstream_id INTEGER,
    description TEXT
    );
        """,
        """
CREATE SEQUENCE cris_lookup.unit_types_sequence;
        """,
        """
CREATE TABLE cris_lookup.unit_types (
    id INTEGER PRIMARY KEY DEFAULT nextval('cris_lookup.unit_types_sequence'),
    upstream_id INTEGER,
    description TEXT
    );
        """,
        """
CREATE TABLE visionzero_lookup.unit_types (
    id INTEGER PRIMARY KEY DEFAULT nextval('cris_lookup.unit_types_sequence'),
    upstream_id INTEGER,
    description TEXT
    );
        """,
        """
CREATE MATERIALIZED VIEW public.road_types AS
        SELECT 
            cris.id, 
            'cris' AS source, 
            cris.upstream_id, 
            cris.description
        FROM 
            cris_lookup.road_types AS cris
    UNION ALL
        SELECT 
            visionzero.id, 
            'visionzero' AS source, 
            visionzero.upstream_id, 
            visionzero.description
        FROM 
            visionzero_lookup.road_types AS visionzero;
        """,
        """
CREATE MATERIALIZED VIEW public.unit_types AS
        SELECT 
            cris.id, 
            'cris' AS source, 
            cris.upstream_id, 
            cris.description
        FROM 
            cris_lookup.unit_types AS cris
    UNION ALL
        SELECT 
            visionzero.id, 
            'visionzero' AS source, 
            visionzero.upstream_id, 
            visionzero.description
        FROM 
            visionzero_lookup.unit_types AS visionzero;
        """,
        """
CREATE INDEX idx_unit_types_id ON public.unit_types (id);
        """,
        """
CREATE INDEX idx_road_types_id ON public.road_types (id);
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
                sql = "INSERT INTO cris_lookup.road_types (upstream_id, description) VALUES (%s, %s) RETURNING id;"
                cursor.execute(sql, (row[0], row[1]))
                returned_id = cursor.fetchone()["id"]
                print((sql % (row[0], f"'{row[1]}'")) + f" --> {returned_id}")
            db.commit()
    with open("seeds/unit_types.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row
        with db.cursor() as cursor:
            for row in reader:
                sql = "INSERT INTO cris_lookup.unit_types (upstream_id, description) VALUES (%s, %s) RETURNING id;"
                cursor.execute(sql, (row[0], row[1]))
                returned_id = cursor.fetchone()["id"]
                print((sql % (row[0], f"'{row[1]}'")) + f" --> {returned_id}")
            db.commit()


# def set_lookup_sequences(db):
#     pairs = [
#         (
#             "cris_lookup.road_types",
#             "visionzero_lookup.road_types",
#             "cris_lookup.road_types_sequence",
#         ),
#         (
#             "cris_lookup.unit_types",
#             "visionzero_lookup.unit_types",
#             "cris_lookup.unit_types_sequence",
#         ),
#     ]

#     with db.cursor() as cursor:
#         for table1, table2, sequence in pairs:
#             sql = f"SELECT MAX(id) as max FROM {table1};"
#             cursor.execute(sql)
#             max_id1 = cursor.fetchone()["max"] or 0
#             print(sql + " --> " + str(max_id1))

#             sql = f"SELECT MAX(id) as max FROM {table2};"
#             cursor.execute(sql)
#             max_id2 = cursor.fetchone()["max"] or 0
#             print(sql + " --> " + str(max_id2))

#             next_seq_val = max(max_id1, max_id2) + 1

#             sql = f"ALTER SEQUENCE {sequence} RESTART WITH {next_seq_val};"
#             print(sql)
#             cursor.execute(sql)

#         db.commit()


def refresh_materialized_views(db):
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT matviewname 
            FROM pg_matviews 
            WHERE schemaname = 'public'
        """
        )
        matviews = [row["matviewname"] for row in cursor.fetchall()]
        for matview in matviews:
            sql = f"REFRESH MATERIALIZED VIEW public.{matview};"
            print(sql)
            cursor.execute(sql)
        db.commit()


def create_fact_tables(db):
    sql_commands = [
        """CREATE TABLE cris_facts.crashes (
    id SERIAL PRIMARY KEY,
    crash_id INTEGER NOT NULL,
    primary_address TEXT,
    road_type_id INTEGER,
    location GEOMETRY(Point, 4326)
);""",
        """CREATE TABLE cris_facts.units (
    id SERIAL PRIMARY KEY,
    unit_id TEXT,
    crash_id INTEGER NOT NULL,
    unit_type_id INTEGER
);""",
        """CREATE TABLE visionzero_facts.crashes (
    id SERIAL PRIMARY KEY,
    cris_id INTEGER NOT NULL REFERENCES cris_facts.crashes(id) ON DELETE CASCADE,
    crash_id INTEGER,
    primary_address TEXT,
    road_type_id INTEGER,
    location GEOMETRY(Point, 4326)
);""",
        """CREATE TABLE visionzero_facts.units (
    id SERIAL PRIMARY KEY,
    cris_id INTEGER NOT NULL REFERENCES cris_facts.units(id) ON DELETE CASCADE,
    unit_id TEXT,
    crash_id INTEGER,
    unit_type_id INTEGER
);""",
    ]

    with db.cursor() as cursor:
        for sql_command in sql_commands:
            print(sql_command)
            cursor.execute(sql_command)
        db.commit()


def create_cris_facts_functions(db):
    sql_commands = [
        """CREATE OR REPLACE FUNCTION cris_facts_crashes_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO visionzero_facts.crashes (cris_id) VALUES (NEW.id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
        """,
        """CREATE OR REPLACE FUNCTION cris_facts_units_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO visionzero_facts.units (cris_id) VALUES (NEW.id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
        """,
    ]

    with db.cursor() as cursor:
        for sql_command in sql_commands:
            print(sql_command)
            cursor.execute(sql_command)
        db.commit()


def create_cris_facts_triggers(db):
    sql_commands = [
        """CREATE TRIGGER trigger_cris_facts_crashes_after_insert
AFTER INSERT ON cris_facts.crashes
FOR EACH ROW EXECUTE FUNCTION cris_facts_crashes_insert_trigger();
        """,
        """CREATE TRIGGER trigger_cris_facts_units_after_insert
AFTER INSERT ON cris_facts.units
FOR EACH ROW EXECUTE FUNCTION cris_facts_units_insert_trigger();
        """,
    ]

    with db.cursor() as cursor:
        for sql_command in sql_commands:
            print(sql_command)
            cursor.execute(sql_command)
        db.commit()
