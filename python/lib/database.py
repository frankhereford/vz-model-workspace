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


def artifically_descync_sequences_from_cris_data(db, sequence_names):
    with db.cursor() as cursor:
        for sequence in sequence_names:
            # Query the current value of the sequence
            cursor.execute(f"SELECT last_value FROM {sequence};")
            last_value = cursor.fetchone()["last_value"]
            # Add 1000 to the current value and set the sequence to the new value
            new_value = last_value + 999
            cursor.execute(f"SELECT setval('{sequence}', {new_value});")
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
);
        """,
        """CREATE TABLE cris_facts.units (
    id SERIAL PRIMARY KEY,
    unit_id INTEGER NOT NULL,
    crash_id INTEGER NOT NULL,
    unit_type_id INTEGER
);
        """,
        """CREATE TABLE visionzero_facts.crashes (
    id SERIAL PRIMARY KEY,
    cris_id INTEGER NOT NULL REFERENCES cris_facts.crashes(id) ON DELETE CASCADE,
    crash_id INTEGER,
    primary_address TEXT,
    road_type_id INTEGER,
    location GEOMETRY(Point, 4326)
);
        """,
        """CREATE TABLE visionzero_facts.units (
    id SERIAL PRIMARY KEY,
    cris_id INTEGER NOT NULL REFERENCES cris_facts.units(id) ON DELETE CASCADE,
    unit_id INTEGER,
    crash_id INTEGER,
    unit_type_id INTEGER
);
        """,
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


def create_lookup_table_substitution_triggers(db):
    sql_commands = [
        """CREATE OR REPLACE FUNCTION substitute_ldm_crash_lookup_table_ids()
RETURNS TRIGGER AS $$
DECLARE
    new_road_type_id int;
BEGIN
    SELECT id INTO new_road_type_id
    FROM public.road_types
    WHERE source = 'cris' AND upstream_id = NEW.road_type_id;

    IF FOUND THEN
        NEW.road_type_id := new_road_type_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
        """,
        """CREATE TRIGGER before_insert_crashes
BEFORE INSERT ON cris_facts.crashes
FOR EACH ROW EXECUTE FUNCTION substitute_ldm_crash_lookup_table_ids();
        """,
        """CREATE OR REPLACE FUNCTION substitute_ldm_unit_lookup_table_ids()
RETURNS TRIGGER AS $$
DECLARE
    new_unit_type_id int;
BEGIN
    SELECT id INTO new_unit_type_id
    FROM public.unit_types
    WHERE source = 'cris' AND upstream_id = NEW.unit_type_id;

    IF FOUND THEN
        NEW.unit_type_id := new_unit_type_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
        """,
        """CREATE TRIGGER before_insert_units
BEFORE INSERT ON cris_facts.units
FOR EACH ROW EXECUTE FUNCTION substitute_ldm_unit_lookup_table_ids();
        """,
    ]

    with db.cursor() as cursor:
        for sql_command in sql_commands:
            print(sql_command)
            cursor.execute(sql_command)
        db.commit()


def populate_fact_tables(db, batch_size=100000):
    with open("seeds/crashes.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row
        with db.cursor() as cursor:
            sql = "INSERT INTO cris_facts.crashes (crash_id, primary_address, road_type_id, location) VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326));"
            batch = []
            for i, row in enumerate(reader, start=1):
                if i > batch_size:
                    continue
                # Create a point geometry from the latitude and longitude
                point = f"POINT({row[2]} {row[1]})"
                batch.append((row[0], row[3], row[4], point))
                if i % batch_size == 0:
                    print((sql % (row[0], f"'{row[3]}'", row[4], point)))
                    cursor.executemany(sql, batch)
                    batch = []  # Reset the batch
            # Execute the remaining batch if it's not empty
            if batch:
                cursor.executemany(sql, batch)
            db.commit()
    with open("seeds/units.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)
        with db.cursor() as cursor:
            sql = "INSERT INTO cris_facts.units (unit_id, crash_id, unit_type_id) VALUES (%s, %s, %s);"
            batch = []
            for i, row in enumerate(reader, start=1):
                if i > batch_size:
                    continue
                batch.append((row[0], row[1], row[2]))
                if i % batch_size == 0:
                    print((sql % (row[0], row[1], row[2])))
                    cursor.executemany(sql, batch)
                    batch = []
            if batch:
                cursor.executemany(sql, batch)
            db.commit()


# ! should do an example of this using triggers and a table, not just a view
def create_unifying_fact_views(db):
    sql_commands = [
        """
        CREATE OR REPLACE VIEW public.crashes AS 
        SELECT 
            -- could remove the coalsece and just use either one
            COALESCE(visionzero_facts.crashes.crash_id, cris_facts.crashes.crash_id) AS crash_id,
            COALESCE(visionzero_facts.crashes.primary_address, cris_facts.crashes.primary_address) AS primary_address,
            COALESCE(visionzero_facts.crashes.road_type_id, cris_facts.crashes.road_type_id) AS road_type_id,
            COALESCE(visionzero_facts.crashes.location, cris_facts.crashes.location) AS location
        FROM 
            cris_facts.crashes
        LEFT JOIN 
            visionzero_facts.crashes 
        ON 
            cris_facts.crashes.id = visionzero_facts.crashes.cris_id;
        """,
        """
        CREATE OR REPLACE VIEW public.units AS 
        SELECT 
            -- this could be simply cris_facts.units.unit_id, but does it matter?
            COALESCE(visionzero_facts.units.unit_id, cris_facts.units.unit_id) AS unit_id,
            COALESCE(visionzero_facts.units.crash_id, cris_facts.units.crash_id) AS crash_id,
            COALESCE(visionzero_facts.units.unit_type_id, cris_facts.units.unit_type_id) AS unit_type_id
        FROM 
            cris_facts.units
        LEFT JOIN 
            visionzero_facts.units 
        ON 
            cris_facts.units.id = visionzero_facts.units.cris_id;
        """,
    ]

    with db.cursor() as cursor:
        for sql_command in sql_commands:
            print(sql_command)
            cursor.execute(sql_command)
        db.commit()
