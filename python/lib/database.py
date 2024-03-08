import csv
import psycopg2
import subprocess
import time
from psycopg2.extras import RealDictCursor
from lib.sqlprettyprint import sql_print, bash_print


def get_db_handle():
    return psycopg2.connect(
        dbname="visionzero",
        user="vz",
        password="vz",
        host="db",
        cursor_factory=RealDictCursor,
    )


def ensure_extensions_exists(db):
    with db.cursor() as cursor:
        # Create postgis extension if it does not exist
        sql = "CREATE EXTENSION IF NOT EXISTS postgis;"
        sql_print(sql)
        cursor.execute(sql)

        # Create periods extension if it does not exist
        sql = "CREATE EXTENSION IF NOT EXISTS periods CASCADE;"
        sql_print(sql)
        cursor.execute(sql)

        # Create ivm extension if it does not exist
        sql = "CREATE EXTENSION IF NOT EXISTS pg_ivm CASCADE;"
        sql_print(sql)
        cursor.execute(sql)

    db.commit()


def disconnect_other_users(db):
    with db.cursor() as cursor:
        sql = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'visionzero' AND pid <> pg_backend_pid();"
        sql_print(sql)
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
        "periods",
        "__pg_ivm__",
    ]

    with db.cursor() as cursor:
        sql = "SELECT schema_name FROM information_schema.schemata;"
        cursor.execute(sql)
        all_schemata = [row["schema_name"] for row in cursor.fetchall()]

        for schema in all_schemata:
            if schema not in keep_schemata:
                sql = f"DROP SCHEMA IF EXISTS {schema} CASCADE;"
                sql_print(sql)
                cursor.execute(sql)

    db.commit()


def drop_public_entities(db):
    tables = ["atd_txdot_locations", "units", "crash_location_map"]
    views = ["crashes"]
    materialized_views = ["road_types", "unit_types"]

    with db.cursor() as cursor:
        for view in views:
            sql = f"DROP VIEW IF EXISTS public.{view};"
            sql_print(sql)
            cursor.execute(sql)

        for materialized_view in materialized_views:
            sql = f"DROP MATERIALIZED VIEW IF EXISTS public.{materialized_view};"
            sql_print(sql)
            cursor.execute(sql)

        for table in tables:
            sql = f"DROP TABLE IF EXISTS public.{table};"
            sql_print(sql)
            cursor.execute(sql)

    db.commit()


def pull_down_locations(db):
    # Run the pg_dump command and pipe the output into psql
    command = (
        f"pg_dump -t atd_txdot_locations | PGPASSWORD=vz psql -U vz -h db -d visionzero"
    )
    bash_print(command)
    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    _, error = process.communicate()

    if process.returncode != -1:
        print(f"Error occurred while running pg_dump and psql: {error}")


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
            sql_print(sql)
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
            sql_print(sql)
            cursor.execute(sql)
    db.commit()


def artifically_descync_sequences_from_cris_data(db, sequence_names):
    with db.cursor() as cursor:
        for sequence in sequence_names:
            # Query the current value of the sequence
            sql = f"SELECT last_value FROM {sequence};"
            sql_print(sql)
            cursor.execute(sql)
            last_value = cursor.fetchone()["last_value"]

            # Add 1000 to the current value and set the sequence to the new value
            new_value = last_value + 999
            sql = f"SELECT setval('{sequence}', {new_value});"
            sql_print(sql)
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
                sql_print((sql % (row[0], f"'{row[1]}'")) + f" --> {returned_id}")
            db.commit()

    with open("seeds/unit_types.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row
        with db.cursor() as cursor:
            for row in reader:
                sql = "INSERT INTO cris_lookup.unit_types (upstream_id, description) VALUES (%s, %s) RETURNING id;"
                cursor.execute(sql, (row[0], row[1]))
                returned_id = cursor.fetchone()["id"]
                sql_print((sql % (row[0], f"'{row[1]}'")) + f" --> {returned_id}")
            db.commit()


def refresh_materialized_views(db):
    with db.cursor() as cursor:
        sql = """
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = 'public'
        """
        cursor.execute(sql)
        matviews = [row["matviewname"] for row in cursor.fetchall()]

        for matview in matviews:
            sql = f"REFRESH MATERIALIZED VIEW public.{matview};"
            sql_print(sql)
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
        "CREATE INDEX idx_crashes_cris_id ON visionzero_facts.crashes (cris_id);",
        "CREATE INDEX idx_crashes_id ON cris_facts.crashes (id);",
        "CREATE INDEX idx_units_crash_id_id ON cris_facts.units (crash_id, id);",
        "CREATE INDEX idx_units_cris_id_crash_id ON visionzero_facts.units (cris_id, crash_id);",
    ]

    with db.cursor() as cursor:
        for sql in sql_commands:
            sql_print(sql)
            cursor.execute(sql)

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
        """CREATE OR REPLACE FUNCTION cris_facts_crashes_delete_trigger()
        RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM visionzero_facts.crashes WHERE cris_id = OLD.id;
            RETURN OLD;
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
        """CREATE OR REPLACE FUNCTION cris_facts_units_delete_trigger()
        RETURNS TRIGGER AS $$
        BEGIN
            DELETE FROM visionzero_facts.units WHERE cris_id = OLD.id;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        """,
    ]

    with db.cursor() as cursor:
        for sql in sql_commands:
            sql_print(sql)
            cursor.execute(sql)

    db.commit()


def create_cris_facts_triggers(db):
    sql_commands = [
        """CREATE TRIGGER trigger_cris_facts_crashes_after_insert
        AFTER INSERT ON cris_facts.crashes
        FOR EACH ROW EXECUTE FUNCTION cris_facts_crashes_insert_trigger();
        """,
        """CREATE TRIGGER trigger_cris_facts_crashes_after_delete
        AFTER DELETE ON cris_facts.crashes
        FOR EACH ROW EXECUTE FUNCTION cris_facts_crashes_delete_trigger();
        """,
        """CREATE TRIGGER trigger_cris_facts_units_after_insert
        AFTER INSERT ON cris_facts.units
        FOR EACH ROW EXECUTE FUNCTION cris_facts_units_insert_trigger();
        """,
        """CREATE TRIGGER trigger_cris_facts_units_after_delete
        AFTER DELETE ON cris_facts.units
        FOR EACH ROW EXECUTE FUNCTION cris_facts_units_delete_trigger();
        """,
    ]

    with db.cursor() as cursor:
        for sql in sql_commands:
            sql_print(sql)
            cursor.execute(sql)

    db.commit()


def create_lookup_table_substitution_triggers(db):
    sql_commands = [
        """CREATE OR REPLACE FUNCTION substitute_ldm_crash_lookup_table_ids()
        RETURNS TRIGGER AS $$
        DECLARE
            new_road_type_id int;
        BEGIN
            -- Check if the road_type_id column is being updated
            IF NEW.road_type_id IS DISTINCT FROM OLD.road_type_id THEN
                SELECT id INTO new_road_type_id
                FROM public.road_types
                WHERE source = 'cris' AND upstream_id = NEW.road_type_id;
                IF FOUND THEN
                    NEW.road_type_id := new_road_type_id;
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """,
        """CREATE TRIGGER before_insert_crashes
        BEFORE INSERT ON cris_facts.crashes
        FOR EACH ROW EXECUTE FUNCTION substitute_ldm_crash_lookup_table_ids();
        """,
        """CREATE TRIGGER before_update_crashes
        BEFORE UPDATE ON cris_facts.crashes
        FOR EACH ROW EXECUTE FUNCTION substitute_ldm_crash_lookup_table_ids();
        """,
        """CREATE OR REPLACE FUNCTION substitute_ldm_unit_lookup_table_ids()
        RETURNS TRIGGER AS $$
        DECLARE
            new_unit_type_id int;
        BEGIN
            -- Check if the unit_type_id column is being updated
            IF NEW.unit_type_id IS DISTINCT FROM OLD.unit_type_id THEN
                SELECT id INTO new_unit_type_id
                FROM public.unit_types
                WHERE source = 'cris' AND upstream_id = NEW.unit_type_id;
                IF FOUND THEN
                    NEW.unit_type_id := new_unit_type_id;
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """,
        """CREATE TRIGGER before_insert_units
        BEFORE INSERT ON cris_facts.units
        FOR EACH ROW EXECUTE FUNCTION substitute_ldm_unit_lookup_table_ids();
        """,
        """CREATE TRIGGER before_update_units
        BEFORE UPDATE ON cris_facts.units
        FOR EACH ROW EXECUTE FUNCTION substitute_ldm_unit_lookup_table_ids();
        """,
    ]

    with db.cursor() as cursor:
        for sql in sql_commands:
            sql_print(sql)
            cursor.execute(sql)

    db.commit()


def populate_fact_tables(db, BE_QUICK_ABOUT_IT=True, batch_size=100000):
    with open("seeds/crashes.csv", "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row

        with db.cursor() as cursor:
            sql = "INSERT INTO cris_facts.crashes (crash_id, primary_address, road_type_id, location) VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326));"
            batch = []
            for i, row in enumerate(reader, start=1):
                if BE_QUICK_ABOUT_IT and i > batch_size:
                    continue

                # Create a point geometry from the latitude and longitude
                point = f"POINT({row[2]} {row[1]})"
                batch.append((row[0], row[3], row[4], point))

                if i % int(batch_size / 10) == 0:
                    sql_print((sql % (row[0], f"'{row[3]}'", row[4], point)))
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
                if BE_QUICK_ABOUT_IT and i > batch_size:
                    continue

                batch.append((row[0], row[1], row[2]))

                if i % int(batch_size / 10) == 0:
                    sql_print((sql % (row[0], row[1], row[2])))
                    cursor.executemany(sql, batch)
                    batch = []

            if batch:
                cursor.executemany(sql, batch)
            db.commit()


def create_unifying_fact_views(db):
    sql_commands = [
        """
        select create_immv('units', '
        SELECT
            cris_facts.units.id AS cris_unit_fact_id,
            visionzero_facts.units.id AS visionzero_unit_fact_id,
            cris_facts.units.unit_id as unit_id,
            cris_facts.units.crash_id as crash_id,
            COALESCE(visionzero_facts.units.unit_type_id, cris_facts.units.unit_type_id) AS unit_type_id
        FROM
            cris_facts.units
        JOIN
            visionzero_facts.units
        ON
            cris_facts.units.id = visionzero_facts.units.cris_id
        ');
        """,
        """
        select create_immv('crash_location_map', '
        SELECT
            cris_facts.crashes.crash_id AS crash_id,
            atd_txdot_locations.polygon_hex_id as location_polygon_hex_id
        FROM
            cris_facts.crashes
        JOIN visionzero_facts.crashes ON cris_facts.crashes.id = visionzero_facts.crashes.cris_id
        JOIN public.atd_txdot_locations ON (
            public.atd_txdot_locations.location_group = 1
            and COALESCE(visionzero_facts.crashes.location, cris_facts.crashes.location) && public.atd_txdot_locations.geometry
            AND ST_Contains(public.atd_txdot_locations.geometry, COALESCE(visionzero_facts.crashes.location, cris_facts.crashes.location))
        )
        ');
        """,
        """
        create view public.crashes as (
        SELECT
            cris_facts.crashes.id AS cris_crash_fact_id,
            visionzero_facts.crashes.id AS visionzero_crash_fact_id,
            cris_facts.crashes.crash_id AS crash_id,
            COALESCE(visionzero_facts.crashes.primary_address, cris_facts.crashes.primary_address) AS primary_address,
            COALESCE(visionzero_facts.crashes.road_type_id, cris_facts.crashes.road_type_id) AS road_type_id,
            COALESCE(visionzero_facts.crashes.location, cris_facts.crashes.location) AS location,
            crash_location_map.location_polygon_hex_id,
            array_agg(distinct public.units.unit_type_id) as units_unit_type_ids
        from cris_facts.crashes
        JOIN visionzero_facts.crashes ON cris_facts.crashes.id = visionzero_facts.crashes.cris_id
        left join crash_location_map on (cris_facts.crashes.crash_id = crash_location_map.crash_id)
        join public.units on (cris_facts.crashes.crash_id = public.units.crash_id)
        group by cris_facts.crashes.id,
                 visionzero_facts.crashes.id,
                 crash_location_map.location_polygon_hex_id
        );
        """,
    ]

    for sql in sql_commands:
        time.sleep(5)
        with db.cursor() as cursor:
            sql_print(sql)
            cursor.execute(sql)
            db.commit()


def create_temporal_tracking(db):
    tables = [
        "cris_facts.crashes",
        "cris_facts.units",
        "visionzero_facts.crashes",
        "visionzero_facts.units",
    ]

    with db.cursor() as cursor:
        for table in tables:
            # Add system time period
            sql = f"SELECT periods.add_system_time_period('{table}', 'validity_start', 'validity_end');"
            sql_print(sql)
            cursor.execute(sql)

            # Add system versioning
            sql = f"SELECT periods.add_system_versioning('{table}');"
            sql_print(sql)
            cursor.execute(sql)

    db.commit()
