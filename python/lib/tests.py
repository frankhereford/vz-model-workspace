import random
import string
from lorem_text import lorem
import psycopg2
from psycopg2.extras import RealDictCursor
from tabulate import tabulate
from lib.sqlprettyprint import sql_print


def cris_user_creates_crash_record_with_two_unit_records(db):
    # Generate a primary address that is a sentence of Lorem Ipsum
    lorem_sentence = lorem.sentence().replace(",", "").replace(".", "").split()
    primary_address = " ".join(lorem_sentence[: random.randint(3, 6)])

    # Generate a crash_id that is an integer but is not used in the table already
    sql = "SELECT MAX(crash_id) AS max FROM cris_facts.crashes;"
    sql_print(sql)

    with db.cursor() as cursor:
        cursor.execute(sql)
        max_crash_id = cursor.fetchone()["max"]
        crash_id = max_crash_id + 1 if max_crash_id else 1

    # Get a valid road_type_id from cris_lookup.road_types.upstream_id
    sql = "SELECT upstream_id FROM cris_lookup.road_types ORDER BY random() LIMIT 1;"
    sql_print(sql)

    with db.cursor() as cursor:
        cursor.execute(sql)
        road_type_id = cursor.fetchone()["upstream_id"]

    # Get a PostGIS point from the centroid of a random record in public.atd_txdot_locations.geometry
    sql = "SELECT ST_Centroid(geometry) AS centroid FROM public.atd_txdot_locations WHERE location_group = 1 ORDER BY random() LIMIT 1;"
    sql_print(sql)

    with db.cursor() as cursor:
        cursor.execute(sql)
        centroid = cursor.fetchone()["centroid"]

    # Insert a crash record (using the lookup-value-ids from CRIS)
    sql = """INSERT INTO cris_facts.crashes (crash_id, primary_address, road_type_id, location)
             VALUES (%s, %s, %s, %s)
             RETURNING crash_id;"""
    params = (crash_id, primary_address, road_type_id, centroid)
    with db.cursor() as cursor:
        cursor.execute(sql, params)
        inserted_crash_id = cursor.fetchone()["crash_id"]

    sql_print(
        sql % (params[0], f"'{params[1]}'", params[2], f"'{params[3]}'")
        + f" --> {inserted_crash_id}"
    )

    db.commit()

    # Generate two unit records related to the crash record
    inserted_unit_ids = []
    for _ in range(2):
        # Generate a unit_id that is an integer but is not used in the table already
        sql = "SELECT MAX(unit_id) AS max FROM cris_facts.units;"
        sql_print(sql)

        with db.cursor() as cursor:
            cursor.execute(sql)
            max_unit_id = cursor.fetchone()["max"]
            unit_id = max_unit_id + 1 if max_unit_id else 1

        # Get a valid unit_type_id from cris_lookup.unit_types.upstream_id
        sql = (
            "SELECT upstream_id FROM cris_lookup.unit_types ORDER BY random() LIMIT 1;"
        )
        sql_print(sql)

        with db.cursor() as cursor:
            cursor.execute(sql)
            unit_type_id = cursor.fetchone()["upstream_id"]

        # Insert a unit record (using the lookup-value-ids from CRIS)
        sql = """INSERT INTO cris_facts.units (unit_id, crash_id, unit_type_id)
                 VALUES (%s, %s, %s)
                 RETURNING unit_id;"""
        params = (unit_id, inserted_crash_id, unit_type_id)
        with db.cursor() as cursor:
            cursor.execute(sql, params)
            inserted_unit_id = cursor.fetchone()["unit_id"]
            inserted_unit_ids.append(inserted_unit_id)

        sql_print(sql % params + f" --> {inserted_unit_id}")

    db.commit()

    return inserted_crash_id, inserted_unit_ids


def vz_user_changes_a_crash_location(db, crash_id):
    # Get the visionzero_crash_fact_id for the record with this crash_id
    sql = "SELECT visionzero_crash_fact_id FROM public.crashes WHERE crash_id = %s;"
    sql_print(sql % crash_id)
    with db.cursor() as cursor:
        cursor.execute(sql, (crash_id,))
        vz_crash_fact_id = cursor.fetchone()["visionzero_crash_fact_id"]

    # Get a PostGIS point from the centroid of a random record in public.atd_txdot_locations.geometry
    sql = "SELECT ST_Centroid(geometry) AS centroid, ST_ASTEXT(ST_Centroid(geometry)) as centroid_wkt FROM public.atd_txdot_locations where location_group = 1 ORDER BY random() LIMIT 1;"
    sql_print(sql)
    with db.cursor() as cursor:
        cursor.execute(sql)
        record = cursor.fetchone()
        centroid = record["centroid"]
        centroid_wkt = record["centroid_wkt"]

    # Update the crash record in visionzero_facts.crashes
    sql = """UPDATE visionzero_facts.crashes SET location = %s
             WHERE id = %s
             RETURNING id;"""
    params = (centroid, vz_crash_fact_id)
    with db.cursor() as cursor:
        cursor.execute(sql, params)
        updated_crash_id = cursor.fetchone()["id"]
        sql_print((sql % (f"'{params[0]}'", params[1])) + f" --> {updated_crash_id}")
    db.commit()

    sql = "SELECT location_polygon_hex_id FROM public.crashes WHERE crash_id = %s;"
    sql_print(sql % crash_id)
    with db.cursor() as cursor:
        cursor.execute(sql, (crash_id,))
        location_polygon_hex_id = cursor.fetchone()["location_polygon_hex_id"]
    db.commit()

    return centroid_wkt, location_polygon_hex_id


def vz_user_changes_a_unit_type(db, crash_id):
    sql = "select visionzero_unit_fact_id, unit_type_id, unit_id from public.units where crash_id = %s order by random() limit 1;"
    with db.cursor() as cursor:
        cursor.execute(sql, (crash_id,))
        record = cursor.fetchone()
        unit_id = record["visionzero_unit_fact_id"]
        initial_unit_type_id = record["unit_type_id"]
        unit_id_cris_space = record["unit_id"]
    sql_print(sql % crash_id + " --> " + str(unit_id))

    sql = "SELECT id FROM public.unit_types where id != %s ORDER BY random() LIMIT 1;"
    with db.cursor() as cursor:
        cursor.execute(sql, (initial_unit_type_id,))
        unit_type_id = cursor.fetchone()["id"]
    sql_print(sql % initial_unit_type_id + f" --> {unit_type_id}")

    sql = "update visionzero_facts.units set unit_type_id = %s where id = %s returning unit_type_id;"
    with db.cursor() as cursor:
        cursor.execute(sql, (unit_type_id, unit_id))
        updated_unit_id = cursor.fetchone()["unit_type_id"]
    sql_print(sql % (unit_type_id, unit_id) + f" --> {updated_unit_id}")

    db.commit()

    return unit_id_cris_space, initial_unit_type_id, unit_type_id


def cris_user_update_crash_location_and_road_type(db, crash_id):
    sql = "SELECT cris_crash_fact_id FROM public.crashes WHERE crash_id = %s;"
    sql_print(sql % crash_id)
    with db.cursor() as cursor:
        cursor.execute(sql, (crash_id,))
        cris_crash_fact_id = cursor.fetchone()["cris_crash_fact_id"]

    # Get a PostGIS point from the centroid of a random record in public.atd_txdot_locations.geometry
    sql = "SELECT ST_Centroid(geometry) AS centroid, ST_ASTEXT(ST_Centroid(geometry)) as centroid_wkt FROM public.atd_txdot_locations where location_group = 1 ORDER BY random() LIMIT 1;"
    sql_print(sql)
    with db.cursor() as cursor:
        cursor.execute(sql)
        record = cursor.fetchone()
        centroid = record["centroid"]
        centroid_wkt = record["centroid_wkt"]

    sql = "select road_type_id from cris_facts.crashes where id = %s;"
    with db.cursor() as cursor:
        cursor.execute(sql, (cris_crash_fact_id,))
        old_road_type_id = cursor.fetchone()["road_type_id"]
        sql_print(sql % cris_crash_fact_id + f" --> {old_road_type_id}")

    # Get a valid road_type_id from cris_lookup.road_types.upstream_id
    sql = "SELECT upstream_id FROM cris_lookup.road_types where id != %s ORDER BY random() LIMIT 1;"
    sql_print(sql % old_road_type_id)
    with db.cursor() as cursor:
        cursor.execute(sql, (old_road_type_id,))
        road_type_id = cursor.fetchone()["upstream_id"]

    # NB, you use the IDs that CRIS uses, and they are translated in passing to be stored into the LDM values
    sql = """UPDATE cris_facts.crashes SET location = %s, road_type_id = %s
             WHERE id = %s
             RETURNING id;"""
    params = (centroid, road_type_id, cris_crash_fact_id)
    with db.cursor() as cursor:
        cursor.execute(sql, params)
        updated_crash_id = cursor.fetchone()["id"]
        sql_print(
            (sql % (f"'{params[0]}'", params[1], params[2]))
            + f" --> {updated_crash_id}"
        )

    db.commit()

    sql = "SELECT location_polygon_hex_id FROM public.crashes WHERE crash_id = %s;"
    sql_print(sql % crash_id)
    with db.cursor() as cursor:
        cursor.execute(sql, (crash_id,))
        location_polygon_hex_id = cursor.fetchone()["location_polygon_hex_id"]

    return updated_crash_id, centroid_wkt, road_type_id, location_polygon_hex_id


def cris_user_changes_a_unit_type(db, crash_id):
    sql = "SELECT cris_unit_fact_id, unit_type_id, unit_id FROM public.units WHERE crash_id = %s ORDER BY random() LIMIT 1;"
    with db.cursor() as cursor:
        cursor.execute(sql, (crash_id,))
        record = cursor.fetchone()
        cris_unit_fact_id = record["cris_unit_fact_id"]
        unit_id = record["unit_id"]
        sql_print(sql % crash_id + " --> " + str(cris_unit_fact_id))

    sql = "select unit_type_id from cris_facts.units where id = %s;"
    with db.cursor() as cursor:
        cursor.execute(sql, (cris_unit_fact_id,))
        initial_unit_type_id = cursor.fetchone()["unit_type_id"]
        sql_print(sql % cris_unit_fact_id + f" --> {initial_unit_type_id}")

    sql = "SELECT upstream_id FROM cris_lookup.unit_types WHERE id != %s ORDER BY random() LIMIT 1;"
    with db.cursor() as cursor:
        cursor.execute(sql, (initial_unit_type_id,))
        unit_type_id = cursor.fetchone()["upstream_id"]
        sql_print(sql % initial_unit_type_id + f" --> {unit_type_id}")

    sql = "UPDATE cris_facts.units SET unit_type_id = %s WHERE id = %s RETURNING unit_type_id;"
    with db.cursor() as cursor:
        cursor.execute(sql, (unit_type_id, cris_unit_fact_id))
        updated_unit_type_id = cursor.fetchone()["unit_type_id"]
        sql_print(
            sql % (unit_type_id, cris_unit_fact_id) + f" --> {updated_unit_type_id}"
        )

    db.commit()

    return unit_id, initial_unit_type_id, unit_type_id


def vz_user_creates_custom_lookup_value_and_uses_it(db, crash_id):
    # Generate a single word of Lorem Ipsum
    lorem_word = lorem.words(1)

    # SQL command to insert a new record into visionzero_lookup.road_types
    sql = "INSERT INTO visionzero_lookup.road_types (description) VALUES (%s) RETURNING id;"

    # Execute the SQL command
    with db.cursor() as cursor:
        cursor.execute(sql, (lorem_word,))
        inserted_id = cursor.fetchone()["id"]
        sql_print(sql % f"'{lorem_word}'" + f" --> {inserted_id}")

    # Refresh the materialized view
    sql = "REFRESH MATERIALIZED VIEW public.road_types;"
    with db.cursor() as cursor:
        cursor.execute(sql)
        sql_print(sql)

    db.commit()

    # Select the id of the record with the generated description
    sql = "SELECT id FROM public.road_types WHERE description = %s;"
    with db.cursor() as cursor:
        cursor.execute(sql, (lorem_word,))
        new_road_type_id = cursor.fetchone()["id"]
        sql_print((sql % f"'{lorem_word}'") + f" --> {new_road_type_id}")

    sql = "select visionzero_crash_fact_id from public.crashes where crash_id = %s"
    with db.cursor() as cursor:
        cursor.execute(sql, (crash_id,))
        record = cursor.fetchone()
        crash_fact_id = record["visionzero_crash_fact_id"]
        sql_print(sql % crash_id + " --> " + str(crash_fact_id))

    sql = "update visionzero_facts.crashes set road_type_id = %s where id = %s returning road_type_id;"
    with db.cursor() as cursor:
        cursor.execute(sql, (new_road_type_id, crash_fact_id))
        updated_road_type_id = cursor.fetchone()["road_type_id"]
        sql_print(
            sql % (new_road_type_id, crash_fact_id) + f" --> {updated_road_type_id}"
        )

    return inserted_id, lorem_word, new_road_type_id


def print_table_from_sql(db, sql, params):
    with db.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, params)
        records = cursor.fetchall()

        if records:
            # Get the column names from the first record's dictionary keys
            column_names = list(records[0].keys())

            # Convert each record dictionary to a list of values
            records_values = [list(record.values()) for record in records]

            # Format the records using tabulate
            table = tabulate(records_values, headers=column_names, tablefmt="grid")

            sql_print(sql % params)
            print(table)
        else:
            print(f"No record found with parameters: {params}")


def query_a_single_crash_for_truth(db, crash_id):
    sql = "SELECT * FROM public.crashes WHERE crash_id = %s;"
    print_table_from_sql(db, sql, (crash_id,))


def query_all_crashes_for_truth_and_print_ten_of_them(db):
    sql = """
    WITH all_crashes AS (
        SELECT * FROM public.crashes
    )
    SELECT * FROM all_crashes
    ORDER BY RANDOM()
    LIMIT 10;
    """
    print_table_from_sql(db, sql, ())


def query_worst_locations(db):
    sql = """
    SELECT
        atd_txdot_locations.location_id,
        COUNT(DISTINCT crash_location_map_immv.crash_id) AS crash_count,
        COUNT(units.unit_id) AS unit_count
    FROM
        atd_txdot_locations
    LEFT JOIN
        crash_location_map_immv ON (atd_txdot_locations.location_id = crash_location_map_immv.location_polygon_hex_id)
    JOIN
        units ON (crash_location_map_immv.crash_id = units.crash_id)
    WHERE
        atd_txdot_locations.location_group = 1
    GROUP BY
        atd_txdot_locations.location_id
    ORDER BY
        COUNT(units.unit_id) DESC,
        COUNT(DISTINCT crash_location_map_immv.crash_id) DESC
    LIMIT 10;
    """
    print_table_from_sql(db, sql, ())
