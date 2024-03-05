import random
import string
from lorem_text import lorem
import psycopg2
from psycopg2.extras import RealDictCursor


def cris_user_creates_crash_record_with_two_unit_records(db):
    # Generate a primary address that is a sentence of Lorem Ipsum
    lorem_sentence = lorem.sentence().replace(",", "").replace(".", "").split()
    primary_address = " ".join(lorem_sentence[: random.randint(3, 6)])

    # Generate a crash_id that is an integer but is not used in the table already
    sql_command = "SELECT MAX(crash_id) AS max FROM cris_facts.crashes;"
    print(sql_command)
    with db.cursor() as cursor:
        cursor.execute(sql_command)
        max_crash_id = cursor.fetchone()["max"]
        crash_id = max_crash_id + 1 if max_crash_id else 1

    # Get a valid road_type_id from cris_lookup.road_types.upstream_id
    sql_command = (
        "SELECT upstream_id FROM cris_lookup.road_types ORDER BY random() LIMIT 1;"
    )
    print(sql_command)
    with db.cursor() as cursor:
        cursor.execute(sql_command)
        road_type_id = cursor.fetchone()["upstream_id"]

    # Get a PostGIS point from the centroid of a random record in public.atd_txdot_locations.geometry
    sql_command = "SELECT ST_Centroid(geometry) AS centroid FROM public.atd_txdot_locations ORDER BY random() LIMIT 1;"
    print(sql_command)
    with db.cursor() as cursor:
        cursor.execute(sql_command)
        centroid = cursor.fetchone()["centroid"]

    # Insert a crash record (using the lookup-value-ids from CRIS)
    sql_command = """INSERT INTO cris_facts.crashes (crash_id, primary_address, road_type_id, location)
    VALUES (%s, %s, %s, %s)
    RETURNING crash_id;"""
    params = (crash_id, primary_address, road_type_id, centroid)
    with db.cursor() as cursor:
        cursor.execute(sql_command, params)
        inserted_crash_id = cursor.fetchone()["crash_id"]
        print(
            (sql_command % (params[0], f"'{params[1]}'", params[2], f"'{params[3]}'"))
            + f" --> {inserted_crash_id}"
        )

    db.commit()

    # Generate two unit records related to the crash record
    for _ in range(2):
        # Generate a unit_id that is an integer but is not used in the table already
        sql_command = "SELECT MAX(unit_id) AS max FROM cris_facts.units;"
        print(sql_command)
        with db.cursor() as cursor:
            cursor.execute(sql_command)
            max_unit_id = cursor.fetchone()["max"]
            unit_id = max_unit_id + 1 if max_unit_id else 1

        # Get a valid unit_type_id from cris_lookup.unit_types.upstream_id
        sql_command = (
            "SELECT upstream_id FROM cris_lookup.unit_types ORDER BY random() LIMIT 1;"
        )
        print(sql_command)
        with db.cursor() as cursor:
            cursor.execute(sql_command)
            unit_type_id = cursor.fetchone()["upstream_id"]

        # Insert a unit record (using the lookup-value-ids from CRIS)
        sql_command = """INSERT INTO cris_facts.units (unit_id, crash_id, unit_type_id)
    VALUES (%s, %s, %s)
    RETURNING unit_id;"""
        params = (unit_id, inserted_crash_id, unit_type_id)
        with db.cursor() as cursor:
            cursor.execute(sql_command, params)
            inserted_unit_id = cursor.fetchone()["unit_id"]
            print((sql_command % params) + f" --> {inserted_unit_id}")

    db.commit()
