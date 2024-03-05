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

    # Insert a crash record (using the lookup-value-ids from CRIS)
    sql_command = """INSERT INTO cris_facts.crashes (crash_id, primary_address, road_type_id)
VALUES (%s, %s, %s)
RETURNING crash_id;"""
    params = (crash_id, primary_address, road_type_id)
    with db.cursor() as cursor:
        cursor.execute(sql_command, params)
        inserted_crash_id = cursor.fetchone()["crash_id"]
        print(
            (sql_command % (params[0], f"'{params[1]}'", params[2]))
            + f" --> {inserted_crash_id}"
        )

    db.commit()
