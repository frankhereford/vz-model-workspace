#!/usr/bin/env python3
"""
Seed VZ Data Model DB with data from CSV files
"""
import csv
import time
import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras
import pandas as pd
from io import StringIO

DBNAME = "visionzero"
USER = "vz"
PASSWORD = "vz"
HOST = "db"
PORT = "5432"


def seed(path, table):
    print(f"Seeding {table} from {path}...")

    conn = psycopg2.connect(
        dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT
    )
    cur = conn.cursor()

    with open(path, "r") as csv_file:
        records = csv.DictReader(csv_file)
        headers = records.fieldnames

    copy = sql.SQL(
        """COPY {}({})
    FROM '{}'
    DELIMITER ','
    CSV HEADER;"""
    ).format(
        sql.SQL(table), sql.SQL(", ").join(map(sql.Identifier, headers)), sql.SQL(path)
    )

    cur.execute(copy.as_string(conn))

    conn.commit()
    conn.close()


def main():
    start = time.time()

    seed(path="/application/csv/locations.csv", table="cris.locations")
    seed(path="/application/csv/unit_types.csv", table="cris.unit_types_lookup")
    seed(path="/application/csv/road_types.csv", table="cris.road_types_lookup")
    seed(path="/application/csv/crashes-short.csv", table="cris.crash_cris_data")
    seed(path="/application/csv/units.csv", table="cris.unit_cris_data")

    end = time.time()
    elapsed_minutes = (end - start) / 60

    print(f"Seeding complete in {elapsed_minutes} min.")


if __name__ == "__main__":
    main()
