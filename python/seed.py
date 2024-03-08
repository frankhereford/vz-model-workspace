#!/usr/bin/env python3
"""
Seed VZ Data Model DB with data from CSV files
"""
import argparse
import csv
import time

import psycopg2
from psycopg2 import sql

DBNAME = "visionzero"
USER = "vz"
PASSWORD = "vz"
HOST = "db"
PORT = "5432"


def seed(path, table, limit):
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


def main(args):
    start = time.time()
    limit = args.limit

    seed(path="/application/csv/locations.csv", table="cris.locations", limit=limit)
    seed(
        path="/application/csv/unit_types.csv",
        table="cris.unit_types_lookup",
        limit=limit,
    )
    seed(
        path="/application/csv/road_types.csv",
        table="cris.road_types_lookup",
        limit=limit,
    )
    seed(
        path="/application/csv/crashes-short.csv",
        table="cris.crash_cris_data",
        limit=limit,
    )
    seed(
        path="/application/csv/units-short.csv",
        table="cris.unit_cris_data",
        limit=limit,
    )

    end = time.time()
    elapsed_minutes = (end - start) / 60

    print(f"Seeding complete in {elapsed_minutes} min.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l", "--limit", type=int, help="limit the number of records to seed"
    )
    args = parser.parse_args()

    main(args)
