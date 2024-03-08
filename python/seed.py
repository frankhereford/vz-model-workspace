#!/usr/bin/env python3
"""
Seed VZ Data Model DB with data from CSV files
"""
import argparse
import csv
import time

import psycopg
from psycopg import sql

DBNAME = "visionzero"
USER = "vz"
PASSWORD = "vz"
HOST = "db"
PORT = "5432"


def seed(path, table, limit=None):
    print(f"Seeding {table} from {path}...")

    conn = psycopg.connect(
        dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT
    )
    cur = conn.cursor()

    with open(path, "r") as csv_file:
        records = csv.reader(csv_file)
        headers = next(records)

        with cur.copy(
            "COPY {} ({}) FROM STDIN".format(table, ",".join(headers))
        ) as copy:
            count = 0
            for record in records:
                # Replace empty strings with None or else location table errors out
                record = [None if x == "" else x for x in record]
                copy.write_row(record)
                count += 1
                if count % 1000 == 0:
                    print(f"{count} records seeded")
                if count == limit:
                    break

    conn.commit()
    conn.close()


def main(args):
    start = time.time()
    limit = args.limit

    seed(
        path="/application/csv/locations.csv",
        table="cris.locations",
        limit=None,
    )
    seed(
        path="/application/csv/unit_types.csv",
        table="cris.unit_types_lookup",
        limit=None,
    )
    seed(
        path="/application/csv/road_types.csv",
        table="cris.road_types_lookup",
        limit=None,
    )
    seed(
        path="/application/csv/crashes.csv",
        table="cris.crash_cris_data",
        limit=limit,
    )
    seed(
        path="/application/csv/units.csv",
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
