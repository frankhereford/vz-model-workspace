#!/usr/bin/env python3
"""
Seed VZ Data Model DB with data from CSV files
"""
import csv
import time
import psycopg2
from psycopg2 import sql

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

    # with open(path, "r") as csv_file:
    #     records = csv.DictReader(csv_file)

    #     count = 0
    #     for row in records:
    #         count += 1
    #         # if count > 10:
    #         #     break

    #         # Get column names and values ready
    #         columns = row.keys()
    #         # Convert empty strings to None
    #         values = [row[col] if len(row[col]) > 0 else None for col in columns]

    #         # Build the insert and execute
    #         insert = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
    #             sql.SQL(table),
    #             sql.SQL(", ").join(map(sql.Identifier, columns)),
    #             sql.SQL(", ").join(map(sql.Literal, values)),
    #         )

    #         print(insert.as_string(conn))
    #         cur.execute(insert.as_string(conn))
    # headers = []

    # with open(path, "r") as csv_file:
    #     records = csv.DictReader(csv_file)
    #     headers = records.fieldnames

    # copy = sql.SQL(
    #     """COPY {}({})
    # FROM '{}'
    # DELIMITER ','
    # CSV HEADER;"""
    # ).format(
    #     sql.SQL(table), sql.SQL(", ").join(map(sql.Identifier, headers)), sql.SQL(path)
    # )

    # cur.execute(copy.as_string(conn))

    columns = None
    with open(path, "r") as csv_file:
        records = csv.DictReader(csv_file)
        columns = records.fieldnames

    # f = open(path)
    # cur.copy_from(f, table, columns=columns, sep=",")

    with open(path) as f:
        cur.copy_expert(
            "COPY {}({}) FROM STDIN WITH HEADER CSV".format(table, ",".join(columns)), f
        )

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
