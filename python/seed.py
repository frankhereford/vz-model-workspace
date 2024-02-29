#!/usr/bin/env python3
"""
Seed VZ Data Model DB with data from CSV files
"""
import psycopg2
from psycopg2 import sql
import csv

DBNAME = "visionzero"
USER = "vz"
PASSWORD = "vz"
HOST = "db"
PORT = "5432"


def seed(path, table):
    conn = psycopg2.connect(
        dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT
    )
    cur = conn.cursor()

    with open(path, "r") as csv_file:
        records = csv.DictReader(csv_file)

        count = 0
        for row in records:
            count += 1
            if count > 10:
                break

            # Get column names and values ready
            columns = row.keys()
            # Convert empty strings to None
            values = [row[col] if len(row[col]) > 0 else None for col in columns]

            # Build the insert and execute
            insert = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.SQL(table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(map(sql.Literal, values)),
            )

            print(insert.as_string(conn))
            cur.execute(insert.as_string(conn))

    conn.commit()
    conn.close()


def main():
    seed(path="./csv/crashes.csv", table="cris.crash_cris_data")
    seed(path="./csv/units.csv", table="cris.unit_cris_data")
    seed(path="./csv/locations.csv", table="cris.locations")
    seed(path="./csv/unit_types.csv", table="cris.unit_types_lookup")
    seed(path="./csv/road_types.csv", table="cris.road_types_lookup")

    print("Seeding complete.")


if __name__ == "__main__":
    main()
