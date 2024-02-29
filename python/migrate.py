#!/usr/bin/env python3
"""
Apply migrations to VZ Data Model DB
"""
import argparse
import csv
import os

import psycopg2
from psycopg2 import sql

DBNAME = "visionzero"
USER = "vz"
PASSWORD = "vz"
HOST = "db"
PORT = "5432"


def apply_sql(path):
    conn = psycopg2.connect(
        dbname=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT
    )
    cur = conn.cursor()

    cur.executescript(open(path, "r").read())

    conn.commit()
    conn.close()


def main(args):
    direction = args.direction
    # TODO: Test applying up and down migrations
    directory_list = os.listdir("./migrations")

    # up
    if direction == "up":
        directory_list.sort()

    # down
    if direction == "down":
        directory_list.sort(reverse=True)

    for directory in directory_list:
        print(f"Applying ./migrations/{directory}/{direction}.sql")
        # apply_sql(f"./migrations/{file}/{direction}.sql")

    print(f"Migrate {args.direction} complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--direction", type=str, help="a migration direction")
    args = parser.parse_args()

    main(args)
