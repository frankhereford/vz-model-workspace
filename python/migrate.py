#!/usr/bin/env python3
"""
Apply migrations to VZ Data Model DB
"""
import csv
import argparse
from os import walk

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
    # TODO: Add direction arg (up or down migration)
    # TODO: Add method to get directories to iterate and apply either down or up

    # TODO: Iterate directories numerically an and call apply_sql on each up or down files

    print(f"Migrate {args.direction} complete.")


if __name__ == "__main__":
    main()
