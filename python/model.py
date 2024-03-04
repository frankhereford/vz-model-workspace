#!/usr/bin/env python3

from lib.database import *

db = get_db_handle()
drop_schemata_except(db, [])
create_schemata(
    db,
    [
        "cris_facts",
        "visionzero_facts",
        "cris_lookup",
        "visionzero_lookup",
    ],
)
# create_fact_tables(db)
