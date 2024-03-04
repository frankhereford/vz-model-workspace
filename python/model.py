#!/usr/bin/env python3

from lib.database import *

db = get_db_handle()
drop_schemata_except(db, ["public", "tiger", "tiger_data", "topology"])
create_schemata(db, ["cris_facts", "visionzero_facts"])
