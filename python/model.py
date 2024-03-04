#!/usr/bin/env python3

from lib.database import *

db = get_db_handle()
drop_schemata_except(db)
create_schemata(db)
create_lookup_tables(db)
populate_lookup_tables(db)
# set_lookup_sequences(db)
refresh_materialized_views(db)
create_fact_tables(db)
