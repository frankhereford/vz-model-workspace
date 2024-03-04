#!/usr/bin/env python3

from lib.database import *

db = get_db_handle()

drop_schemata_except(db)
create_schemata(db)
create_lookup_tables(db)
artifically_descync_sequences_from_cris_data(db)
populate_lookup_tables(db)
refresh_materialized_views(db)
create_fact_tables(db)
create_cris_facts_functions(db)
create_cris_facts_triggers(db)
create_lookup_table_substitution_triggers(db)
populate_fact_tables(db)
