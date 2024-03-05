#!/usr/bin/env python3

from lib.database import *
from lib.tests import *

BE_QUICK_ABOUT_IT = True
BATCH_SIZE = 1e5

BUILD = True
TEST = False

db = get_db_handle()


# fmt: off
if BUILD is True:
    # create extension periods cascade;
    disconnect_other_users(db)
    drop_schemata_except(db)
    drop_public_entities(db)
    ensure_extensions_exists(db)
    pull_down_locations(db)
    create_schemata(db)
    create_lookup_tables(db)
    artifically_descync_sequences_from_cris_data(db, ["cris_lookup.road_types_sequence", "cris_lookup.unit_types_sequence"])
    populate_lookup_tables(db)
    refresh_materialized_views(db)
    create_fact_tables(db)
    create_cris_facts_functions(db)
    create_cris_facts_triggers(db)
    create_temporal_tracking(db)
    artifically_descync_sequences_from_cris_data(db, ["cris_facts.crashes_id_seq", "cris_facts.units_id_seq","visionzero_facts.crashes_id_seq", "visionzero_facts.units_id_seq"])
    create_lookup_table_substitution_triggers(db)
    populate_fact_tables(db, BE_QUICK_ABOUT_IT, batch_size=BATCH_SIZE)
    create_unifying_fact_views(db)


if TEST is True:
    cris_user_creates_crash_record_with_two_unit_records(db)
