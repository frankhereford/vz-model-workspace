#!/usr/bin/env python3

import argparse
from lib.database import *
from lib.tests import *
from lib.colors import colors


def main():
    parser = argparse.ArgumentParser(description="Database setup and testing script")
    parser.add_argument(
        "-q", "--quick", action="store_true", help="Perform quick setup"
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=1e5,
        help="Batch size for populating fact tables",
    )
    parser.add_argument(
        "-m",
        "--mode",
        choices=["build", "test"],
        default="build",
        help="Mode: build or test",
    )
    args = parser.parse_args()

    BE_QUICK_ABOUT_IT = args.quick
    BATCH_SIZE = args.batch_size
    BUILD = args.mode == "build"
    TEST = args.mode == "test"

    db = get_db_handle()

    # todo - add maintenance functions to VZ side, so you can insert on it as well and get the empty cris record

    # fmt: off
    if BUILD is True:
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
        crash_id, unit_ids = cris_user_creates_crash_record_with_two_unit_records(db)
        print(f"{colors.RED}Created crash_id as a CRIS user, {crash_id}, with unit_ids: {unit_ids}{colors.ENDC}")
        input()

        new_location, new_polygon_value = vz_user_changes_a_crash_location(db, crash_id)
        print(f"{colors.RED}As a VZ user, changed crash_id {crash_id} to new location as a VZ user: {new_location} which places it in {new_polygon_value}.{colors.ENDC}")
        input()

        unit_id, initial_user_type, changed_unit_type = vz_user_changes_a_unit_type(db, crash_id)
        print(f"{colors.RED}As a VZ user, changed unit_type_id {initial_user_type} to {changed_unit_type} for crash_id {crash_id}, unit_id {unit_id}.{colors.ENDC}")
        input()

        updated_crash_id, centroid_wkt, road_type_id, new_polygon_value = cris_user_update_crash_location_and_road_type(db, crash_id)
        print(f"{colors.RED}As a CRIS user, updated crash_id {crash_id} to new location {centroid_wkt} and road_type_id {road_type_id}, and it remains in {new_polygon_value}.{colors.ENDC}")
        input()


if __name__ == "__main__":
    main()
