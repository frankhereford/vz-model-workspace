#!/usr/bin/env python3

import argparse
from lib.database import *
from lib.tests import *
from lib.colors import colors
import random


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
    parser.add_argument(
        "-s",
        "--step",
        action="store_true",
        help="Engage input() calls in the test routine for step-by-step execution",
    )
    args = parser.parse_args()

    BE_QUICK_ABOUT_IT = args.quick
    BATCH_SIZE = args.batch_size
    BUILD = args.mode == "build"
    TEST = args.mode == "test"
    STEP = args.step

    db = get_db_handle()

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
        print(f"{colors.RED}Created crash_id as a CRIS user, {crash_id}, with unit_ids: {unit_ids}{colors.ENDC}\n")
        if STEP:
            input("Press Enter to continue...")

        new_location, new_polygon_value = vz_user_changes_a_crash_location(db, crash_id)
        print(f"{colors.RED}As a VZ user, changed crash_id {crash_id} to new location as a VZ user: {new_location} which places it in {new_polygon_value}.{colors.ENDC}\n")
        if STEP:
            input("Press Enter to continue...")

        unit_id, initial_user_type, changed_unit_type = vz_user_changes_a_unit_type(db, crash_id)
        print(f"{colors.RED}As a VZ user, changed unit_type_id {initial_user_type} to {changed_unit_type} for crash_id {crash_id}, unit_id {unit_id}.{colors.ENDC}\n")
        if STEP:
            input("Press Enter to continue...")

        updated_crash_id, centroid_wkt, road_type_id, new_polygon_value = cris_user_update_crash_location_and_road_type(db, crash_id)
        print(f"{colors.RED}As a CRIS user, updated crash_id {crash_id} to new location {centroid_wkt} and road_type_id {road_type_id}, and it remains in {new_polygon_value}.{colors.ENDC}\n")
        if STEP:
            input("Press Enter to continue...")

        unit_id_cris_space, initial_unit_type_id, updated_unit_type_id = cris_user_changes_a_unit_type(db, crash_id)
        print(f"{colors.RED}As a CRIS user, changed unit_type_id {initial_unit_type_id} to {updated_unit_type_id} for crash_id {crash_id}, unit_id {unit_id_cris_space}.{colors.ENDC}\n")
        if STEP:
            input("Press Enter to continue...")

        inserted_id, lorem_word, new_road_type_id = vz_user_creates_custom_lookup_value_and_uses_it(db, crash_id)
        print(f"{colors.RED}As a VZ user, inserted a new lookup value with id {inserted_id} and value '{lorem_word}'.{colors.ENDC}\n")
        print(f"{colors.RED}As a VZ user, updated crash_id {crash_id} to use the new road_type_id {new_road_type_id}.{colors.ENDC}\n")
        if STEP:
            input("Press Enter to continue...")

        print(f"{colors.RED}\nQuery a single record for the current state of truth.{colors.ENDC}\n")
        query_a_single_crash_for_truth(db, crash_id)
        if STEP:
            input("Press Enter to continue...")

        print(f"{colors.RED}\nQuery the truth for /all/ records and select 10 to print at random.{colors.ENDC}\n")
        query_all_crashes_for_truth_and_print_ten_of_them(db)
        if STEP:
            input("Press Enter to continue...")

        print(f"{colors.RED}\nDemonstrate aggregate queries on locations. Top locations in terms of units-in-crashes desc, crashes desc.{colors.ENDC}\n")
        query_worst_locations(db)
        if STEP:
            input("Press Enter to continue...")

        print(f"{colors.RED}\nAdd an editable column to the crashes table.{colors.ENDC}\n")
        new_column = add_editable_column_to_crashes_table(db)
        if STEP:
            input("Press Enter to continue...")

        print(f"{colors.RED}\nStarting a loop to update the new column with random values and query the crash record.{colors.ENDC}\n")
        for _ in range(20):
            entity = 'cris' if random.random() < 0.5 else 'visionzero'
            print(f"{colors.RED}Updating column {new_column} with a random value as {entity} for crash_id {crash_id}.{colors.ENDC}")
            update_column_with_random_value(db, crash_id, new_column, entity)
            print(f"{colors.RED}Querying the crash record after the update.{colors.ENDC}")
            query_a_single_crash_for_truth(db, crash_id)

        print(f"{colors.RED}\nQuerying the crash history for crash_id {crash_id} and column {new_column}.{colors.ENDC}\n")
        query_a_single_crash_history(db, crash_id, new_column)


if __name__ == "__main__":
    main()
