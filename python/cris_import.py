import csv
from datetime import datetime
import io
import os
import time

import pytz
import requests

from columns import COLUMNS

FILE_DIR = "cris_csvs"
HASURA_ENDPOINT = "http://localhost:8084/v1/graphql"
UPLOAD_BATCH_SIZE = 1000

CRASH_UPSERT_MUTATION = """
mutation UpsertCrashes($objects: [db_crashes_cris_insert_input!]!) {
  insert_db_crashes_cris(
    objects: $objects, 
    on_conflict: {
        constraint: crashes_cris_pkey,
        update_columns: [$updateColumns]
    }) {
    affected_rows
  }
}
"""

UNIT_UPSERT_MUTATION = """
mutation UpsertUnits($objects: [db_units_cris_insert_input!]!) {
  insert_db_units_cris(
    objects: $objects, 
    on_conflict: {
        constraint: unique_units_cris,
        update_columns: [$updateColumns]
    }) {
    affected_rows
  }
}
"""

PERSON_UPSERT_MUTATION = """
mutation UpsertPeople($objects: [db_people_cris_insert_input!]!) {
  insert_db_people_cris(
    objects: $objects, 
    on_conflict: {
        constraint: unique_people_cris,
        update_columns: [$updateColumns]
    }) {
    affected_rows
  }
}"""

mutations = {
    "crashes": CRASH_UPSERT_MUTATION,
    "units": UNIT_UPSERT_MUTATION,
    "persons": PERSON_UPSERT_MUTATION,
}


def make_upsert_mutation(table_name):
    """Sets the column names in the upsert on conflict array"""
    upsert_mutation = mutations[table_name]
    updateColumns = [
        col["name"]
        for col in COLUMNS
        if col["table_name"] == table_name
        and col["source"] == "cris"
        and not col["is_primary_key"]
    ]
    return upsert_mutation.replace("$updateColumns", ", ".join(updateColumns))


def make_hasura_request(*, query, endpoint, variables=None):
    payload = {"query": query, "variables": variables}
    res = requests.post(endpoint, json=payload)
    res.raise_for_status()
    data = res.json()
    try:
        return data["data"]
    except KeyError:
        raise ValueError(data)


def get_file_meta(filename):
    """
    Returns:
        schema_year, extract_id, table_name
    """
    filename_parts = filename.split("_")
    return filename_parts[1], filename_parts[2], filename_parts[3]


def get_files_todo():
    files_todo = []
    for filename in os.listdir(FILE_DIR):
        if not filename.endswith(".csv"):
            continue
        schema_year, extract_id, table_name = get_file_meta(filename)

        # ignore all tables except these
        if table_name not in ("crash", "unit", "person", "primaryperson"):
            continue

        file = {
            "schema_year": schema_year,
            "table_name": table_name,
            "path": os.path.join(FILE_DIR, filename),
            "extract_id": extract_id,
        }
        files_todo.append(file)
    return files_todo


def load_csv(filename):
    with open(filename, "r") as fin:
        reader = csv.DictReader(fin)
        return [row for row in reader]


def lower_case_keys(list_of_dicts):
    return [{key.lower(): value for key, value in row.items()} for row in list_of_dicts]


def remove_unsupported_columns(rows, table_name):
    col_names_to_keep = [
        col["name"]
        for col in COLUMNS
        if col["table_name"] == table_name and col["source"] == "cris"
    ]
    return [
        {key: val for key, val in row.items() if key in col_names_to_keep}
        for row in rows
    ]


def handle_empty_strings(rows):
    for row in rows:
        for key, val in row.items():
            if val == "":
                row[key] = None
    return rows


def handle_crash_date_time(rows):
    """Combine crash_date and crash_time and format as ISO string.

    The same ISO timestamp is stored as `crash_date` and `crash_time`
    """
    tz = pytz.timezone("US/Central")
    for row in rows:
        # parse a string that looks like '12/22/2023'
        crash_date = row["crash_date"]
        month, day, year = crash_date.split("/")
        # parse a string that looks like '12:19 PM'
        crash_time = row["crash_time"]
        hour_minute, am_pm = crash_time.split(" ")
        hour, minute = hour_minute.split(":")
        if am_pm.lower() == "pm":
            hour = int(hour) + 12 if hour != "12" else int(hour)
        else:
            hour = int(hour) if hour != "12" else 0
        # create a tz-aware instance of this datetime
        crash_date_dt = tz.localize(
            datetime(int(year), int(month), int(day), hour=hour, minute=int(minute))
        )
        # save the ISO string with tz offset
        crash_date_iso = crash_date_dt.isoformat()
        row["crash_date"] = crash_date_iso
        row["crash_time"] = crash_date_iso


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def main():
    files_todo = get_files_todo()
    extract_ids = list(set([f["extract_id"] for f in files_todo]))
    # assumes extract ids are sortable oldest > newest by filename. todo: is that right?
    extract_ids.sort()
    for extract_id in extract_ids:
        # get schema years that match this extract ID
        schema_years = list(
            set([f["schema_year"] for f in files_todo if f["extract_id"] == extract_id])
        )
        schema_years.sort()
        for schema_year in schema_years:
            for table_name in ["crashes", "units", "persons"]:
                file = next(
                    (
                        f
                        for f in files_todo
                        if f["extract_id"] == extract_id
                        and f["schema_year"] == schema_year
                        and table_name.startswith(f["table_name"])
                    ),
                    None,
                )

                if not file:
                    raise (
                        f"No {table_name} file found in extract. This should never happen!"
                    )

                print(f"processing {table_name}")

                records = handle_empty_strings(
                    remove_unsupported_columns(
                        lower_case_keys(load_csv(file["path"])), table_name
                    )
                )

                if table_name == "crashes":
                    handle_crash_date_time(records)

                # annoying redundant branch to combine primary persons and persons
                if table_name == "persons":

                    p_person_file = next(
                        (
                            f
                            for f in files_todo
                            if f["extract_id"] == extract_id
                            and f["schema_year"] == schema_year
                            and f["table_name"].startswith("primaryperson")
                        ),
                        None,
                    )

                    for p in records:
                        p["is_primary"] = False

                    pp_records = handle_values(
                        remove_unsupported_columns(
                            lower_case_keys(load_csv(p_person_file["path"])),
                            table_name,
                        )
                    )
                    for pp in pp_records:
                        pp["is_primary"] = True
                    records = pp_records + records

                upsert_mutation = make_upsert_mutation(table_name)

                for chunk in chunks(records, UPLOAD_BATCH_SIZE):
                    print("uploading records...")
                    start_time = time.time()
                    res = make_hasura_request(
                        endpoint=HASURA_ENDPOINT,
                        query=upsert_mutation,
                        variables={"objects": chunk},
                    )
                    print(f"Completed in {time.time() - start_time}")
main()
