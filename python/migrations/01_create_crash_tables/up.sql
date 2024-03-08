-- Create CRIS schema
CREATE SCHEMA cris;

-- Crash CRIS Table Definition
CREATE TABLE cris.crash_cris_data (
    "crash_id" int4 NOT NULL,
    "latitude" float8,
    "longitude" float8,
    "primary_address" text,
    "road_type_id" int4,
    "non_editable_column" text,
    PRIMARY KEY ("crash_id")
);

COMMENT ON TABLE cris.crash_cris_data IS 'Crash data sourced from CRIS';

-- Crash Edits Table Definition
CREATE TABLE cris.crash_edit_data (
    "crash_id" int4 NOT NULL,
    "latitude" float8,
    "longitude" float8,
    "primary_address" text,
    "road_type_id" int4,
    PRIMARY KEY ("crash_id")
);

COMMENT ON TABLE cris.crash_edit_data IS 'Crash data sourced from VZE edits';

-- Crash Computed Table Definition
CREATE TABLE cris.crash_computed_data (
    "crash_id" int4 NOT NULL,
    "location_id" varchar,
    "unique_unit_types" integer[],
    PRIMARY KEY ("crash_id")
);

COMMENT ON TABLE cris.crash_computed_data IS
'Computed crash data sourced from CRIS, VZE edits, or both';
