-- Create CRIS schema
CREATE SCHEMA cris;

-- Crash CRIS Data Table Definition
CREATE TABLE "cris"."crash_cris_data" (
    "crash_id" int4 NOT NULL,
    "road_type_id" int4,
    "latitude" float8,
    "longitude" float8,
    "address_confirmed_primary" text,
    PRIMARY KEY ("crash_id")
);

-- Crash Edits Table Definition
CREATE TABLE "cris"."crash_edit_data" (
    "crash_id" int4 NOT NULL,
    "road_type_id" int4,
    "unique_unit_types" jsonb,
    "latitude" float8,
    "longitude" float8,
    "location_id" varchar,
    "address_confirmed_primary" text,
    PRIMARY KEY ("crash_id")
);

-- Crash Computed Table Definition
CREATE TABLE "cris"."crash_computed_data" (
    "crash_id" int4 NOT NULL,
    "position" GEOMETRY (GEOMETRY, 4326),
    "location_id" varchar,
    PRIMARY KEY ("crash_id")
);
