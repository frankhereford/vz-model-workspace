-- Location Table Definition
CREATE TABLE cris.locations (
    "location_id" varchar NOT NULL,
    "description" text NOT NULL,
    "address" text,
    "metadata" json,
    "last_update" date NOT NULL DEFAULT now(),
    "is_retired" bool NOT NULL DEFAULT false,
    "is_studylocation" bool NOT NULL DEFAULT false,
    "priority_level" int4 NOT NULL DEFAULT 0,
    "shape" GEOMETRY (MULTIPOLYGON, 4326),
    "latitude" float8,
    "longitude" float8,
    "scale_factor" float8,
    "geometry" GEOMETRY (MULTIPOLYGON, 4326),
    "unique_id" varchar,
    "asmp_street_level" int4,
    "road" int4,
    "intersection" int4,
    "spine" GEOMETRY (MULTILINESTRING, 4326),
    "overlapping_geometry" GEOMETRY (MULTIPOLYGON, 4326),
    "intersection_union" int4 DEFAULT 0,
    "broken_out_intersections_union" int4 DEFAULT 0,
    "road_name" varchar(512),
    "level_1" int4 DEFAULT 0,
    "level_2" int4 DEFAULT 0,
    "level_3" int4 DEFAULT 0,
    "level_4" int4 DEFAULT 0,
    "level_5" int4 DEFAULT 0,
    "street_level" varchar(16),
    "is_intersection" int4 NOT NULL DEFAULT 0,
    "is_svrd" int4 NOT NULL DEFAULT 0,
    "council_district" int4,
    "non_cr3_report_count" int4,
    "cr3_report_count" int4,
    "total_crash_count" int4,
    "total_comprehensive_cost" int4,
    "total_speed_mgmt_points" numeric(6, 2) DEFAULT null::numeric,
    "non_injury_count" int4 NOT NULL DEFAULT 0,
    "unknown_injury_count" int4 NOT NULL DEFAULT 0,
    "possible_injury_count" int4 NOT NULL DEFAULT 0,
    "non_incapacitating_injury_count" int4 NOT NULL DEFAULT 0,
    "suspected_serious_injury_count" int4 NOT NULL DEFAULT 0,
    "death_count" int4 NOT NULL DEFAULT 0,
    "crash_history_score" numeric(4, 2) DEFAULT null::numeric,
    "sidewalk_score" int4,
    "bicycle_score" int4,
    "transit_score" int4,
    "community_dest_score" int4,
    "minority_score" int4,
    "poverty_score" int4,
    "community_context_score" int4,
    "total_cc_and_history_score" numeric(4, 2) DEFAULT null::numeric,
    "is_intersecting_district" int4 DEFAULT 0,
    "polygon_id" varchar(16),
    "signal_engineer_area_id" int4,
    "development_engineer_area_id" int4,
    "polygon_hex_id" varchar(16),
    "location_group" int2 DEFAULT 0,
    PRIMARY KEY ("location_id")
);

COMMENT ON TABLE cris.locations IS 'Locations of interest to associate with crash records';
