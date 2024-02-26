-- Unit Types Lookup Table Definition
CREATE TABLE cris.unit_types_lookup (
    "id" int4 NOT NULL,
    "description" varchar(128),
    PRIMARY KEY ("id")
);

COMMENT ON TABLE cris.unit_types_lookup IS 'Lookup table for unit types';

-- Road Types Lookup Table Definition
CREATE TABLE cris.road_types_lookup (
    "id" int4 NOT NULL,
    "description" varchar(128),
    PRIMARY KEY ("id")
);

COMMENT ON TABLE cris.road_types_lookup IS 'Lookup table for crash road types';
