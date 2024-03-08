-- Unit Types Lookup Table Definition
CREATE TABLE cris.unit_types_lookup (
    "id" int4 NOT NULL,
    "description" varchar(128),
    PRIMARY KEY ("id")
);

COMMENT ON TABLE cris.unit_types_lookup IS 'Lookup table for unit types';

ALTER TABLE cris.unit_cris_data ADD CONSTRAINT fk_unit_type_id
FOREIGN KEY (unit_type_id)
REFERENCES cris.unit_types_lookup (id)
ON DELETE SET NULL;

ALTER TABLE cris.unit_edit_data ADD CONSTRAINT fk_unit_type_id
FOREIGN KEY (unit_type_id)
REFERENCES cris.unit_types_lookup (id)
ON DELETE SET NULL;

-- Road Types Lookup Table Definition
CREATE TABLE cris.road_types_lookup (
    "id" int4 NOT NULL,
    "description" varchar(128),
    PRIMARY KEY ("id")
);

COMMENT ON TABLE cris.road_types_lookup IS 'Lookup table for crash road types';

-- Add foreign key constraint on crash edit table to road types lookup table
ALTER TABLE cris.crash_edit_data ADD CONSTRAINT fk_road_type_id
FOREIGN KEY (road_type_id)
REFERENCES cris.road_types_lookup (id)
ON DELETE SET NULL;

ALTER TABLE cris.crash_cris_data ADD CONSTRAINT fk_road_type_id
FOREIGN KEY (road_type_id)
REFERENCES cris.road_types_lookup (id)
ON DELETE SET NULL;
