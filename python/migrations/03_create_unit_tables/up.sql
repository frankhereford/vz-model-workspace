-- Unit CRIS Table Definition
CREATE TABLE cris.unit_cris_data (
    unit_id int4 NOT NULL,
    unit_type_id int4,
    crash_id int4 NOT NULL,
    PRIMARY KEY (unit_id)
);

COMMENT ON TABLE cris.unit_cris_data IS 'Unit data sourced from CRIS';

CREATE INDEX ON cris.unit_cris_data (crash_id);

-- Unit Edits Table Definition
CREATE TABLE cris.unit_edit_data (
    unit_id int4 NOT NULL,
    unit_type_id int4,
    PRIMARY KEY (unit_id)
);

CREATE INDEX ON cris.unit_edit_data (unit_type_id);

COMMENT ON TABLE cris.unit_edit_data IS 'Unit data sourced from VZE edits';

-- Unit Computed Table Definition
CREATE TABLE cris.unit_computed_data (
    unit_id int4 NOT NULL,
    PRIMARY KEY (unit_id)
);

COMMENT ON TABLE cris.unit_computed_data IS 'Computed unit data sourced from CRIS, VZE edits, or both';
