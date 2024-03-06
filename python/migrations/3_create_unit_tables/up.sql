-- Unit CRIS Table Definition
CREATE TABLE cris.unit_cris_data (
    unit_id int4 NOT NULL,
    crash_id int4 NOT NULL,
    unit_type_id int4,
    PRIMARY KEY (unit_id),
    CONSTRAINT fk_crash_id
    FOREIGN KEY (crash_id)
    REFERENCES cris.crash_cris_data (crash_id)
    ON DELETE CASCADE ON UPDATE CASCADE
);

COMMENT ON TABLE cris.unit_cris_data IS 'Unit data sourced from CRIS';

-- Unit Edits Table Definition
CREATE TABLE cris.unit_edit_data (
    unit_id int4 NOT NULL,
    unit_type_id int4,
    PRIMARY KEY (unit_id)
);

COMMENT ON TABLE cris.unit_edit_data IS 'Unit data sourced from VZE edits';

-- Unit Computed Table Definition
CREATE TABLE cris.unit_computed_data (
    unit_id int4 NOT NULL,
    PRIMARY KEY (unit_id)
);

COMMENT ON TABLE cris.unit_computed_data IS 'Computed unit data sourced from CRIS, VZE edits, or both';
