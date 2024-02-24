-- Unit CRIS Table Definition
CREATE TABLE cris.unit_cris_data (
    unit_id int4 NOT NULL,
    crash_id int4 NOT NULL,
    unit_type_id int4,
    PRIMARY KEY (unit_id)
);

-- Unit Edits Table Definition
CREATE TABLE cris.unit_edit_data (
    unit_id int4 NOT NULL,
    unit_type_id int4,
    PRIMARY KEY (unit_id)
);

-- Unit Computed Table Definition
CREATE TABLE cris.unit_computed_data (
    unit_id int4 NOT NULL,
    PRIMARY KEY (unit_id)
);
