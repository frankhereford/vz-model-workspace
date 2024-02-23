-- Add trigger to make new rows in "cris"."crash_edit_data" and "cris"."crash_edit_data" on insert new CRIS row
CREATE OR REPLACE FUNCTION cris.crash_create_edit_and_computed_rows()
RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
BEGIN
    INSERT INTO cris.crash_edit_data (crash_id) 
    VALUES (NEW.crash_id);

    INSERT INTO cris.crash_computed_data (crash_id) 
    VALUES (NEW.crash_id);

    RETURN NEW;
END;
$$

COMMENT ON FUNCTION cris.crash_create_edit_and_computed_rows IS 'Create matching rows in edit and computed tables with same crash_id';

CREATE OR REPLACE TRIGGER crash_create_a_edit_and_computed_rows
AFTER INSERT ON cris.crash_cris_data
FOR EACH ROW EXECUTE FUNCTION cris.crash_create_edit_and_computed_rows();

COMMENT ON TRIGGER crash_create_a_edit_and_computed_rows ON cris.crash_cris_data IS 'Create matching rows in edit and computed tables with same crash_id as new CRIS record';

-- Triggers to populate "cris"."crash_computed_data"

-- Add triggers to calculate position from latitude and longitude on crash insert and also crash edit update
CREATE OR REPLACE FUNCTION cris.crash_create_position()
RETURNS trigger LANGUAGE plpgsql
AS $$
DECLARE
    updated_position GEOMETRY (GEOMETRY, 4326);
BEGIN
   updated_position = ST_SetSRID(ST_Point(NEW.longitude, NEW.latitude), 4326);
   
   UPDATE cris.crash_computed_data
   SET position = updated_position 
   WHERE crash_id = NEW.crash_id;

   RETURN NEW;
END;
$$

COMMENT ON FUNCTION cris.crash_create_position IS 'Calculate position from latitude and longitude';

CREATE OR REPLACE TRIGGER crash_create_b_cris_position
AFTER INSERT ON cris.crash_cris_data
FOR EACH ROW EXECUTE FUNCTION cris.crash_create_position();

COMMENT ON TRIGGER crash_create_b_cris_position ON cris.crash_cris_data IS 'Create position from latitude and longitude on insert';

CREATE OR REPLACE TRIGGER crash_create_edit_position
AFTER UPDATE ON cris.crash_edit_data
FOR EACH ROW EXECUTE FUNCTION cris.crash_create_position();

COMMENT ON TRIGGER crash_create_edit_position on cris.crash_edit_data IS 'Create position from latitude and longitude on update';
