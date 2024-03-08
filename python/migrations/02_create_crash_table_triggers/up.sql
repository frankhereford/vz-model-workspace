-- Add trigger to make new rows in "cris"."crash_edit_data" and "cris"."crash_edit_data" on insert new CRIS row
CREATE OR REPLACE FUNCTION cris.crash_create_edit_and_computed_rows()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO cris.crash_edit_data (crash_id) 
    VALUES (NEW.crash_id);

    INSERT INTO cris.crash_computed_data (crash_id) 
    VALUES (NEW.crash_id);

    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION cris.crash_create_edit_and_computed_rows IS 'Create matching rows in edit and computed tables with same crash_id';

CREATE OR REPLACE TRIGGER CRASH_CREATE_A_EDIT_AND_COMPUTED_ROWS
AFTER INSERT ON CRIS.CRASH_CRIS_DATA
FOR EACH ROW EXECUTE FUNCTION cris.crash_create_edit_and_computed_rows();

COMMENT ON TRIGGER CRASH_CREATE_A_EDIT_AND_COMPUTED_ROWS ON CRIS.CRASH_CRIS_DATA IS 'Create matching rows in edit and computed tables with same crash_id as new CRIS record';
