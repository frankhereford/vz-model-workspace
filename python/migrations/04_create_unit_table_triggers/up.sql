-- Add trigger to make new rows in "cris"."unit_edit_data" and "cris"."unit_edit_data" on insert new CRIS row
CREATE OR REPLACE FUNCTION cris.unit_create_edit_and_computed_rows()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO cris.unit_edit_data (unit_id) 
    VALUES (NEW.unit_id);

    INSERT INTO cris.unit_computed_data (unit_id) 
    VALUES (NEW.unit_id);

    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION cris.unit_create_edit_and_computed_rows IS 'Create matching rows in edit and computed tables with same unit_id';

CREATE OR REPLACE TRIGGER unit_create_a_edit_and_computed_rows
AFTER INSERT ON cris.unit_cris_data
FOR EACH ROW EXECUTE FUNCTION cris.unit_create_edit_and_computed_rows();

COMMENT ON TRIGGER unit_create_a_edit_and_computed_rows ON cris.unit_cris_data IS 'Create matching rows in edit and computed tables with same unit_id as new CRIS record';
