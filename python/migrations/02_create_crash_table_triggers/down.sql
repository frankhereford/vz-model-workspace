-- Drop trigger to make new rows in "cris"."crash_edit_data" and "cris"."crash_edit_data" on insert new row
DROP TRIGGER IF EXISTS crash_create_a_edit_and_computed_rows ON cris.crash_cris_data;
DROP FUNCTION IF EXISTS cris.crash_create_edit_and_computed_rows;
