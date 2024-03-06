-- Drop location calculation triggers
DROP TRIGGER IF EXISTS crash_create_b_cris_location ON cris.crash_cris_data;
DROP TRIGGER IF EXISTS crash_create_edit_location ON cris.crash_edit_data;
DROP FUNCTION IF EXISTS cris.crash_create_location;
