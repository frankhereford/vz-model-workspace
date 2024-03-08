-- Create a test case, or simply write out the steps that would be involved if we wanted to add a new editable column to crashes

-- Add the new column to the CRIS and edit tables
ALTER TABLE cris.crash_cris_data ADD secondary_address text;
ALTER TABLE cris.crash_edit_data ADD secondary_address text;

-- Drop and recreate the crashes view
DROP VIEW IF EXISTS cris.crashes;

CREATE OR REPLACE VIEW cris.crashes AS
SELECT * FROM cris.generate_crash_cris_and_edits_query()
LEFT JOIN cris.crash_computed_data USING ("crash_id");
