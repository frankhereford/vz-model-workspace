-- Create a query that demonstrates the correct source of truth when crashes and units have edits from both CRIS and the VZ user
-- First update a crash row
UPDATE cris.crash_edit_data SET primary_address = 'correct address'
WHERE crash_id = 5;
-- Then, make an update to a unit in that crash
UPDATE cris.unit_edit_data SET unit_type_id = 1 WHERE unit_id = 15;
-- Query for a single crash by ID, unique unit types should now contain 1
SELECT * FROM cris.crashes WHERE crash_id = 5;
-- Query for a large number of crashes
SELECT * FROM cris.crashes;
