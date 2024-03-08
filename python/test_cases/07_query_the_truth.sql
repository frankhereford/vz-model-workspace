-- Create a query that demonstrates the correct source of truth when crashes and units have edits from both CRIS and the VZ user

-- Query for a single crash by ID and notice its primary address and unique unit types
SELECT * FROM cris.crashes WHERE crash_id = 5;
-- Make a VZ update the crash row
UPDATE cris.crash_edit_data SET primary_address = 'correct address'
WHERE crash_id = 5;
-- Then, make an update to a unit in that crash
UPDATE cris.unit_edit_data SET unit_type_id = 1 WHERE unit_id = 15;
-- Query for a single crash by ID, unique unit types should now contain 1
SELECT * FROM cris.crashes WHERE crash_id = 5;

-- Query for a large number of crashes
SELECT * FROM cris.crashes;
-- OR with a filter
SELECT * FROM cris.crashes WHERE location_id IS NOT NULL;
-- OR with more than one filter
SELECT * FROM cris.crashes WHERE location_id IS NOT NULL AND road_type_id > 1;
