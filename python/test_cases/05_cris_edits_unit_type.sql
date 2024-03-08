-- CRIS user updates a unit type
-- Check cris.units and unit_type_id should update from 6 to 1
UPDATE cris.unit_cris_data SET unit_type_id = 1 WHERE unit_id = 2;
