-- VZ user adds a custom lookup value and uses it

-- Check cris.unit_types_lookup and the custom value should be added
-- Check cris.units and unit_type_id should update from 999 to 10
-- Check cris.crashes and unique_unit_types should update from {2,3,4,5} to {2,3,5,999}
INSERT INTO cris.unit_types_lookup ("id", "description") VALUES
(999, 'custom value');

UPDATE cris.unit_edit_data SET unit_type_id = 999 WHERE unit_id = 10;
