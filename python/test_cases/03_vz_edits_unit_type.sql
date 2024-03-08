-- VZ user edits a unit type

-- Check cris.units and unit_type_id should update from 4 to 1
-- Check cris.crashes and unique_unit_types should update from {2,4,6} to {1,2,4,6}
UPDATE cris.unit_edit_data SET unit_type_id = 1 WHERE unit_id = 1;
