-- Unit Types Lookup Table Definition
ALTER TABLE cris.crash_edit_data DROP CONSTRAINT IF EXISTS fk_road_type_id;
DROP TABLE IF EXISTS cris.unit_types_lookup;
DROP TABLE IF EXISTS cris.road_types_lookup;
