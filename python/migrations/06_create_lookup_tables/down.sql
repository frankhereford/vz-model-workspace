-- Drop lookup tables and foreign key constraints
ALTER TABLE cris.crash_edit_data DROP CONSTRAINT IF EXISTS fk_road_type_id;
ALTER TABLE cris.crash_cris_data DROP CONSTRAINT IF EXISTS fk_road_type_id;
ALTER TABLE cris.unit_edit_data DROP CONSTRAINT IF EXISTS fk_unit_type_id;
ALTER TABLE cris.unit_cris_data DROP CONSTRAINT IF EXISTS fk_unit_type_id;
DROP TABLE IF EXISTS cris.unit_types_lookup;
DROP TABLE IF EXISTS cris.road_types_lookup;
