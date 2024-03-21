drop trigger if exists set_cris_person_unit_id on db.people_cris;
drop function if exists people_cris_set_unit_id;

drop trigger if exists insert_new_crashes_cris on db.crashes_cris;
drop function if exists db.crashes_cris_insert_rows;

drop trigger if exists insert_new_units_cris on db.units_cris;
drop function if exists db.units_cris_insert_rows;

drop trigger if exists insert_new_people_cris on db.people_cris;
drop function if exists db.people_cris_insert_rows;

drop trigger if exists update_crash_from_cris_crash_update on db.crashes_cris;
drop function if exists db.crashes_cris_update;

drop trigger if exists update_crash_from_vz_crash_update on db.crashes_vz;
drop function if exists db.crashes_vz_update;

drop trigger if exists update_unit_from_cris_units_update on db.units_cris;
drop function if exists units_cris_update;

drop trigger if exists update_unit_from_vz_unit_update on db.units_vz;
drop function if exists db.units_vz_update;
