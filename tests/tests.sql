-------------------------------
-- 1. Create records 
-------------------------------
-- create crash ID 1, road type is `alley`
insert into db.crashes_cris (crash_id, road_type_id)
values (1, 1);
-- create two units: vehicle and truck
insert into db.units_cris (crash_id, unit_nbr, unit_type_id)
values (1, 1, 1);
insert into db.units_cris (crash_id, unit_nbr, unit_type_id)
values (1, 2, 4);
-- add a primary and non-primary person to each unit
insert into db.people_cris (crash_id, unit_nbr, prsn_nbr, is_primary)
values (1, 1, 1, true);
insert into db.people_cris (crash_id, unit_nbr, prsn_nbr, is_primary)
values (1, 1, 2, false);
insert into db.people_cris (crash_id, unit_nbr, prsn_nbr, is_primary)
values (1, 2, 1, true);
insert into db.people_cris (crash_id, unit_nbr, prsn_nbr, is_primary)
values (1, 2, 2, false);

-- verify that cris records match inserts
select * from people_cris_view;
-- verify that vz records are blank except for IDs
select * from people_vz_view;
-- verify that unified records match cris
select * from people_unified_view;

-------------------------------
-- 2. Update crashes
-------------------------------
-- set crash road type to 3 - arterial
update db.crashes_cris set road_type_id = 3 where crash_id = 1;

-- verify that cris crash id 1 has road_type_id 3 (arterial)
select * from people_cris_view;
-- verify that vz records are blank except for IDs
select * from people_vz_view;
-- verify that unified records have road type 3 (arterial)
select * from people_unified_view;

-- override cris road type by setting vz to 4 (highway)
update db.crashes_vz set road_type_id = 4 where crash_id = 1;

-- verify that cris crash id 1 has road_type_id 3 (arterial)
select * from people_cris_view;
-- verify that vz records have road_type_id 4 (highway)
select * from people_vz_view;
-- verify that unified records have road type 4 (highway)
select * from people_unified_view;

-- revert vz road type edit
update db.crashes_vz set road_type_id = null where crash_id = 1;

-- verify that cris crash id 1 has road_type_id 3 (arterial)
select * from people_cris_view;
-- verify that vz records have are blank except for IDS
select * from people_vz_view;
-- verify that unified records have road type 4 (arterial)
select * from people_unified_view;


-------------------------------
-- 2. Update units
-------------------------------
-- update one cris unit to be bicycle 
-- (we are simulating an import here by using crash_id + unit_nbr)
update db.units_cris set unit_type_id = 5 where crash_id = 1 and unit_nbr = 1;

-- verify that cris crash id 1 unit nbr 1 has unit type 5 (bicycle)
select * from people_cris_view;
-- verify that vz records have are blank except for IDS
select * from people_vz_view;
-- erify that unified crash id 1 unit nbr 1 has unit type 5 (bicycle)
select * from people_unified_view;

-- override unit value in vz to be motorcycle
-- (we are simulating hasura by using unit.id key to update)
update db.units_vz set unit_type_id = 3 where id = 1;

-- verify that vz unit override is taking effect
select * from people_cris_view;
select * from people_vz_view;
select * from people_unified_view;

-- update cris unit value, which should have no effect
update db.units_cris set unit_type_id = 1 where crash_id = 1 and unit_nbr = 1;

-- and verify that unified table still reflects vz edit
select * from people_cris_view;
select * from people_vz_view;
select * from people_unified_view;

-- revert vz override
update db.units_vz set unit_type_id = null where id = 1;

-- and verify that unified table now reflects cris values
select * from people_cris_view;
select * from people_vz_view;
select * from people_unified_view;

-------------------------------
-- 3. Update people
-------------------------------
-- remove primary flag from one person
-- (we are simulating a cris import by using crashid + unit nbr + prsn nbr)
update db.people_cris set is_primary = false where crash_id = 1 and unit_nbr = 1 and prsn_nbr = 1;

-- verify that there is not only one primary person across all 4 people records
select * from people_cris_view;
select * from people_vz_view;
select * from people_unified_view;

-- edit vz people to set to primary
update db.people_vz set is_primary = false where id = 2;

update db.people_vz set is_primary = true where id = 1;
update db.people_vz set is_primary = null where id = 1;


