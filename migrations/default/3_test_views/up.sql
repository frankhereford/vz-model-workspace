create or replace view people_cris_view as (
    select
        db.crashes_cris.crash_id as crash_id,
        db.units_cris.id as unit_id,
        db.units_cris.unit_nbr,
        db.people_cris.id as person_id,
        db.people_cris.prsn_nbr,
        db.people_cris.is_primary as is_primary,
        db.unit_types.description as unit_type_description,
        road_types.description as road_type_description
    from
        db.people_cris
    left join db.units_cris on db.units_cris.id = db.people_cris.unit_id
    left join
        db.crashes_cris
        on db.units_cris.crash_id = db.crashes_cris.crash_id
    left join db.road_types on db.crashes_cris.road_type_id = road_types.id
    left join db.unit_types on db.units_cris.unit_type_id = db.unit_types.id
    order by crash_id asc, unit_id asc, person_id asc
);

create or replace view people_vz_view as (
    select
        db.crashes_vz.crash_id as crash_id,
        db.crashes_vz.road_type_id,
        db.units_vz.id as unit_id,
        db.units_vz.unit_nbr,
        db.units_vz.unit_type_id,
        db.people_vz.id as person_id,
        db.people_vz.prsn_nbr,
        db.people_vz.is_primary as is_primary,
        db.unit_types.description as unit_type_description,
        road_types.description as road_type_description
    from
        db.people_vz
    left join db.people_cris on db.people_vz.id = db.people_cris.id
    left join db.units_vz on db.units_vz.id = db.people_cris.unit_id
    left join db.units_cris on db.units_vz.id = db.units_cris.id
    left join db.crashes_cris on db.units_cris.crash_id = db.crashes_cris.crash_id
    left join db.crashes_vz on db.crashes_cris.crash_id = db.crashes_vz.crash_id
    left join db.road_types on db.crashes_vz.road_type_id = road_types.id
    left join db.unit_types on db.units_vz.unit_type_id = db.unit_types.id
    order by crash_id asc, unit_id asc, person_id asc
);

create or replace view people_unified_view as (
    select
        db.crashes.crash_id as crash_id,
        db.units.id as unit_id,
        db.units.unit_nbr,
        db.people.id as person_id,
        db.people.prsn_nbr,
        db.people.is_primary as is_primary,
        db.unit_types.description as unit_type_description,
        road_types.description as road_type_description
    from
        db.people
    left join db.units on db.units.id = db.people.unit_id
    left join db.crashes on db.units.crash_id = db.crashes.crash_id
    left join db.road_types on db.crashes.road_type_id = road_types.id
    left join db.unit_types on db.units.unit_type_id = db.unit_types.id
    order by crash_id asc, unit_id asc, person_id asc
);
