drop schema if exists db cascade;
create schema db;

-------------------------------------------
-------- lookup tables --------------------
-------------------------------------------
create table db.road_types (
    id integer primary key,
    description text not null,
    owner text
);

create table db.unit_types (
    id integer primary key,
    description text not null,
    owner text not null
);

insert into db.road_types (id, description, owner) values
(1, 'alley', 'cris'),
(2, 'collector', 'cris'),
(3, 'arterial', 'cris'),
(4, 'highway', 'cris'),
(5, 'other', 'cris'),
(90000, 'urban trail', 'vz');

-- the check constraints prevent us from creating cris-owned values
-- in our protected vz namespace. we will add additional constraints
-- to the _cris tables to prevent cris from using our vz lookup values
alter table db.road_types add constraint road_type_owner_check
check ((id < 90000 and owner = 'cris') or (id >= 90000 and owner = 'vz'));

insert into db.unit_types (id, description, owner) values
(1, 'vehicle', 'cris'),
(2, 'pedestrian', 'cris'),
(3, 'motorcycle', 'cris'),
(4, 'truck', 'cris'),
(5, 'bicycle', 'cris'),
(6, 'other', 'cris'),
(90000, 'e-scooter', 'vz');

alter table db.unit_types add constraint unit_type_owner_check
check ((id < 90000 and owner = 'cris') or (id >= 90000 and owner = 'vz'));

-------------------------------------------
-------- Locations table -----------------
-------------------------------------------
create table db.locations (
    location_id character varying primary key,
    geometry public.geometry (multipolygon, 4326),
    location_group integer default 0
);

create index locations_geometry_index on db.locations using gist (geometry);

-------------------------------------------
-------- Crash tables ---------------------
-------------------------------------------
create table db.crashes_cris (
    crash_id integer primary key,
    road_type_id integer references db.road_types (id),
    onsys_fl boolean,
    rr_relat_fl boolean,
    schl_bus_fl boolean,
    toll_road_fl boolean,
    private_dr_fl boolean,
    txdot_rptable_fl boolean,
    thousand_damage_fl boolean,
    road_constr_zone_fl boolean,
    medical_advisory_fl boolean,
    active_school_zone_fl boolean,
    road_constr_zone_wrkr_fl boolean,
    at_intrsct_fl boolean,
    case_id text,
    rpt_block_num text,
    rpt_sec_block_num text,
    rpt_sec_hwy_num text,
    rpt_sec_street_desc text,
    rpt_sec_street_name text,
    rpt_sec_street_pfx text,
    rpt_sec_street_sfx text,
    rpt_street_desc text,
    rpt_street_name text,
    rpt_street_pfx text,
    rpt_street_sfx text,
    rpt_hwy_num text,
    rpt_ref_mark_dir text,
    rpt_ref_mark_dist_uom text,
    surf_type_id integer,
    crash_date timestamp with time zone,
    latitude numeric,
    longitude numeric,
    rpt_ref_mark_offset_amt numeric,
    crash_speed_limit integer,
    fhe_collsn_id integer,
    light_cond_id integer,
    obj_struck_id integer,
    rpt_rdwy_sys_id integer,
    rpt_road_part_id integer,
    rpt_sec_rdwy_sys_id integer,
    rpt_sec_road_part_id integer,
    traffic_cntl_id integer,
    wthr_cond_id integer,
    intrsct_relat_id integer,
    investigat_agency_id integer,
    surf_cond_id integer,
    rpt_city_id integer,
    rpt_cris_cnty_id integer,
    investigator_narrative text,
    crash_time timestamp with time zone
);

-- this prevents cris from using a value that falls within
-- our custom lookup value namespace
alter table db.crashes_cris add constraint cris_road_type_chk
check (road_type_id < 90000);

create table db.crashes_vz (
    crash_id integer primary key not null references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    road_type_id integer references db.road_types (id),
    onsys_fl boolean,
    rr_relat_fl boolean,
    schl_bus_fl boolean,
    toll_road_fl boolean,
    private_dr_fl boolean,
    txdot_rptable_fl boolean,
    thousand_damage_fl boolean,
    road_constr_zone_fl boolean,
    medical_advisory_fl boolean,
    active_school_zone_fl boolean,
    road_constr_zone_wrkr_fl boolean,
    at_intrsct_fl boolean,
    case_id text,
    rpt_block_num text,
    rpt_sec_block_num text,
    rpt_sec_hwy_num text,
    rpt_sec_street_desc text,
    rpt_sec_street_name text,
    rpt_sec_street_pfx text,
    rpt_sec_street_sfx text,
    rpt_street_desc text,
    rpt_street_name text,
    rpt_street_pfx text,
    rpt_street_sfx text,
    rpt_hwy_num text,
    rpt_ref_mark_dir text,
    rpt_ref_mark_dist_uom text,
    surf_type_id integer,
    crash_date timestamp with time zone,
    latitude numeric,
    longitude numeric,
    rpt_ref_mark_offset_amt numeric,
    crash_speed_limit integer,
    fhe_collsn_id integer,
    light_cond_id integer,
    obj_struck_id integer,
    rpt_rdwy_sys_id integer,
    rpt_road_part_id integer,
    rpt_sec_rdwy_sys_id integer,
    rpt_sec_road_part_id integer,
    traffic_cntl_id integer,
    wthr_cond_id integer,
    intrsct_relat_id integer,
    investigat_agency_id integer,
    surf_cond_id integer,
    rpt_city_id integer,
    rpt_cris_cnty_id integer,
    investigator_narrative text,
    crash_time timestamp with time zone
);

create table db.crashes (
    crash_id integer unique not null references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    road_type_id integer references db.road_types (id),
    onsys_fl boolean,
    rr_relat_fl boolean,
    schl_bus_fl boolean,
    toll_road_fl boolean,
    private_dr_fl boolean,
    txdot_rptable_fl boolean,
    thousand_damage_fl boolean,
    road_constr_zone_fl boolean,
    medical_advisory_fl boolean,
    active_school_zone_fl boolean,
    road_constr_zone_wrkr_fl boolean,
    at_intrsct_fl boolean,
    case_id text,
    rpt_block_num text,
    rpt_sec_block_num text,
    rpt_sec_hwy_num text,
    rpt_sec_street_desc text,
    rpt_sec_street_name text,
    rpt_sec_street_pfx text,
    rpt_sec_street_sfx text,
    rpt_street_desc text,
    rpt_street_name text,
    rpt_street_pfx text,
    rpt_street_sfx text,
    rpt_hwy_num text,
    rpt_ref_mark_dir text,
    rpt_ref_mark_dist_uom text,
    surf_type_id integer,
    crash_date timestamp with time zone,
    latitude numeric,
    longitude numeric,
    rpt_ref_mark_offset_amt numeric,
    crash_speed_limit integer,
    fhe_collsn_id integer,
    light_cond_id integer,
    obj_struck_id integer,
    rpt_rdwy_sys_id integer,
    rpt_road_part_id integer,
    rpt_sec_rdwy_sys_id integer,
    rpt_sec_road_part_id integer,
    traffic_cntl_id integer,
    wthr_cond_id integer,
    intrsct_relat_id integer,
    investigat_agency_id integer,
    surf_cond_id integer,
    rpt_city_id integer,
    rpt_cris_cnty_id integer,
    investigator_narrative text,
    crash_time timestamp with time zone,
    geog geography,
    location_id text references db.locations (location_id)
);


-------------------------------------------
-------- Unit tables ----------------------
-------------------------------------------
create table db.units_cris (
    id serial primary key,
    crash_id integer not null references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    unit_nbr integer not null,
    unit_type_id integer references db.unit_types (id),
    constraint unique_units_cris unique (crash_id, unit_nbr)
);

alter table db.units_cris add constraint cris_unit_type_chk
check (unit_type_id < 90000);


create table db.units_vz (
    id integer primary key references db.units_cris (
        id
    ) on delete cascade on update restrict,
    crash_id integer references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    unit_nbr integer,
    unit_type_id integer references db.unit_types (id),
    constraint unique_units_vz unique (crash_id, unit_nbr)
);

create table db.units (
    id integer primary key references db.units_cris (id)
    on delete cascade on update restrict,
    crash_id integer not null references db.crashes (
        crash_id
    ) on delete cascade on update restrict,
    unit_nbr integer not null,
    unit_type_id integer references db.unit_types (id),
    constraint unique_units unique (crash_id, unit_nbr)
);

-------------------------------------------
-------- People tables --------------------
-------------------------------------------
create table db.people_cris (
    id serial primary key,
    unit_id integer references db.units_cris (
        id
    ) on delete cascade on update restrict,
    crash_id integer references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    prsn_nbr integer not null,
    unit_nbr integer not null,
    is_primary boolean default false,
    constraint unique_people_cris unique (unit_id, prsn_nbr)
);

create index on db.people_cris (unit_id);
create index on db.people_cris (crash_id);

create table db.people_vz (
    id integer references db.people_cris (id)
    on delete cascade on update restrict,
    unit_id integer references db.units_cris (
        id
    ),
    prsn_nbr integer,
    unit_nbr integer,
    is_primary boolean
);

create index on db.people_vz (id);
create index on db.people_vz (unit_id);

create table db.people (
    id integer references db.people_cris (id)
    on delete cascade on update restrict,
    unit_id integer not null references db.units (
        id
    ),
    prsn_nbr integer,
    unit_nbr integer,
    is_primary boolean,
    constraint unique_people unique (unit_id, prsn_nbr)
);

create index on db.people (id);
create index on db.people (unit_id);

-------------------------------------------
-------- Change log -----------------------
-------------------------------------------
create table db.change_log (
    id serial primary key,
    record_id integer not null,
    record_type text not null,
    operation_type text not null,
    record_json jsonb not null,
    created_at timestamp with time zone default now(),
    created_by text not null
);

create index on db.change_log (record_id, record_type);
