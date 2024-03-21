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
(9000000, 'urban trail', 'vz');

-- the check constraints prevent us from creating cris-owned values
-- in our protected vz namespace. we will add additional constraints
-- to the _cris tables to prevent cris from using our vz lookup values
alter table db.road_types add constraint road_type_owner_check
check ((id < 9000000 and owner = 'cris') or (id >= 9000000 and owner = 'vz'));

insert into db.unit_types (id, description, owner) values
(1, 'vehicle', 'cris'),
(2, 'pedestrian', 'cris'),
(3, 'motorcycle', 'cris'),
(4, 'spaceship', 'cris'),
(5, 'bicycle', 'cris'),
(6, 'other', 'cris'),
(9000000, 'e-scooter', 'vz');

alter table db.unit_types add constraint unit_type_owner_check
check ((id < 9000000 and owner = 'cris') or (id >= 9000000 and owner = 'vz'));

-------------------------------------------
-------- Crash tables ---------------------
-------------------------------------------
create table db.crashes_cris (
    crash_id integer primary key,
    road_type_id integer references db.road_types (id)
);

-- this prevents cris from using a value that falls within
-- our custom lookup value namespace
alter table db.crashes_cris add constraint cris_road_type_chk
check (road_type_id < 9000000);

create table db.crashes_vz (
    crash_id integer not null references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    road_type_id integer references db.road_types (id)
);

create table db.crashes (
    crash_id integer unique not null references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    road_type_id integer references db.road_types (id)
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
check (unit_type_id < 9000000);

create table db.units_vz (
    id integer primary key references db.units_cris (id),
    crash_id integer references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    unit_nbr integer,
    unit_type_id integer references db.unit_types (id),
    constraint unique_units_vz unique (crash_id, unit_nbr)
);

create table db.units (
    id integer primary key references db.units_cris (id),
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
    constraint unique_people_cris unique (unit_id, prsn_nbr),
    constraint unique_people_cris unique (crash_id, prsn_nbr)
);

create index on db.people_cris (unit_id);

create table db.people_vz (
    id integer references db.people_cris (id),
    unit_id integer references db.units_cris (
        id
    ),
    -- crash_id integer references db.crashes_cris (crash_id),
    prsn_nbr integer,
    unit_nbr integer,
    is_primary boolean
);

create index on db.people_vz (unit_id);

create table db.people (
    id integer references db.people_cris (id),
    unit_id integer not null references db.units (
        id
    ),
    -- crash_id integer references db.crashes (crash_id),
    prsn_nbr integer,
    unit_nbr integer,
    is_primary boolean,
    constraint unique_people unique (unit_id, prsn_nbr)
);

create index on db.people (unit_id);

-------------------------------------------
-------- Triggers -------------------------
-------------------------------------------
--
-- sets a person record's unit_id column based on the crash_id and unit_nbr
-- the crash_id column exists on the people_cris table, but not on the
-- people_vz or people tables
--
create or replace function db.people_cris_set_unit_id()
returns trigger
language plpgsql
as $$
DECLARE
   unit_record record;
BEGIN
    SELECT INTO unit_record *
        FROM db.units_cris where crash_id = new.crash_id and unit_nbr = new.unit_nbr;
    new.unit_id = unit_record.id;
    RETURN new;
END;
$$;

create trigger set_cris_person_unit_id
before insert on db.people_cris
for each row
execute procedure db.people_cris_set_unit_id();

--
-- handles when a new cris crash record is inserted by copying
-- to the vz_ and unified tables
--
create or replace function db.crashes_cris_insert_rows()
returns trigger
language plpgsql
as
$$
BEGIN
    -- insert new unified/truth record
    INSERT INTO db.crashes (crash_id, road_type_id) values (
        new.crash_id, new.road_type_id
    );
    -- insert new (editable) vz record (only record ID)
    INSERT INTO db.crashes_vz (crash_id) values (new.crash_id);
    RETURN NULL;
END;
$$;

create or replace trigger insert_new_crashes_cris
after insert on db.crashes_cris
for each row
execute procedure db.crashes_cris_insert_rows();

--
-- handles when a new unit crash record is inserted by copying
-- to the vz_ and unified tables
--
create or replace function db.units_cris_insert_rows()
returns trigger
language plpgsql
as
$$
BEGIN
    -- insert new unified/truth record
    INSERT INTO db.units (id, crash_id, unit_nbr, unit_type_id) values (
        new.id, new.crash_id, new.unit_nbr, new.unit_type_id
    );
    -- insert new (editable) vz record (only record ID)
    INSERT INTO db.units_vz (id) values (new.id);
    RETURN NULL;
END;
$$;

create or replace trigger insert_new_units_cris
after insert on db.units_cris
for each row
execute procedure db.units_cris_insert_rows();

--
-- handles when a new cris people record is inserted by copying
-- to the vz_ and unified tables
--
create or replace function db.people_cris_insert_rows()
returns trigger
language plpgsql
as
$$
BEGIN
    -- insert new unified/truth record
    INSERT INTO db.people (id, unit_id, prsn_nbr, unit_nbr, is_primary) values (
        new.id, new.unit_id, new.prsn_nbr, new.unit_nbr, new.is_primary
    );
    -- insert new (editable) vz record (only record ID)
    INSERT INTO db.people_vz (id) values (new.id);
    RETURN NULL;
END;
$$;

create or replace trigger insert_new_people_cris
after insert on db.people_cris
for each row
execute procedure db.people_cris_insert_rows();
