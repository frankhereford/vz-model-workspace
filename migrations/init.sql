drop schema if exists db cascade;
create schema db;

-------------------------------------------
-------- Crash tables ---------------------
-------------------------------------------
create table db.crashes_cris (
    crash_id integer primary key,
    test_column text
);

create table db.crashes_vz (
    crash_id integer not null references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    test_column text
);

create table db.crashes (
    crash_id integer unique not null references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    test_column text
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
    constraint unique_units_cris unique (crash_id, unit_nbr)
);

create table db.units_vz (
    id integer primary key references db.units_cris (id),
    crash_id integer references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    unit_nbr integer,
    constraint unique_units_vz unique (crash_id, unit_nbr)
);

create table db.units (
    id integer primary key references db.units_cris (id),
    crash_id integer not null references db.crashes (
        crash_id
    ) on delete cascade on update restrict,
    unit_nbr integer not null,
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
    INSERT INTO db.crashes (crash_id, test_column) values (
        new.crash_id, new.test_column
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
-- handles when a new cris crash record is inserted by copying
-- to the vz_ and unified tables
--
create or replace function db.units_cris_insert_rows()
returns trigger
language plpgsql
as
$$
BEGIN
    -- insert new unified/truth record
    INSERT INTO db.units (id, crash_id, unit_nbr) values (
        new.id, new.crash_id, new.unit_nbr
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

-------------------------------------------
-------- Tests___ -------------------------
-------------------------------------------

insert into db.crashes_cris (crash_id, test_column) values (1, 'h');
insert into db.units_cris (crash_id, unit_nbr) values (1, 1);
insert into db.people_cris (crash_id, unit_nbr, prsn_nbr, is_primary) values (1, 1, 1, true);
select * from db.crashes_cris;
select * from db.crashes_vz;
select * from db.crashes;
select * from db.units_cris;
select * from db.units_vz;
select * from db.units;
select * from db.people_cris;
select * from db.people_vz;
select * from db.people;
