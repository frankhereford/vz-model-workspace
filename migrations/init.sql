drop schema if exists db cascade;
create schema db;

create table db.crashes_cris (
    crash_id integer primary key
);

create table db.crashes_vz (like db.crashes_cris including all);
create table db.crashes (like db.crashes_cris including all);

alter table db.crashes_vz add constraint crashes_vz_crash_id_fkey foreign key (
    crash_id
) references db.crashes_cris (
    crash_id
) on delete cascade on update restrict;

alter table db.crashes add constraint crashes_crash_id_fkey foreign key (
    crash_id
) references db.crashes_cris (
    crash_id
) on delete cascade on update restrict;


create table db.units_cris (
    id serial primary key,
    crash_id integer not null references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    unit_nbr integer not null,
    constraint unique_units_cris unique (crash_id, unit_nbr)
);

-- create the vz editable units layer
-- this table has no "not null" constraints
-- and the pk is not serial (it will be copying from cris_ table) 
create table db.units_vz (
    id integer primary key references db.units_cris (id),
    crash_id integer references db.crashes_cris (
        crash_id
    ) on delete cascade on update restrict,
    unit_nbr integer,
    constraint unique_units_vz unique (crash_id, unit_nbr)
);

-- create the "truth" units layer
create table db.units (
    id integer primary key references db.units_cris (id),
    crash_id integer not null references db.crashes (
        crash_id
    ) on delete cascade on update restrict,
    unit_nbr integer not null,
    constraint unique_units unique (crash_id, unit_nbr)
);


create table db.persons_cris (
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
    constraint unique_persons_cris unique (unit_id, prsn_nbr)
);

create index on db.persons_cris (unit_id);

create table db.persons_vz (
    id integer references db.persons_cris (id),
    unit_id integer references db.units_cris (
        id
    ),
    crash_id integer references db.crashes_cris (crash_id),
    prsn_nbr integer,
    unit_nbr integer,
    is_primary boolean
);

create index on db.persons_vz (unit_id);

create table db.persons (
    id integer references db.persons_cris (id),
    unit_id integer not null references db.units (
        id
    ),
    crash_id integer references db.crashes (crash_id),
    prsn_nbr integer,
    unit_nbr integer,
    is_primary boolean,
    constraint unique_persons unique (unit_id, prsn_nbr)
);

create index on db.persons (unit_id);

create or replace function db.persons_cris_set_unit_id()
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
before insert on db.persons_cris
for each row
execute procedure db.persons_cris_set_unit_id();
