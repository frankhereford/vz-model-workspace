--
-- handle crash record insert by copying
-- to the vz_ and unified tables
--
create or replace function db.crashes_cris_insert_rows()
returns trigger
language plpgsql
as
$$
BEGIN
    -- insert new unified record
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
-- handle a unit record insert by copying
-- to the vz_ and unified tables
--
create or replace function db.units_cris_insert_rows()
returns trigger
language plpgsql
as
$$
BEGIN
    -- insert new unified record
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
-- handle a people record insert by setting
-- the  unit_id column based on the crash_id and unit_nbr
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
-- handle a people record insert by copying
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

--
-- handle a cris crash update by updating the
-- unified crash record from cris + vz values
--
create or replace function db.crashes_cris_update()
returns trigger
language plpgsql
as $$
declare
    new_cris_jb jsonb := to_jsonb (new);
    old_cris_jb jsonb := to_jsonb (old);
    vz_record_jb jsonb;
    column_name text;
    updates_todo text [] := '{}';
    update_stmt text := 'update db.crashes set ';
begin
    -- get corresponding the VZ record as jsonb
    SELECT to_jsonb(crashes_vz) INTO vz_record_jb from db.crashes_vz where db.crashes_vz.crash_id = new.crash_id;

    -- for every key in the cris json object
    for column_name in select jsonb_object_keys(new_cris_jb) loop
        -- if the new value doesn't match the old
        if(new_cris_jb -> column_name <> old_cris_jb -> column_name) then
            -- see if the vz record has a value for this field
            if (vz_record_jb ->> column_name is  null) then
                -- this value is not overridden by VZ
                -- so update the unified record with this new value
                updates_todo := updates_todo || format('%I = $1.%I', column_name, column_name);
            end if;
        end if;
    end loop;
    if(array_length(updates_todo, 1) > 0) then
        -- complete the update statement by joining all `set` clauses together
        update_stmt := update_stmt
            || array_to_string(updates_todo, ',')
            || format(' where db.crashes.crash_id = %s', new.crash_id);
        raise notice 'Executing unified record update: %', update_stmt;
        execute (update_stmt) using new;
    else
        raise notice 'No changes to unified record needed';
    end if;
    return null;
end;
$$;

create trigger update_crash_from_cris_crash_update
after update on db.crashes_cris for each row
execute procedure db.crashes_cris_update();

--
-- handle a vz crash update by updating the
-- unified crash record from cris + vz values
--
create or replace function db.crashes_vz_update()
returns trigger
language plpgsql
as $$
declare
    new_vz_jb jsonb := to_jsonb (new);
    old_vz_jb jsonb := to_jsonb (old);
    cris_record_jb jsonb;
    column_name text;
    updates_todo text [] := '{}';
    update_stmt text := 'update db.crashes set ';
begin
    -- get corresponding the cris record as jsonb
    SELECT to_jsonb(crashes_cris) INTO cris_record_jb from db.crashes_cris where db.crashes_cris.crash_id = new.crash_id;
    -- for every key in the vz json object
    for column_name in select jsonb_object_keys(new_vz_jb) loop
        --  create a set statement for the column
        if cris_record_jb ? column_name then
            -- if this column exists on the cris table, coalesce vz + cris values
            updates_todo := updates_todo
            || format('%I = coalesce($1.%I, cris_record.%I)', column_name, column_name, column_name);
        else
            updates_todo := updates_todo
            || format('%I = $1.%I', column_name, column_name);
        end if;
    end loop;
    -- join all `set` clauses together
    update_stmt := update_stmt
        || array_to_string(updates_todo, ',')
        || format(' from (select * from db.crashes_cris where db.crashes_cris.crash_id = %s) as cris_record', new.crash_id)
        || format(' where db.crashes.crash_id = %s ', new.crash_id);
    raise notice 'Executing unified record update: %', update_stmt;
    execute (update_stmt) using new;
    return null;
end;
$$;

create trigger update_crash_from_vz_crash_update
after update on db.crashes_vz for each row
execute procedure db.crashes_vz_update();
