-- Add triggers to get distinct unit types associated with a crash and update crash_computed_data with them
CREATE OR REPLACE FUNCTION cris.get_crash_unit_types()
RETURNS trigger AS $$
DECLARE
    crash_unit_types integer[];
    unit_crash_id integer;
    edit_unit_type_id integer;
    cris_unit_type_id integer;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        -- get the crash_id from the cris unit record
        SELECT crash_id INTO unit_crash_id FROM cris.unit_cris_data WHERE unit_id = NEW.unit_id;

        -- coalesce the unit type id from the cris and edit tables
        SELECT
	        array_agg(DISTINCT COALESCE(cris.unit_edit_data.unit_type_id, cris.unit_cris_data.unit_type_id))
        INTO
            crash_unit_types
        FROM
            cris.unit_cris_data
        LEFT JOIN cris.unit_edit_data ON cris.unit_cris_data.unit_id = cris.unit_edit_data.unit_id
        WHERE
            crash_id = unit_crash_id;

        -- update the crash_computed_data column
        UPDATE cris.crash_computed_data
        SET unique_unit_types = crash_unit_types
        WHERE crash_id = unit_crash_id;

        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        SELECT unique_unit_types INTO crash_unit_types FROM cris.crash_computed_data WHERE crash_id = NEW.crash_id;

        IF NEW.unit_type_id = ANY(crash_unit_types) THEN
            RETURN NEW;
        ELSE 
            crash_unit_types = array_append(crash_unit_types, NEW.unit_type_id);
        END IF;
    
        UPDATE cris.crash_computed_data
        SET unique_unit_types = crash_unit_types
        WHERE crash_id = NEW.crash_id;
        
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION cris.get_crash_unit_types IS 'Find distinct unit types associated with a crash and update crash_computed_data with them';

CREATE OR REPLACE TRIGGER UNIT_UNIQUE_CRASH_UNIT_TYPES_ON_INSERT
AFTER INSERT ON CRIS.UNIT_CRIS_DATA
FOR EACH ROW EXECUTE FUNCTION cris.get_crash_unit_types();

COMMENT ON TRIGGER UNIT_UNIQUE_CRASH_UNIT_TYPES_ON_INSERT ON CRIS.UNIT_CRIS_DATA IS 'Update crash unique unit types on unit insert';

CREATE OR REPLACE TRIGGER UNIT_UNIQUE_CRASH_UNIT_TYPES_ON_UPDATE
AFTER UPDATE ON CRIS.UNIT_EDIT_DATA
FOR EACH ROW EXECUTE FUNCTION cris.get_crash_unit_types();

COMMENT ON TRIGGER UNIT_UNIQUE_CRASH_UNIT_TYPES_ON_UPDATE ON CRIS.UNIT_EDIT_DATA IS 'Update crash unique unit types on unit update';
