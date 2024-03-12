-- Add triggers to calculate location from latitude and longitude on crash insert and also crash edit update
CREATE OR REPLACE FUNCTION cris.crash_create_location()
RETURNS TRIGGER AS $$
DECLARE
    updated_location_id varchar;
    updated_position geometry(Geometry,4326);
    edited_latitude float;
    edited_longitude float;
BEGIN
    -- Determine if the crash latitude and longitude have been edited
    IF TG_OP = 'UPDATE' AND TG_TABLE_NAME = 'crash_cris_data' THEN
        SELECT latitude, longitude INTO edited_latitude, edited_longitude FROM cris.crash_edit_data WHERE crash_id = NEW.crash_id;

        IF edited_latitude IS NOT NULL AND edited_longitude IS NOT NULL THEN
            RETURN NEW;
        END IF;
    END IF;

    IF NEW.latitude IS NULL OR NEW.longitude IS NULL THEN
            RETURN NEW;
    END IF;

    updated_position = ST_SetSRID(ST_Point(NEW.longitude, NEW.latitude), 4326);

    updated_location_id = (
                    SELECT location_id 
                    FROM cris.locations 
                    WHERE (geometry && updated_position)
                    AND ST_Contains(geometry, updated_position)
                    LIMIT 1
                );
   
   UPDATE cris.crash_computed_data
   SET location_id = updated_location_id
   WHERE crash_id = NEW.crash_id;

   RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION cris.crash_create_location IS 'Calculate location from latitude and longitude';

CREATE OR REPLACE TRIGGER CRASH_CREATE_B_CRIS_LOCATION
AFTER INSERT ON CRIS.CRASH_CRIS_DATA
FOR EACH ROW EXECUTE FUNCTION cris.crash_create_location();

COMMENT ON TRIGGER CRASH_CREATE_B_CRIS_LOCATION ON CRIS.CRASH_CRIS_DATA IS 'Create location from latitude and longitude on insert';

CREATE OR REPLACE TRIGGER CRASH_UPDATE_B_CRIS_LOCATION
AFTER UPDATE ON CRIS.CRASH_CRIS_DATA
FOR EACH ROW WHEN (NEW.latitude <> OLD.latitude OR NEW.longitude <> OLD.longitude) EXECUTE FUNCTION cris.crash_create_location();

COMMENT ON TRIGGER CRASH_CREATE_B_CRIS_LOCATION ON CRIS.CRASH_CRIS_DATA IS 'Create location from latitude and longitude on insert';

CREATE OR REPLACE TRIGGER CRASH_CREATE_EDIT_LOCATION
AFTER UPDATE ON CRIS.CRASH_EDIT_DATA
FOR EACH ROW EXECUTE FUNCTION cris.crash_create_location();

COMMENT ON TRIGGER CRASH_CREATE_EDIT_LOCATION ON CRIS.CRASH_EDIT_DATA IS 'Create location from latitude and longitude on update';
