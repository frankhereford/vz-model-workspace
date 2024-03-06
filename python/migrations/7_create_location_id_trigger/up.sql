-- Add triggers to calculate location from latitude and longitude on crash insert and also crash edit update
CREATE OR REPLACE FUNCTION cris.crash_create_location()
RETURNS TRIGGER AS $$
DECLARE
    updated_location_id varchar;
    updated_position geometry(Geometry,4326);
BEGIN
   updated_position = ST_SetSRID(ST_Point(NEW.longitude, NEW.latitude), 4326);
   updated_location_id = (
                    SELECT location_id 
                    FROM cris.locations 
                    WHERE (geometry && updated_position)
                    AND ST_Contains(geometry, updated_position)
                    LIMIT 1 --TODO: This should be temporary until we get our polygons in order
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

CREATE OR REPLACE TRIGGER CRASH_CREATE_EDIT_LOCATION
AFTER UPDATE ON CRIS.CRASH_EDIT_DATA
FOR EACH ROW EXECUTE FUNCTION cris.crash_create_location();

COMMENT ON TRIGGER CRASH_CREATE_EDIT_LOCATION ON CRIS.CRASH_EDIT_DATA IS 'Create location from latitude and longitude on update';
