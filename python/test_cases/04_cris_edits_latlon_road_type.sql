-- CRIS user updates the crash lat/lon, and road type
-- Check cris.crashes and the location should update from 31EF24BA49 to 0BA851E71E (unless a VZ edit has changed it)
-- The road type should update from 4 to 1 (unless a VZ edit has changed it)
UPDATE cris.crash_cris_data SET
    latitude = 30.39960212, longitude = -97.74077394, road_type_id = 1
WHERE crash_id = 1;
