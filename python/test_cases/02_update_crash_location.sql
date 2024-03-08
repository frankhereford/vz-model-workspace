-- VZ user changes a crash’s Location ID by updating the crash lat/lon
-- Check cris.crashes and location should update from 31EF24BA49 to C6B46A5293
UPDATE cris.crash_edit_data SET latitude = 30.27893478, longitude = -97.73610246
WHERE crash_id = 1;
