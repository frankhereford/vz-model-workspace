-- View that counts the number of units at each location
CREATE OR REPLACE VIEW cris.locations_with_unit_counts AS
SELECT
    cris.locations.location_id,
    COUNT(cris.units.unit_id) AS units_count
FROM cris.locations
LEFT JOIN cris.crashes
    ON cris.locations.location_id = cris.crashes.location_id
LEFT JOIN cris.units
    ON cris.crashes.crash_id = cris.units.crash_id
GROUP BY cris.locations.location_id;
