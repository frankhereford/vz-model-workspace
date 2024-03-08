-- Create a query/view that powers a simplified version of the locations table, 
-- for example by calculating total number of units per location 
-- (for reference see: locations_with_crash_injury_counts in the DB)

-- Query the view added in the migrations
SELECT * FROM cris.locations_with_unit_counts WHERE location_id = '31EF24BA49';
