-- create the query statement from the columns exposed by the information_schema table
CREATE OR REPLACE FUNCTION cris.generate_crash_edits_query()
RETURNS SETOF cris.crash_edit_data
AS $$
BEGIN
	RETURN QUERY EXECUTE 
        format('select %s from cris.crash_edit_data LEFT JOIN cris.crash_cris_data ON cris.crash_edit_data.crash_id = cris.crash_cris_data.crash_id', 
        string_agg(format('coalesce(cris.crash_edit_data.%s, cris.crash_cris_data.%s) as %s', column_name, column_name, column_name), ',')) edit_query
        FROM
            information_schema.columns
        WHERE
            table_schema = 'cris'
            AND table_name = 'crash_edit_data';
END;
$$
LANGUAGE plpgsql;

-- SELECT * FROM cris.generate_crash_edits_query();

-- the query that is generated
-- SELECT
-- 	coalesce(cris.crash_edit_data.crash_id, cris.crash_cris_data.crash_id) AS crash_id,
-- 	coalesce(cris.crash_edit_data.road_type_id, cris.crash_cris_data.road_type_id) AS road_type_id,
-- 	coalesce(cris.crash_edit_data.latitude, cris.crash_cris_data.latitude) AS latitude,
-- 	coalesce(cris.crash_edit_data.longitude, cris.crash_cris_data.longitude) AS longitude,
-- 	coalesce(cris.crash_edit_data.primary_address, cris.crash_cris_data.primary_address) AS primary_address
-- FROM
-- 	cris.crash_edit_data
-- 	LEFT JOIN cris.crash_cris_data ON cris.crash_edit_data.crash_id = cris.crash_cris_data.crash_id
