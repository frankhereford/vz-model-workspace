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


-- the view
-- SELECT * FROM cris.generate_crash_edits_query() AS edited
-- LEFT JOIN cris.crash_computed_data 
-- ON edited.crash_id = cris.crash_computed_data.crash_id;




-- Get 
-- get editable columns from information_schema
CREATE OR REPLACE FUNCTION cris.generate_crash_cris_query()
RETURNS SETOF cris.crash_cris_data
AS $$
DECLARE
    editable_columns text[];
BEGIN
    SELECT
-- 	string_agg(format('%L',column_name), ',')
	array_agg(column_name)
    INTO editable_columns
    FROM
        information_schema.columns
    WHERE
        table_schema = 'cris'
        AND table_name = 'crash_edit_data'
        AND column_name NOT IN ('crash_id');

	RAISE NOTICE 'Value: %', editable_columns;
	RETURN QUERY EXECUTE
        format('select %s from cris.crash_cris_data', 
        string_agg(column_name, ',')) crash_query
        FROM
            information_schema.columns
        WHERE
            table_schema = 'cris'
            AND table_name = 'crash_cris_data'
            AND NOT (column_name = ANY(editable_columns));
END;
$$
LANGUAGE plpgsql;






-- merge together non-edit and edit columns and return the edited values if they exist
CREATE OR REPLACE FUNCTION cris.generate_crash_edits_query()
RETURNS SETOF cris.crash_cris_data
LANGUAGE plpgsql
AS $function$
DECLARE
    editable_columns text[];
    non_edit_columns text;
BEGIN
    SELECT
	array_agg(column_name)
    INTO editable_columns
    FROM
        information_schema.columns
    WHERE
        table_schema = 'cris'
        AND table_name = 'crash_edit_data';
--         AND column_name NOT IN ('crash_id');

    non_edit_columns = string_agg('cris.crash_cris_data.' || column_name, ',')
    -- non_edit_query = format('(select %s from cris.crash_cris_data) as non_edits', 
        -- string_agg(column_name, ',')) crash_query
        FROM
            information_schema.columns
        WHERE
            table_schema = 'cris'
            AND table_name = 'crash_cris_data'
            AND NOT (column_name = ANY(editable_columns));

	RETURN QUERY EXECUTE
        format('select %s, %s from cris.crash_edit_data LEFT JOIN cris.crash_cris_data ON cris.crash_edit_data.crash_id = cris.crash_cris_data.crash_id', 
        string_agg(format('coalesce(cris.crash_edit_data.%s, cris.crash_cris_data.%s) as %s', column_name, column_name, column_name), ','), non_edit_columns) edit_query
        FROM
            information_schema.columns
        WHERE
            table_schema = 'cris'
            AND table_name = 'crash_edit_data';
END;
$function$
