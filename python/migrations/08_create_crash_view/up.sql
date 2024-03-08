-- build a query to coalesce edited values for editable columns and non-coalesced values for non-editable columns
CREATE OR REPLACE FUNCTION cris.generate_crash_cris_and_edits_query()
RETURNS SETOF cris.crash_cris_data
AS $$
DECLARE
    editable_columns text[];
    cris_columns text[];
    query_columns text[];
    col text;
BEGIN
    -- get editable column names
    SELECT
	array_agg(column_name)
    INTO editable_columns
    FROM
        information_schema.columns
    WHERE
        table_schema = 'cris'
        AND table_name = 'crash_edit_data';

    -- get cris columns
    SELECT array_agg(column_name)
    INTO cris_columns
    FROM
        information_schema.columns
    WHERE
        table_schema = 'cris'
        AND table_name = 'crash_cris_data';

    -- iterate cris columns and build up the query columns depending on if the column is editable and needs coalescing or not
    FOREACH col IN ARRAY cris_columns 
    LOOP
        IF col = ANY(editable_columns) THEN
            query_columns := query_columns || format('coalesce(cris.crash_edit_data.%s, cris.crash_cris_data.%s) as %s', col, col, col);
        ELSE
            query_columns := query_columns || col;
        END IF;
    END LOOP;

    -- return query to get edited values of columns if they exist or non-edited values if they don't for editable columns
    -- this joins the values of non-editable columns so we have a complete crash row
	RETURN QUERY EXECUTE
        format('select %s from cris.crash_cris_data LEFT JOIN cris.crash_edit_data ON cris.crash_cris_data.crash_id = cris.crash_edit_data.crash_id', 
        array_to_string(query_columns, ','));
END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION cris.generate_crash_cris_and_edits_query IS 'Find non-editable columns, coalesce edited and CRIS values, and return a crash query';

-- View to merge together results of coallesced edits, CRIS data, and computed data
CREATE OR REPLACE VIEW cris.crashes AS
SELECT * FROM cris.generate_crash_cris_and_edits_query()
LEFT JOIN cris.crash_computed_data using ("crash_id");

COMMENT ON VIEW CRIS.CRASHES IS 'Merge together coalesced edit/cris columns and computed columns';
