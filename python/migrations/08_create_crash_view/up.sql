-- build a query to coalesce edited values for editable columns and non-coalesced values for non-editable columns
CREATE OR REPLACE FUNCTION cris.make_crashes_view()
RETURNS void
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

    -- get all cris columns
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

    -- make the view that coalesces the cris and edit columns and joins the computed columns
	EXECUTE 'CREATE OR REPLACE VIEW cris.crashes AS SELECT * FROM (' ||
            format('select %s from cris.crash_cris_data LEFT JOIN cris.crash_edit_data ON cris.crash_cris_data.crash_id = cris.crash_edit_data.crash_id', 
            array_to_string(query_columns, ',')) ||
            ') as crash_edits LEFT JOIN cris.crash_computed_data using ("crash_id");';
    
    RETURN;
END;
$$ LANGUAGE PLPGSQL;

-- call function to create view
SELECT cris.make_crashes_view();

COMMENT ON FUNCTION cris.make_crashes_view IS 'Find non-editable columns, coalesce edited and CRIS values, and make a view to merge them together';

-- View to merge together results of coalesced edits, CRIS data, and computed data

COMMENT ON VIEW CRIS.CRASHES IS 'Merge together coalesced edit/cris columns and computed columns';
