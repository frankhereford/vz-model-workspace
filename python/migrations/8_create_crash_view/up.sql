-- merge together non-edit and edit columns and return the edited values if they exist
CREATE OR REPLACE FUNCTION cris.generate_cris_and_edits_query()
RETURNS SETOF cris.crash_cris_data
AS $$
DECLARE
    editable_columns text[];
    non_edit_columns text;
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

    -- get non-editable column names
    non_edit_columns = string_agg('cris.crash_cris_data.' || column_name, ',')
        FROM
            information_schema.columns
        WHERE
            table_schema = 'cris'
            AND table_name = 'crash_cris_data'
            AND NOT (column_name = ANY(editable_columns));

    -- return query to get edited values of columns if they exist or non-edited values if they don't for editable columns
    -- this joins the values of non-editable columns so we have a complete crash row
	RETURN QUERY EXECUTE
        format('select %s, %s from cris.crash_edit_data LEFT JOIN cris.crash_cris_data ON cris.crash_edit_data.crash_id = cris.crash_cris_data.crash_id', 
        string_agg(format('coalesce(cris.crash_edit_data.%s, cris.crash_cris_data.%s) as %s', column_name, column_name, column_name), ','), non_edit_columns) edit_query
        FROM
            information_schema.columns
        WHERE
            table_schema = 'cris'
            AND table_name = 'crash_edit_data';
END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION cris.generate_cris_and_edits_query IS 'Find non-editable columns, coalesce edited and CRIS values, and return a crash query';

CREATE OR REPLACE VIEW cris.crashes AS
SELECT * FROM cris.generate_cris_and_edits_query() as crash_edits
LEFT JOIN cris.crash_computed_data using ("crash_id");

COMMENT ON VIEW CRIS.CRASHES IS 'Merge together coalesced edit/cris columns and computed columns';
