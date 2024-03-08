-- merge together non-edit and edit columns and return the edited values if they exist
CREATE OR REPLACE FUNCTION cris.generate_unit_cris_and_edits_query()
RETURNS SETOF cris.unit_cris_data
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
        AND table_name = 'unit_edit_data';

    -- get non-editable column names
    non_edit_columns = string_agg(column_name, ',')
        FROM
            information_schema.columns
        WHERE
            table_schema = 'cris'
            AND table_name = 'unit_cris_data'
            AND NOT (column_name = ANY(editable_columns));

    -- return query to get edited values of columns if they exist or non-edited values if they don't for editable columns
    -- this joins the values of non-editable columns so we have a complete unit row
	RETURN QUERY EXECUTE
        format('select %s, %s from cris.unit_cris_data LEFT JOIN cris.unit_edit_data ON cris.unit_cris_data.unit_id = cris.unit_edit_data.unit_id', 
        string_agg(format('coalesce(cris.unit_edit_data.%s, cris.unit_cris_data.%s) as %s', column_name, column_name, column_name), ','), non_edit_columns) edit_query
        FROM
            information_schema.columns
        WHERE
            table_schema = 'cris'
            AND table_name = 'unit_edit_data';
END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION cris.generate_unit_cris_and_edits_query IS 'Find non-editable columns, coalesce edited and CRIS values, and return a unit query';

CREATE OR REPLACE VIEW cris.units AS
SELECT * FROM cris.generate_unit_cris_and_edits_query() as unit_edits
LEFT JOIN cris.unit_computed_data using ("unit_id");

COMMENT ON VIEW cris.units IS 'Merge together coalesced edit/cris columns and computed columns';
