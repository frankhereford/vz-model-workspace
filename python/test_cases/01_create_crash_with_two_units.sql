-- CRIS user creates a crash record with two unit records 

-- Adjust crash_id and unit_id to avoid conflicts with existing data if needed
INSERT INTO cris.crash_cris_data (
    "crash_id",
    "latitude",
    "longitude",
    "primary_address",
    "road_type_id",
    "non_editable_column"
) VALUES
(1001, 30.42975686, -97.92607304, 'IKE3C9E32M0LPHV6M3IH', 4, NULL);

INSERT INTO cris.unit_cris_data ("unit_id", "crash_id", "unit_type_id") VALUES
(2001, 1001, 4);

INSERT INTO cris.unit_cris_data ("unit_id", "crash_id", "unit_type_id") VALUES
(2002, 1001, 2);

-- Check cris.crashes and unique_unit_types should be {2,4} for crash_id 1001
