-- Count Rows
SELECT COUNT(*) AS total_rows FROM velospot_all_records;

-- Summary statistics for categorical
SELECT
    COUNT(DISTINCT attributes_id) AS distinct_attributes_id,
    COUNT(DISTINCT attributes_station_name) AS distinct_attributes_station_name,
    COUNT(DISTINCT attributes_station_status_installed) AS distinct_attributes_station_status_installed,
    COUNT(DISTINCT attributes_station_status_renting) AS distinct_attributes_station_status_renting,
    COUNT(DISTINCT attributes_station_status_returning) AS distinct_attributes_station_status_returning,
    COUNT(DISTINCT featureid) AS distinct_featureid,
	COUNT(DISTINCT id) AS distinct_id
FROM velospot_all_records;

-- Summary for numerical columns

SELECT 
    COUNT(attributes_station_status_num_vehicle_available) AS vehicles_count,
    AVG(attributes_station_status_num_vehicle_available) AS avg_vehicles,
    MIN(attributes_station_status_num_vehicle_available) AS min_vehicles,
    MAX(attributes_station_status_num_vehicle_available) AS max_vehicles
FROM velospot_all_records;

--  Drop columns not necessary

ALTER TABLE velospot_all_records
DROP COLUMN attributes_station_status_installed,
DROP COLUMN attributes_station_status_renting,
DROP COLUMN attributes_station_status_returning;


-- Set the session time zone to GMT-2
SET TIME ZONE 'Etc/GMT+2';

-- Change timestamp column to TIMESTAMPTZ
ALTER TABLE velospot_all_records
ALTER COLUMN timestamp TYPE TIMESTAMPTZ USING (timestamp AT TIME ZONE 'Etc/GMT+2');

-- Change the data type of geometry_x and geometry_y to double precision
ALTER TABLE velospot_all_records
ALTER COLUMN geometry_x TYPE double precision USING geometry_x::double precision,
ALTER COLUMN geometry_y TYPE double precision USING geometry_y::double precision;




