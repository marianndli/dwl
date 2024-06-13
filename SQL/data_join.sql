-- Set the session time zone to UTC (GMT+0) for interruptions
SET TIME ZONE 'UTC';

-- Drop the existing table if it exists
DROP TABLE IF EXISTS joined_velospot_interruptions;

-- Create the new table and populate it with the joined data
CREATE TABLE joined_velospot_interruptions AS
WITH interruptions AS (
  SELECT
    i.id,
    i.abfahrtszeit AS abfahrtszeit_timestamp,
    i.lon AS interruption_lon,
    i.lat AS interruption_lat,
    i.betriebstag,
    i.fahrt_bezeichner,
    i.betreiber_id,
    i.produkt_id,
    i.linien_id,
    i.linien_text,
    i.verkehrsmittel_text,
    i.zusatzfahrt_tf,
    i.bpuic,
    i.haltestellen_name,
    i.ankunftszeit,
    t.train_station_name
  FROM
    rel_interruptions2 i
  JOIN
    trainstations t
  ON
    i.haltestellen_name = t.train_station_name
),
velos AS (
  SELECT
    v.attributes_station_name,
    v.timestamp AT TIME ZONE 'Etc/GMT-2' AS timestamp,
    v.attributes_station_status_num_vehicle_available,
    t.train_station_name
  FROM
    velospot_all_records v
  JOIN
    trainstations t
  ON
    v.attributes_station_name = t.velospot_station_name
),
velos_before AS (
  SELECT
    i.id,
    i.train_station_name,
    SUM(v.attributes_station_status_num_vehicle_available) AS total_vehicles_before,
    COUNT(DISTINCT v.attributes_station_name) AS station_count_before
  FROM
    interruptions i
  JOIN
    velos v
  ON
    v.train_station_name = i.train_station_name
  WHERE
    v.timestamp BETWEEN i.abfahrtszeit_timestamp - INTERVAL '10 minutes' AND i.abfahrtszeit_timestamp
  GROUP BY i.id, i.train_station_name
),
velos_after AS (
  SELECT
    i.id,
    i.train_station_name,
    SUM(v.attributes_station_status_num_vehicle_available) AS total_vehicles_after,
    COUNT(DISTINCT v.attributes_station_name) AS station_count_after
  FROM
    interruptions i
  JOIN
    velos v
  ON
    v.train_station_name = i.train_station_name
  WHERE
    v.timestamp BETWEEN i.abfahrtszeit_timestamp AND i.abfahrtszeit_timestamp + INTERVAL '10 minutes'
  GROUP BY i.id, i.train_station_name
)
SELECT
  i.id AS interruption_id,
  i.abfahrtszeit_timestamp,
  i.interruption_lon,
  i.interruption_lat,
  i.betriebstag,
  i.fahrt_bezeichner,
  i.betreiber_id,
  i.produkt_id,
  i.linien_id,
  i.linien_text,
  i.verkehrsmittel_text,
  i.zusatzfahrt_tf,
  i.bpuic,
  i.haltestellen_name,
  i.ankunftszeit,
  vb.total_vehicles_before AS "velos 10min before",
  vf.total_vehicles_after AS "velos 10min after",
  (vf.total_vehicles_after - vb.total_vehicles_before) AS velos_difference
FROM
  interruptions i
INNER JOIN velos_before vb ON i.id = vb.id AND i.train_station_name = vb.train_station_name
INNER JOIN velos_after vf ON i.id = vf.id AND i.train_station_name = vf.train_station_name
WHERE
  vb.station_count_before = vf.station_count_after
ORDER BY i.abfahrtszeit_timestamp;




SELECT 
    COUNT(velos_difference) AS vehicles_count_diff,
    AVG(velos_difference) AS avg_vehicles_diff,
    MIN(velos_difference) AS min_vehicles_diff,
    MAX(velos_difference) AS max_vehicles_diff
FROM joined_velospot_interruptions;

