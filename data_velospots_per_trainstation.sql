CREATE TABLE trainstations (
    id SERIAL PRIMARY KEY,  -- Example of an auto-increment primary key
    station_name VARCHAR(255) NOT NULL,  -- Example of a text column
    count_velospotname33 VARCHAR(255) NOT NULL  -- Example of another text column
);

-- Add the new column count_velospot33
ALTER TABLE trainstations
ADD COLUMN count_velospot33 NUMERIC;


-- Step 1: Drop the unwanted column
ALTER TABLE trainstations
DROP COLUMN count_velospot33;

-- Step 1: Drop the unwanted column
ALTER TABLE trainstations
DROP COLUMN id;

-- Step 2: Create a temporary table with distinct rows
CREATE TABLE temp_trainstations AS
SELECT DISTINCT ON (station_name, count_velospotname33)
    station_name,
    count_velospotname33
FROM trainstations;

-- Step 3: Drop the original table
DROP TABLE trainstations;


-- Step 4: Rename the temporary table to the original table name
ALTER TABLE temp_trainstations RENAME TO trainstations;

-- Step 5: Rename columns as needed
ALTER TABLE trainstations
RENAME COLUMN station_name TO velospot_station_name;

ALTER TABLE trainstations
RENAME COLUMN count_velospotname33 TO train_station_name;


-- Step 1: Create a new table with aggregated data
CREATE TABLE velospots_per_trainstation AS
SELECT 
    train_station_name,
    COUNT(velospot_station_name) AS velospot_count
FROM 
    trainstations
GROUP BY 
    train_station_name;


