-- Count Rows
SELECT COUNT(*) AS total_rows FROM rel_interruptions2;

-- Summary statistics for categorical
SELECT
    COUNT(DISTINCT fahrt_bezeichner) AS distinct_fahrt_bezeichner,
    COUNT(DISTINCT produkt_id) AS distinct_produkt_id,
    COUNT(DISTINCT linien_id) AS distinct_linien_id,
    COUNT(DISTINCT linien_text) AS distinct_linien_text,
    COUNT(DISTINCT verkehrsmittel_text) AS distinct_verkehrsmittel_text,
    COUNT(DISTINCT haltestellen_name) AS distinct_haltestellen_name,
	COUNT(DISTINCT an_prognose) AS distinct_an_prognose,
	COUNT(DISTINCT ab_prognose) AS distinct_ab_prognose
FROM rel_interruptions2;


-- Count of True and False for ankunftsverspatung and abfahrtsverspatung
SELECT
    SUM(CASE WHEN ankunftsverspatung = 'true' THEN 1 ELSE 0 END) AS count_true_ankunftsverspatung,
    SUM(CASE WHEN ankunftsverspatung = 'false' THEN 1 ELSE 0 END) AS count_false_ankunftsverspatung,
    SUM(CASE WHEN abfahrtsverspatung = 'true' THEN 1 ELSE 0 END) AS count_true_abfahrtsverspatung,
    SUM(CASE WHEN abfahrtsverspatung = 'false' THEN 1 ELSE 0 END) AS count_false_abfahrtsverspatung,
	SUM(CASE WHEN zusatzfahrt_tf = 'false' THEN 1 ELSE 0 END) AS count_false_zusatzfahrt,
	SUM(CASE WHEN zusatzfahrt_tf = 'true' THEN 1 ELSE 0 END) AS count_true_zusatzfahrt,
	SUM(CASE WHEN durchfahrt_tf = 'false' THEN 1 ELSE 0 END) AS count_false_durchfahrtfahrt,
	SUM(CASE WHEN durchfahrt_tf = 'true' THEN 1 ELSE 0 END) AS count_true_durchfahrtfahrt
FROM rel_interruptions2;

--  Drop columns not necessary

ALTER TABLE rel_interruptions2
DROP COLUMN betreiber_abk,
DROP COLUMN betreiber_name,
DROP COLUMN faellt_aus_tf,
DROP COLUMN an_prognose_status,
DROP COLUMN an_prognose,
DROP COLUMN ab_prognose,
DROP COLUMN ab_prognose_status,
DROP COLUMN ankunftsverspatung,
DROP COLUMN abfahrtsverspatung,
DROP COLUMN durchfahrt_tf;

-- Remove the 'T' delimiter from ankunftszeit
UPDATE rel_interruptions2
SET ankunftszeit = REPLACE(ankunftszeit, 'T', ' ');

-- Remove the 'T' delimiter from abfahrtszeit
UPDATE rel_interruptions2
SET abfahrtszeit = REPLACE(abfahrtszeit, 'T', ' ');


-- Set the session time zone to UTC (GMT+0)
SET TIME ZONE 'UTC';

-- Update NaN values to NULL in ankunftszeit
UPDATE rel_interruptions2
SET ankunftszeit = NULL
WHERE ankunftszeit = 'NaN';

-- Update NaN values to NULL in abfahrtszeit
UPDATE rel_interruptions2
SET abfahrtszeit = NULL
WHERE abfahrtszeit = 'NaN';


-- Change date and timestamp columns using conditional conversion
ALTER TABLE rel_interruptions2
ALTER COLUMN betriebstag TYPE DATE USING betriebstag::DATE,
ALTER COLUMN ankunftszeit TYPE TIMESTAMPTZ USING 
    CASE 
        WHEN ankunftszeit IS NULL THEN NULL 
        ELSE ankunftszeit::TIMESTAMPTZ 
    END,
ALTER COLUMN abfahrtszeit TYPE TIMESTAMPTZ USING 
    CASE 
        WHEN abfahrtszeit IS NULL THEN NULL 
        ELSE abfahrtszeit::TIMESTAMPTZ 
    END;

-- Check if betriebstag and fahrt_bezeichner together work as unique identifier
SELECT 
  betriebstag,
  fahrt_bezeichner,
  COUNT(*) AS count
FROM 
  rel_interruptions2
GROUP BY 
  betriebstag, 
  fahrt_bezeichner
HAVING 
  COUNT(*) > 1;
  
  
-- Check if time and geo point are a unique identifier 
  
SELECT 
  abfahrtszeit,
  haltestellen_name,
  COUNT(*) AS count
FROM 
  rel_interruptions2
GROUP BY 
  abfahrtszeit, 
  haltestellen_name
HAVING 
  COUNT(*) > 1;


-- no unique identifier found, therefore new column added
ALTER TABLE rel_interruptions2 ADD COLUMN id SERIAL PRIMARY KEY;

-- Change the data type of lat and lon to double precision
ALTER TABLE rel_interruptions2
ALTER COLUMN lon TYPE double precision USING lon::double precision,
ALTER COLUMN lat TYPE double precision USING lat::double precision;
