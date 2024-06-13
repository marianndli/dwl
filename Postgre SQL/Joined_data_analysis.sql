-- Summary statistics for bike availability before and after interruptions
SELECT
    COUNT(*) AS total_interruptions,
    AVG("velos 10min before") AS avg_bikes_before,
    MIN("velos 10min before") AS min_bikes_before,
    MAX("velos 10min before") AS max_bikes_before,
    AVG("velos 10min after") AS avg_bikes_after,
    MIN("velos 10min after") AS min_bikes_after,
    MAX("velos 10min after") AS max_bikes_after,
    AVG(velos_difference) AS avg_bikes_difference,
    MIN(velos_difference) AS min_bikes_difference,
    MAX(velos_difference) AS max_bikes_difference
FROM
    joined_velospot_interruptions;
