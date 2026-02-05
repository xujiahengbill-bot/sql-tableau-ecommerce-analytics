use taxi;
CREATE TABLE taxi_analysis_export AS
SELECT 
    -- 1. Temporal Dimensions
    STR_TO_DATE(tpep_pickup_datetime, '%c/%e/%Y %H:%i') AS pickup_timestamp,
    DATE(STR_TO_DATE(tpep_pickup_datetime, '%c/%e/%Y %H:%i')) AS pickup_date,
    HOUR(STR_TO_DATE(tpep_pickup_datetime, '%c/%e/%Y %H:%i')) AS pickup_hour,
    DAYOFWEEK(STR_TO_DATE(tpep_pickup_datetime, '%c/%e/%Y %H:%i')) AS day_of_week,

    -- 2. Categorical Dimensions
    payment_type,
    passenger_count,
    CASE 
        WHEN trip_distance < 2 THEN '< 2 miles'
        WHEN trip_distance < 5 THEN '2–5 miles'
        WHEN trip_distance < 10 THEN '5–10 miles'
        ELSE '10+ miles'
    END AS distance_bucket,

    -- 3. Geospatial Dimensions (Rounded for heatmap density)
    ROUND(pickup_latitude, 2) AS pickup_lat_bin,
    ROUND(pickup_longitude, 2) AS pickup_lon_bin,

    -- 4. Raw Metrics (For Tableau Aggregation)
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    TIMESTAMPDIFF(
        MINUTE, 
        STR_TO_DATE(tpep_pickup_datetime, '%c/%e/%Y %H:%i'), 
        STR_TO_DATE(tpep_dropoff_datetime, '%c/%e/%Y %H:%i')
    ) AS duration_minutes
FROM taxi_cleaned;

SET SQL_SAFE_UPDATES = 0;

UPDATE taxi_analysis_export 
SET distance_bucket = REPLACE(distance_bucket, '–', '-')
WHERE distance_bucket LIKE '%–%';

select * from taxi_analysis_export


