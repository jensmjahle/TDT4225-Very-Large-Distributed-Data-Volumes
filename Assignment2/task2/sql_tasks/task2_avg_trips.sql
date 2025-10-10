SELECT
    COUNT(*) / COUNT(DISTINCT taxi_id) AS avg_trips_per_taxi
FROM Trip;
