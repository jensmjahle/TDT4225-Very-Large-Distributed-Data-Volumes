SELECT
    taxi_id,
    COUNT(*) AS total_trips
FROM Trip
GROUP BY taxi_id
ORDER BY total_trips DESC
LIMIT 20;
