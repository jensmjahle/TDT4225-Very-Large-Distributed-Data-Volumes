SELECT
    taxi_id,
    call_type,
    COUNT(*) AS trips,
    RANK() OVER (PARTITION BY taxi_id ORDER BY COUNT(*) DESC) AS rank_per_taxi
FROM Trip
GROUP BY taxi_id, call_type
HAVING rank_per_taxi = 1;
