
WITH call_counts AS (
    SELECT
        taxi_id,
        call_type,
        COUNT(*) AS trip_count
    FROM Trip
    GROUP BY taxi_id, call_type
),
ranked AS (
    SELECT
        taxi_id,
        call_type,
        trip_count,
        RANK() OVER (PARTITION BY taxi_id ORDER BY trip_count DESC) AS rnk
    FROM call_counts
)
SELECT
    taxi_id,
    call_type AS most_used_call_type,
    trip_count
FROM ranked
WHERE rnk = 1
ORDER BY trip_count DESC;
