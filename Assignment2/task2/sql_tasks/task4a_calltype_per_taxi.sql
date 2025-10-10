
WITH call_counts AS (
    SELECT
        taxi_id,
        call_type,
        COUNT(*) AS trips
    FROM Trip
    GROUP BY taxi_id, call_type
),

top_per_taxi AS (
    SELECT
        taxi_id,
        call_type,
        trips,
        RANK() OVER (PARTITION BY taxi_id ORDER BY trips DESC) AS rnk
    FROM call_counts
)

-- Count how many taxis have each call_type as their top one
SELECT
    call_type,
    COUNT(*) AS num_taxis_with_this_top_calltype
FROM top_per_taxi
WHERE rnk = 1
GROUP BY call_type
ORDER BY num_taxis_with_this_top_calltype DESC;
