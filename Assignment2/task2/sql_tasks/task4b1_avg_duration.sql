
WITH trip_durations AS (
    SELECT
        trip_id,
        MAX(seq) * 15 AS duration_sec
    FROM Point
    GROUP BY trip_id
)
SELECT
    t.call_type,
    ROUND(AVG(td.duration_sec), 2) AS avg_duration_sec
FROM Trip AS t
JOIN trip_durations AS td
    ON t.trip_id = td.trip_id
GROUP BY t.call_type
ORDER BY t.call_type;
