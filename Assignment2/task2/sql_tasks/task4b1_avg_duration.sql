
WITH trip_lengths AS (
    SELECT
        t.trip_id,
        t.call_type,
        MAX(p.seq) AS max_seq
    FROM Trip AS t
    JOIN Point AS p ON t.trip_id = p.trip_id
    GROUP BY t.trip_id, t.call_type
)
SELECT
    call_type,
    ROUND(AVG(max_seq * 15), 2) AS avg_duration_sec
FROM trip_lengths
GROUP BY call_type;
