
WITH trip_durations AS (
    SELECT
        t.trip_id,
        t.taxi_id,
        t.timestamp AS start_time,
        DATE(t.timestamp) AS start_date,
        DATE_ADD(t.timestamp, INTERVAL (MAX(p.seq) - MIN(p.seq)) * 15 SECOND) AS end_time,
        DATE(DATE_ADD(t.timestamp, INTERVAL (MAX(p.seq) - MIN(p.seq)) * 15 SECOND)) AS end_date
    FROM Trip t
    JOIN Point p ON t.trip_id = p.trip_id
    GROUP BY t.trip_id, t.taxi_id, t.timestamp
)
SELECT
    trip_id,
    taxi_id,
    start_time,
    end_time,
    TIMESTAMPDIFF(SECOND, start_time, end_time) / 60 AS duration_min
FROM trip_durations
WHERE start_date <> end_date
ORDER BY start_time
LIMIT 100;
