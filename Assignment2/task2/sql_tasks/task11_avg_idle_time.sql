
WITH trip_end_times AS (
    SELECT
        t.trip_id,
        t.taxi_id,
        t.timestamp AS start_time,
        DATE_ADD(t.timestamp, INTERVAL (MAX(p.seq) * 15) SECOND) AS end_time
    FROM Trip t
    JOIN Point p ON t.trip_id = p.trip_id
    GROUP BY t.trip_id, t.taxi_id, t.timestamp
),
with_next AS (
    SELECT
        taxi_id,
        trip_id,
        start_time,
        end_time,
        LEAD(start_time) OVER (PARTITION BY taxi_id ORDER BY start_time) AS next_start
    FROM trip_end_times
)
SELECT
    taxi_id,
    ROUND(AVG(TIMESTAMPDIFF(MINUTE, end_time, next_start)), 2) AS avg_idle_minutes,
    ROUND(SUM(TIMESTAMPDIFF(MINUTE, end_time, next_start)), 2) AS total_idle_minutes,
    COUNT(*) AS idle_intervals
FROM with_next
WHERE next_start IS NOT NULL
  AND next_start > end_time
  AND TIMESTAMPDIFF(HOUR, end_time, next_start) < 6  -- ignore long breaks (>6 h)
GROUP BY taxi_id
HAVING idle_intervals > 0
ORDER BY avg_idle_minutes DESC
LIMIT 20;
