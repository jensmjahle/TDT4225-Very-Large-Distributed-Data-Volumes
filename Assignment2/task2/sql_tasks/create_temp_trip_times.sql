-- ------------------------------------------------------------
-- TEMP TABLE: trip_id, taxi_id, start_time, end_time (cached)
-- ------------------------------------------------------------

CREATE TEMPORARY TABLE temp_trip_times AS
SELECT
    t.trip_id,
    t.taxi_id,
    t.timestamp AS start_time,
    DATE_ADD(t.timestamp, INTERVAL (MAX(p.seq) * 15) SECOND) AS end_time
FROM Trip AS t
JOIN Point AS p ON t.trip_id = p.trip_id
GROUP BY t.trip_id, t.taxi_id, t.timestamp;

-- Speedup indexes
CREATE INDEX idx_temp_trip_times_taxi_id ON temp_trip_times(taxi_id);
CREATE INDEX idx_temp_trip_times_time ON temp_trip_times(start_time, end_time);
