-- ------------------------------------------------------------
-- Candidate overlapping trips (same time window)
-- ------------------------------------------------------------

SELECT
    a.trip_id AS trip_a,
    a.taxi_id AS taxi_a,
    b.trip_id AS trip_b,
    b.taxi_id AS taxi_b,
    a.start_time AS start_a,
    a.end_time AS end_a,
    b.start_time AS start_b,
    b.end_time AS end_b
FROM temp_trip_times a
JOIN temp_trip_times b
  ON a.taxi_id < b.taxi_id
 AND a.start_time <= b.end_time
 AND b.start_time <= a.end_time
LIMIT 5000;
