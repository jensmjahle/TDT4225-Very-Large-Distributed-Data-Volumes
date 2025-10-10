-- ------------------------------------------------------------
-- Get overlapping trips by comparing start and end times
-- ------------------------------------------------------------

WITH trip_a AS (
    SELECT trip_id, taxi_id, start_time, end_time FROM trip_times_stage
),
trip_b AS (
    SELECT trip_id, taxi_id, start_time, end_time FROM trip_times_stage
)
SELECT
    a.taxi_id AS taxi_a,
    b.taxi_id AS taxi_b,
    a.trip_id AS trip_a,
    b.trip_id AS trip_b
FROM trip_a AS a
JOIN trip_b AS b
    ON a.taxi_id <> b.taxi_id
   AND a.start_time <= b.end_time
   AND a.end_time >= b.start_time;