-- ------------------------------------------------------------
-- Get overlapping trips by time
-- ------------------------------------------------------------

SELECT
    a.taxi_id AS taxi_a,
    b.taxi_id AS taxi_b,
    a.trip_id AS trip_a,
    b.trip_id AS trip_b
FROM
    (SELECT * FROM trip_times) AS a
JOIN
    (SELECT * FROM trip_times) AS b
ON
    a.taxi_id <> b.taxi_id
    AND a.start_time <= b.end_time
    AND a.end_time >= b.start_time
LIMIT 5000;
