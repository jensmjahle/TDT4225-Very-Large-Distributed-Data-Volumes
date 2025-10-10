-- ------------------------------------------------------------
-- Task 8: Find taxi pairs within 5 meters and 5 seconds
-- ------------------------------------------------------------

WITH point_times AS (
    SELECT
        p.trip_id,
        t.taxi_id,
        (UNIX_TIMESTAMP(t.timestamp) + (p.seq * 15)) AS point_time,
        p.latitude,
        p.longitude
    FROM Point p
    JOIN Trip t ON p.trip_id = t.trip_id
)

SELECT DISTINCT
    LEAST(a.taxi_id, b.taxi_id) AS taxi_id_1,
    GREATEST(a.taxi_id, b.taxi_id) AS taxi_id_2
FROM point_times a
JOIN point_times b
    ON a.taxi_id <> b.taxi_id
   AND ABS(a.point_time - b.point_time) <= 5  -- within 5 seconds
   AND (
        6371000 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(a.latitude - b.latitude) / 2), 2) +
                COS(RADIANS(a.latitude)) * COS(RADIANS(b.latitude)) *
                POWER(SIN(RADIANS(a.longitude - b.longitude) / 2), 2)
            )
        )
    ) <= 5  -- within 5 meters
LIMIT 100;  -- limit for sanity (remove later)
