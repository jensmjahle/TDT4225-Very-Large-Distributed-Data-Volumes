
WITH trip_points AS (
    SELECT
        p.trip_id,
        p.seq,
        p.latitude,
        p.longitude
    FROM Point p
),
trip_start_end AS (
    SELECT
        t.trip_id,
        t.taxi_id,
        MIN(tp.latitude) AS start_lat,
        MIN(tp.longitude) AS start_lon,
        (SELECT latitude FROM Point p2 WHERE p2.trip_id = t.trip_id ORDER BY seq ASC LIMIT 1) AS start_latitude,
        (SELECT longitude FROM Point p2 WHERE p2.trip_id = t.trip_id ORDER BY seq ASC LIMIT 1) AS start_longitude,
        (SELECT latitude FROM Point p3 WHERE p3.trip_id = t.trip_id ORDER BY seq DESC LIMIT 1) AS end_latitude,
        (SELECT longitude FROM Point p3 WHERE p3.trip_id = t.trip_id ORDER BY seq DESC LIMIT 1) AS end_longitude
    FROM Trip t
    JOIN trip_points tp ON t.trip_id = tp.trip_id
    GROUP BY t.trip_id, t.taxi_id
)
SELECT
    trip_id,
    taxi_id,
    start_latitude,
    start_longitude,
    end_latitude,
    end_longitude,
    ROUND(
        6371 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(end_latitude - start_latitude) / 2), 2) +
                COS(RADIANS(start_latitude)) *
                COS(RADIANS(end_latitude)) *
                POWER(SIN(RADIANS(end_longitude - start_longitude) / 2), 2)
            )
        ),
        3
    ) AS distance_km
FROM trip_start_end
HAVING distance_km <= 0.05  -- within 50 m
ORDER BY distance_km ASC

