
WITH trip_bounds AS (
    SELECT
        t.trip_id,
        t.taxi_id,
        t.timestamp AS start_time,
        DATE_ADD(t.timestamp, INTERVAL (MAX(p.seq) * 15) SECOND) AS end_time,
        SUBSTRING_INDEX(
            GROUP_CONCAT(p.latitude ORDER BY p.seq ASC SEPARATOR ','), ',', 1
        ) AS start_lat,
        SUBSTRING_INDEX(
            GROUP_CONCAT(p.longitude ORDER BY p.seq ASC SEPARATOR ','), ',', 1
        ) AS start_lon,
        SUBSTRING_INDEX(
            GROUP_CONCAT(p.latitude ORDER BY p.seq DESC SEPARATOR ','), ',', 1
        ) AS end_lat,
        SUBSTRING_INDEX(
            GROUP_CONCAT(p.longitude ORDER BY p.seq DESC SEPARATOR ','), ',', 1
        ) AS end_lon
    FROM Trip AS t
    JOIN Point AS p ON t.trip_id = p.trip_id
    GROUP BY t.trip_id, t.taxi_id, t.timestamp
)
SELECT * FROM trip_bounds
ORDER BY taxi_id, start_time;
