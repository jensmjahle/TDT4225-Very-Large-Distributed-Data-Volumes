SELECT COUNT(*) AS invalid_trips
FROM (
    SELECT trip_id, COUNT(*) AS num_points
    FROM Point
    GROUP BY trip_id
    HAVING num_points < 3
) AS invalid;
