
SELECT
    COUNT(*) AS invalid_trips
FROM (
    SELECT trip_id
    FROM Point
    GROUP BY trip_id
    HAVING COUNT(*) < 3
) AS invalid;
