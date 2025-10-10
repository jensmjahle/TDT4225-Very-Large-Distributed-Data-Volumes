SELECT
    (SELECT COUNT(DISTINCT taxi_id) FROM Trip) AS total_taxis,
    (SELECT COUNT(*) FROM Trip) AS total_trips,
    (SELECT COUNT(*) FROM Point) AS total_points;
