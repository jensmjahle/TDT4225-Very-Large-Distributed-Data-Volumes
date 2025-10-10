-- ------------------------------------------------------------
-- Task 5: Retrieve per-taxi point data for time and distance computation
-- ------------------------------------------------------------

SELECT
    t.taxi_id,
    p.trip_id,
    p.seq,
    p.latitude,
    p.longitude
FROM Trip AS t
JOIN Point AS p ON t.trip_id = p.trip_id
ORDER BY t.taxi_id, p.trip_id, p.seq;
