
SELECT
    t.call_type,
    t.trip_id,
    p.seq,
    p.latitude,
    p.longitude
FROM Trip AS t
JOIN Point AS p
    ON t.trip_id = p.trip_id
ORDER BY t.call_type, t.trip_id, p.seq;
