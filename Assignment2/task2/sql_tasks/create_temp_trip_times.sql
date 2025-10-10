-- ------------------------------------------------------------
-- Create a reusable staging table for trip times
-- ------------------------------------------------------------

DROP TABLE IF EXISTS trip_times_stage;

CREATE TABLE trip_times_stage AS
SELECT
    t.trip_id,
    t.taxi_id,
    t.timestamp AS start_time,
    TIMESTAMPADD(SECOND, (MAX(p.seq) * 15), t.timestamp) AS end_time
FROM Trip t
JOIN Point p ON t.trip_id = p.trip_id
GROUP BY t.trip_id, t.taxi_id, t.timestamp;
