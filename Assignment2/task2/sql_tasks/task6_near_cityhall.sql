-- ------------------------------------------------------------
-- Task 6: Find candidate points near Porto City Hall
-- ------------------------------------------------------------
-- Bounding box around City Hall to reduce dataset size
-- (~0.002° ≈ 200 m margin)
SELECT
    p.trip_id,
    p.latitude,
    p.longitude
FROM Point AS p
WHERE p.latitude BETWEEN 41.1559 AND 41.1599
  AND p.longitude BETWEEN -8.6311 AND -8.6271;
