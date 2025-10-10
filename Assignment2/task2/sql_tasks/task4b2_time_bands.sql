
SELECT
    call_type,
    ROUND(SUM(HOUR(timestamp) BETWEEN 0 AND 5) / COUNT(*), 3) AS share_00_06,
    ROUND(SUM(HOUR(timestamp) BETWEEN 6 AND 11) / COUNT(*), 3) AS share_06_12,
    ROUND(SUM(HOUR(timestamp) BETWEEN 12 AND 17) / COUNT(*), 3) AS share_12_18,
    ROUND(SUM(HOUR(timestamp) BETWEEN 18 AND 23) / COUNT(*), 3) AS share_18_24
FROM Trip
GROUP BY call_type
ORDER BY call_type;
