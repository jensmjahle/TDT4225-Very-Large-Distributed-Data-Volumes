SELECT
    call_type,
    COUNT(*) AS total_trips,
    AVG(TIMESTAMPDIFF(SECOND, MIN(timestamp), MAX(timestamp))) AS avg_duration_sec
FROM Trip
GROUP BY call_type;
