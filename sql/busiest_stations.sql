-- The 10 busiest stations on one day: train arrivals summed over all collected hours.
SELECT
    station_name,
    SUM(train_arrivals) AS train_arrivals,
    MAX(line_count)     AS lines_served,
    COUNT(*)            AS hours_observed
FROM tfl_lake.gold_station_hourly
WHERE dt = '2026-10-21'
GROUP BY station_name
ORDER BY train_arrivals DESC
LIMIT 10;
