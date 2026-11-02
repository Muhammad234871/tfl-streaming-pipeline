-- Minutes each line spent in a non-good status on one day, worst first.
-- A line can have several statuses at once, so its percentages can add up to more than 100%.
SELECT
    line_name,
    status_description,
    status_minutes,
    observed_minutes,
    pct_of_observed
FROM tfl_lake.gold_line_status_daily
WHERE dt = '2026-10-21'
  AND status_severity < 10
ORDER BY status_minutes DESC;
