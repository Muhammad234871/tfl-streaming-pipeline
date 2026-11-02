-- The 10 longest gaps between trains on Tube lines on one day.
-- Tube stations have NaPTAN IDs starting with 940GZZLU; platforms with fewer than
-- 5 trains in the hour are skipped, as one or two gaps say little about the service.
SELECT
    line_id,
    station_name,
    platform_name,
    hour,
    trains,
    median_headway_min,
    max_headway_min
FROM tfl_lake.gold_line_headways
WHERE dt = '2026-10-21'
  AND naptan_id LIKE '940GZZLU%'
  AND trains >= 5
ORDER BY max_headway_min DESC
LIMIT 10;
