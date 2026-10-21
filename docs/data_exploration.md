# Data exploration: TfL Unified API

Notes from exploring the source data before building the pipeline (October 2026).

## Endpoints used

| Data | Endpoint | Role in the pipeline |
|---|---|---|
| Arrivals (live train predictions) | `https://api.tfl.gov.uk/Mode/{mode}/Arrivals?count=-1` | Main stream → Kafka topic `tfl.arrivals.raw` |
| Line status (current status of every line) | `https://api.tfl.gov.uk/Line/Mode/tube,dlr,elizabeth-line,overground/Status` | Second stream → Kafka topic `tfl.line-status.raw` |
| Severity codes | `https://api.tfl.gov.uk/Line/Meta/Severity` | Reference data, loaded once as a dimension table (not streamed) |

## Volume

Records returned by one arrivals poll (`count=-1`):

| Mode | Records per poll |
|---|---|
| tube | 4,257 |
| overground | 3,133 |
| elizabeth-line | 2,155 |
| dlr | 251 |
| **Total** | **9,796** |

- Polling every 60 seconds, the theoretical maximum is 9,796 × 1,440 ≈ **14 million events per day**.
  The real number will be lower: far fewer trains run overnight, and the Night Tube only runs
  on Friday and Saturday. The measured daily figure replaces this estimate once the pipeline is running.
- Line status returns 18–19 lines per poll (one record per line).
- The severity endpoint returns 232 rows: one per severity level **per mode**.

> **Gotcha:** without `count=-1` the arrivals endpoint returns only around 100 records for the
> tube instead of about 4,300. The pipeline would silently collect about 3% of the data without failing.

## Arrivals: key fields

| Field | Meaning |
|---|---|
| `id` | Prediction ID |
| `vehicleId` | The train |
| `lineId` | Line, e.g. `metropolitan` |
| `naptanId` | Official station ID (use this for joins, not `stationName`) |
| `timestamp` | When TfL generated the prediction (not when we collected it) |
| `expectedArrival` | Predicted arrival time at the station |
| `timeToStation` | Seconds until arrival. **Not returned with `count=-1`**, so the pipeline never stores it (see Phase 3 findings) |
| `currentLocation` | Free text, e.g. "At Pinner Platform 1" |
| `$type`, `timing` | API metadata, mostly noise (`timing` is also absent with `count=-1`). Drop in silver. |

### Findings
- **`id` is not unique.** One tube poll had 4,313 records but only 3,952 distinct IDs, and
  almost all IDs appeared again in the next poll: TfL updates the same prediction every poll
  until the train arrives. `id` alone cannot be the primary key.
- **One train appears many times.** A `vehicleId` appears once for each upcoming station
  (e.g. Piccadilly train 224 appeared 22 times in one poll). One row means
  "train X is expected at station Y at time Z".
- **Placeholder values.** Some records have `vehicleId = "000"` (57 Metropolitan line records in
  one poll), which is not a real train. Silver keeps these rows and sets `vehicle_id` to NULL
  (see Phase 3 findings).
- Because `timestamp` is TfL's time, the pipeline adds its own `ingested_at` timestamp to each record.

## Findings from Spark exploration (Phase 3)

Bronze data loaded with PySpark (local lake, 2 days: 2026-10-15 and 2026-10-19, 534,182 arrival rows).
Spark read `dt` and `hour` directly from the Hive-style folder names (partition discovery).

| # | Finding | Evidence | Silver rule |
|---|---|---|---|
| 1 | **`timeToStation` and `timing` are not stored.** With `count=-1` the API returns a slimmer record without them. | Absent from the inferred schema | Derive `time_to_station_s = expected_arrival − prediction_ts` |
| 2 | **`(poll_id, id)` is not a unique key.** The DLR reuses one prediction `id` for several different trains at the same station (different platforms and arrival times). | 534,182 rows but 496,933 distinct `(poll_id, id)`; 18,471 shared keys. Example: one `id` at Gallions Reach with 5 trains, 4–10 minutes apart | Deduplicate on the **full row**: at-least-once replays are byte-identical. Only 52 rows were true duplicates |
| 3 | **Missing train numbers.** `vehicleId` is empty (mostly the DLR) or the placeholder `"000"`. | 16,836 empty + 6,076 `"000"` | **Keep the rows** (they are real arrivals) and set `vehicle_id` to NULL |
| 4 | **Timestamps are strings** with a varying number of decimals, e.g. `2026-10-19T10:18:22.89Z` and `...22.551Z`. | `printSchema()` shows `string` | Cast to `timestamp` |
| 5 | `timeToLive` always equals `expectedArrival`. | Sample rows | Drop as redundant |

Rows are dropped only when they cannot be placed at all: no `lineId`, no `naptanId` or no
`expectedArrival`.

> **Lesson:** the planned deduplication key `(poll_id, id)` would have silently deleted about
> 37,000 real DLR arrivals (7% of the rows). Exploring the stored data before writing the
> transformation caught it.

## Line status: key fields

| Field | Meaning |
|---|---|
| `id` | Line, e.g. `bakerloo` |
| `lineStatuses[]` | **A list:** a line can have several statuses at once |
| `lineStatuses[].statusSeverity` | Numeric code (see below) |
| `lineStatuses[].statusSeverityDescription` | Text, e.g. "Minor Delays" |
| `lineStatuses[].reason` | Human-readable explanation. Only present when there is a disruption. |
| `lineStatuses[].disruption` | Nested object with more detail |

### Findings
- **`lineStatuses` is a list.** During exploration the Elizabeth line had 4 statuses at the
  same time (severe delays on one section, minor delays on another). Silver needs one row
  per line per status (`explode` in Spark), not one row per line.
- `reason` and `disruption` are missing when the line has Good Service.

## Severity codes that matter

| Code | Meaning |
|---|---|
| 10 | Good Service |
| 9 | Minor Delays |
| 6 | Severe Delays |
| 2 | Suspended |
| 1 | Closed |
| 20 | Service Closed (normal overnight state, not a problem) |

Lower numbers are generally worse. The full list is at `/Line/Meta/Severity`.

## Ideas for analytics (gold layer)
- Minutes of severe or minor delays per line per day.
- Busiest stations by predicted arrivals per hour.
- Average time between trains (headway) per line and station.
