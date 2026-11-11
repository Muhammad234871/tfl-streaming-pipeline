"""Data quality checks for one day of silver and gold. Exits with code 1 if any check fails.

The results (row counts, pass/warn/fail and messages per table) are also saved to
gold/pipeline_health, before exiting, so the dashboard can show failed days too.

Usage:
    python -m tfl_pipeline.jobs.quality --dt 2026-10-21 [--lake ./data/lake]
"""

import argparse
import sys
from datetime import UTC, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tfl_pipeline.jobs.common import get_spark, write_partitioned

TABLES = {
    "silver_arrivals": "silver/arrivals",
    "silver_line_status": "silver/line_status",
    "gold_line_status_daily": "gold/line_status_daily",
    "gold_station_hourly": "gold/station_hourly",
    "gold_line_headways": "gold/line_headways",
    "gold_line_status_intervals": "gold/line_status_intervals",
    "gold_collection_coverage": "gold/collection_coverage",
}

HEALTH_SCHEMA = (
    "table_name string, row_count bigint, status string, messages string, "
    "checked_at timestamp, dt string"
)


def check_quality(tables: dict[str, DataFrame]) -> tuple[list[str], list[str]]:
    """Return (failures, warnings) for one day of tables."""
    failures, warnings = [], []

    for name, df in tables.items():
        if df.count() == 0:
            failures.append(f"{name}: no rows")
    if failures:
        return failures, warnings  # the other checks are meaningless on empty tables

    arrivals = tables["silver_arrivals"]
    missing_keys = arrivals.where(
        F.col("line_id").isNull()
        | F.col("naptan_id").isNull()
        | F.col("expected_arrival").isNull()
    ).count()
    if missing_keys:
        failures.append(
            f"silver_arrivals: {missing_keys} rows missing line, station or arrival time"
        )

    duplicates = arrivals.count() - arrivals.dropDuplicates().count()
    if duplicates:
        failures.append(f"silver_arrivals: {duplicates} duplicate rows")

    bad_pct = (
        tables["gold_line_status_daily"]
        .where((F.col("pct_of_observed") < 0) | (F.col("pct_of_observed") > 100))
        .count()
    )
    if bad_pct:
        failures.append(
            f"gold_line_status_daily: {bad_pct} rows with pct_of_observed outside 0-100"
        )

    headways = tables["gold_line_headways"]
    bad_headways = headways.where(
        (F.col("median_headway_min") < 0) | (F.col("max_headway_min") > 30)
    ).count()
    if bad_headways:
        failures.append(
            f"gold_line_headways: {bad_headways} rows with headways outside 0-30 minutes"
        )

    zero_medians = headways.where(F.col("median_headway_min") == 0).count()
    if zero_medians:
        warnings.append(
            f"gold_line_headways: {zero_medians} rows with a 0-minute median (known double counting)"
        )

    return failures, warnings


def health_rows(
    dt: str,
    counts: dict[str, int],
    failures: list[str],
    warnings: list[str],
    checked_at: datetime,
) -> list[dict]:
    """One row per table: its row count, status (fail > warn > pass) and messages.

    Every message starts with its table's name ("silver_arrivals: 3 duplicate rows"),
    which is how it's matched to its table.
    """
    rows = []
    for table, row_count in counts.items():
        mine = [m for m in failures + warnings if m.startswith(f"{table}:")]
        if any(m in failures for m in mine):
            status = "fail"
        elif mine:
            status = "warn"
        else:
            status = "pass"
        rows.append(
            {
                "table_name": table,
                "row_count": row_count,
                "status": status,
                "messages": "; ".join(m.removeprefix(f"{table}: ") for m in mine),
                "checked_at": checked_at,
                "dt": dt,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", required=True, help="day to check, e.g. 2026-10-21")
    parser.add_argument("--lake", default="./data/lake", help="lake root folder")
    args = parser.parse_args()

    spark = get_spark("quality_checks")
    tables = {
        name: spark.read.parquet(f"{args.lake}/{path}").where(F.col("dt") == args.dt)
        for name, path in TABLES.items()
    }
    failures, warnings = check_quality(tables)

    # Save the results before deciding the exit code, so failed days are recorded too.
    counts = {name: df.count() for name, df in tables.items()}
    rows = health_rows(args.dt, counts, failures, warnings, datetime.now(UTC))
    health = spark.createDataFrame(rows, HEALTH_SCHEMA)
    write_partitioned(health, f"{args.lake}/gold/pipeline_health")
    spark.stop()

    for w in warnings:
        print("WARNING:", w)
    for f in failures:
        print("FAILED:", f)
    if failures:
        sys.exit(1)
    print(f"all quality checks passed for {args.dt}")


if __name__ == "__main__":
    main()
