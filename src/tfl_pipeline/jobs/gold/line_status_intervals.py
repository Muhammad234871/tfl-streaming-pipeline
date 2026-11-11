"""Silver -> gold: when each line was in each status, as time intervals, for one day.

Silver has one row per line, status and poll (every minute). This joins consecutive polls
with the same status into one interval with a start and an end, so the dashboard can draw
a timeline ("Bakerloo: Severe Delays 07:42-09:10").

A gap of more than MAX_GAP_S between two polls of a status ends the interval. Missing one
poll doesn't split it, but a longer gap in our collection does: we don't know the status
while we weren't collecting, so we don't draw it.

Usage:
    python -m tfl_pipeline.jobs.gold.line_status_intervals --dt 2026-11-06 [--lake ./data/lake]
"""

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from tfl_pipeline.jobs.common import get_spark, read_silver, write_partitioned

POLL_INTERVAL_S = 60
MAX_GAP_S = 180  # up to two missed polls still count as the same interval

STATUS_KEY = ["dt", "line_id", "status_severity", "status_description"]


def transform(silver: DataFrame) -> DataFrame:
    """One row per line, status and unbroken run of polls (gaps-and-islands)."""
    # One row per poll for each line and status (a poll can repeat a status row).
    polls = silver.groupBy(*STATUS_KEY, "line_name", "mode", "poll_id").agg(
        F.min("ingested_at").alias("ingested_at"),
        F.max("reason").alias("reason"),
    )

    # A new interval starts where the previous poll of this status is too long ago.
    w = Window.partitionBy(*STATUS_KEY).orderBy("ingested_at")
    gap_s = F.col("ingested_at").cast("long") - F.lag("ingested_at").over(w).cast(
        "long"
    )
    starts = polls.withColumn(
        "is_start", F.when(gap_s.isNull() | (gap_s > MAX_GAP_S), 1).otherwise(0)
    )
    # Running total of starts = an id for each interval within its status.
    numbered = starts.withColumn(
        "interval_id",
        F.sum("is_start").over(w.rowsBetween(Window.unboundedPreceding, 0)),
    )

    return (
        numbered.groupBy(*STATUS_KEY, "line_name", "mode", "interval_id")
        .agg(
            F.min("ingested_at").alias("start_ts"),
            # The last poll stands for the minute after it, like status_minutes in gold.
            (
                F.max("ingested_at") + F.expr(f"INTERVAL {POLL_INTERVAL_S} SECONDS")
            ).alias("end_ts"),
            F.count("*").alias("polls"),
            # The latest explanation TfL gave during the interval.
            F.max_by("reason", "ingested_at").alias("reason"),
        )
        .drop("interval_id")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", required=True, help="day to process, e.g. 2026-11-06")
    parser.add_argument("--lake", default="./data/lake", help="lake root folder")
    args = parser.parse_args()

    out_path = f"{args.lake}/gold/line_status_intervals"

    spark = get_spark("gold_line_status_intervals")
    silver = read_silver(spark, args.lake, "line_status", args.dt)

    gold = transform(silver)
    write_partitioned(gold, out_path)

    written = spark.read.parquet(out_path).where(F.col("dt") == args.dt)
    print(f"gold rows written for {args.dt}:", written.count())
    written.where(F.col("status_severity") < 10).orderBy("line_id", "start_ts").select(
        "line_name", "status_description", "start_ts", "end_ts", "polls"
    ).show(30, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
