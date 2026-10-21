"""Silver -> gold: minutes per line per status for one day.

Usage:
    python -m tfl_pipeline.jobs.gold.line_status_daily --dt 2026-10-19 [--lake ./data/lake]
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tfl_pipeline.jobs.common import get_spark, read_silver, write_partitioned


def transform(silver: DataFrame) -> DataFrame:
    """Aggregate line status to one row per line, day and status."""
    per_status = silver.groupBy(
        "dt", "line_id", "line_name", "mode", "status_severity", "status_description"
    ).agg(F.countDistinct("poll_id").alias("status_minutes"))

    observed = silver.groupBy("dt", "line_id").agg(
        F.countDistinct("poll_id").alias("observed_minutes")
    )

    return per_status.join(observed, ["dt", "line_id"]).withColumn(
        "pct_of_observed",
        F.round(100 * F.col("status_minutes") / F.col("observed_minutes"), 1),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", required=True, help="day to process, e.g. 2026-10-19")
    parser.add_argument("--lake", default="./data/lake", help="lake root folder")
    args = parser.parse_args()

    spark = get_spark("gold_line_status_daily")
    silver = read_silver(spark, args.lake, "line_status", args.dt)
    print("Silver rows:", silver.count())

    gold = transform(silver)
    write_partitioned(gold, f"{args.lake}/gold/line_status_daily")

    written = spark.read.parquet(f"{args.lake}/gold/line_status_daily").where(
        F.col("dt") == args.dt
    )
    print(f"gold rows written for {args.dt}:", written.count())
    written.where(F.col("status_severity") < 10).orderBy(F.desc("status_minutes")).show(
        truncate=False
    )
    spark.stop()


if __name__ == "__main__":
    main()
