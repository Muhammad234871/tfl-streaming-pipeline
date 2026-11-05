"""Silver -> gold: how much of each hour of one day was actually collected.

The producer polls TfL once a minute, so a complete hour has 60 polls. Hours with fewer
polls had gaps (the producer was stopped or the machine was off); the dashboard uses this
to tell "no trains" from "no data". Always 24 rows per day, including hours with 0 polls.

Usage:
    python -m tfl_pipeline.jobs.gold.collection_coverage --dt 2026-11-04 [--lake ./data/lake]
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tfl_pipeline.jobs.common import get_spark, read_silver, write_partitioned

EXPECTED_POLLS_PER_HOUR = 60  # POLL_INTERVAL_SECONDS = 60


def transform(silver: DataFrame, dt: str) -> DataFrame:
    """One row per hour 0-23: dt, hour, polls, coverage_pct."""
    spark = silver.sparkSession

    polls = silver.groupBy(F.hour("ingested_at").alias("hour")).agg(
        F.countDistinct("poll_id").alias("polls")
    )

    # All 24 hours, so hours without any data become rows with 0 polls.
    hours = spark.range(24).select(F.col("id").cast("int").alias("hour"))

    coverage = F.least(F.col("polls") / EXPECTED_POLLS_PER_HOUR * 100, F.lit(100.0))
    return (
        hours.join(polls, on="hour", how="left")
        .fillna(0, subset=["polls"])
        .withColumn("coverage_pct", F.round(coverage, 1))
        .withColumn("dt", F.lit(dt))
        .select("dt", "hour", "polls", "coverage_pct")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", required=True, help="day to process, e.g. 2026-11-04")
    parser.add_argument("--lake", default="./data/lake", help="lake root folder")
    args = parser.parse_args()

    out_path = f"{args.lake}/gold/collection_coverage"

    spark = get_spark("gold_collection_coverage")
    silver = read_silver(spark, args.lake, "arrivals", args.dt)

    gold = transform(silver, args.dt)
    write_partitioned(gold, out_path)
    written = spark.read.parquet(out_path).where(F.col("dt") == args.dt)
    total = written.agg(F.sum("polls")).first()[0]
    print(f"gold rows written for {args.dt}: {written.count()} ({total} polls)")
    written.orderBy("hour").show(24)

    spark.stop()


if __name__ == "__main__":
    main()
