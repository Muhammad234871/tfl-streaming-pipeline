"""Silver -> gold: time between consecutive trains per line, station and platform, per hour.

Built on arrival events (see tfl_pipeline.jobs.gold.arrival_events). The headway is the
time from one arrival to the next on the same line and platform (so the same direction).
Gaps longer than MAX_HEADWAY_S are treated as gaps in data collection and dropped.

Usage:
    python -m tfl_pipeline.jobs.gold.line_headways --dt 2026-10-19 [--lake ./data/lake]
"""

import argparse

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from tfl_pipeline.jobs.common import get_spark, read_silver, write_partitioned
from tfl_pipeline.jobs.gold.arrival_events import arrival_events

MIN_HEADWAY_S = 60  # no two trains use one platform this close; shorter is a duplicate
MAX_HEADWAY_S = 1800  # longer gaps are treated as gaps in our data, not in the service


def headways(silver: DataFrame) -> DataFrame:
    """One row per arrival, with the seconds since the previous train on the same platform."""
    w = Window.partitionBy("line_id", "naptan_id", "platform_name").orderBy(
        "expected_arrival"
    )

    def seconds_since_previous() -> Column:
        prev = F.lag("expected_arrival").over(w)
        return F.col("expected_arrival").cast("long") - prev.cast("long")

    # Pass 1: drop arrivals that come too soon after the previous one (duplicates).
    events = arrival_events(silver).withColumn("gap_s", seconds_since_previous())
    distinct_trains = events.where(
        F.col("gap_s").isNull() | (F.col("gap_s") >= MIN_HEADWAY_S)
    ).drop("gap_s")

    # Pass 2: the real headways between the remaining trains.
    with_gap = distinct_trains.withColumn("headway_s", seconds_since_previous())
    return with_gap.where(
        F.col("headway_s").isNotNull() & (F.col("headway_s") <= MAX_HEADWAY_S)
    )


def transform(silver: DataFrame) -> DataFrame:
    """Aggregate headways to one row per line, station, platform and hour."""
    return (
        headways(silver)
        .withColumn("hour", F.hour("expected_arrival"))
        .groupBy("dt", "hour", "line_id", "naptan_id", "station_name", "platform_name")
        .agg(
            F.count("*").alias("trains"),
            F.round(F.avg("headway_s") / 60, 1).alias("avg_headway_min"),
            F.round(F.percentile_approx("headway_s", 0.5) / 60, 1).alias(
                "median_headway_min"
            ),
            F.round(F.max("headway_s") / 60, 1).alias("max_headway_min"),
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", required=True, help="day to process, e.g. 2026-10-19")
    parser.add_argument("--lake", default="./data/lake", help="lake root folder")
    args = parser.parse_args()

    out_path = f"{args.lake}/gold/line_headways"

    spark = get_spark("gold_line_headways")
    silver = read_silver(spark, args.lake, "arrivals", args.dt)
    print("Silver rows:", silver.count())

    gold = transform(silver)
    write_partitioned(gold, out_path)
    written = spark.read.parquet(out_path).where(F.col("dt") == args.dt)
    print(f"gold rows written for {args.dt}:", written.count())
    written.where(
        (F.col("line_id") == "victoria")
        & F.col("station_name").startswith("Oxford Circus")
    ).orderBy("hour", "platform_name").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
