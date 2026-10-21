"""Silver -> gold: train arrivals per station per hour, for one day.

Predictions are turned into arrival events first: a train counts as arriving when it is
predicted within 60 s of the station, and repeated sightings of the same train at the same
platform within 10 minutes count once.

Usage:
    python -m tfl_pipeline.jobs.gold_station_hourly --dt 2026-10-21 [--lake ./data/lake]
"""

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from tfl_pipeline.jobs.common import get_spark, read_silver, write_partitioned

ARRIVING_WITHIN_S = 60  # a prediction this close counts as "arriving now"
SAME_ARRIVAL_GAP_S = 600  # sightings closer than this are the same arrival


def arrival_events(silver: DataFrame) -> DataFrame:
    """One row per train arrival at a station platform."""
    near = silver.where(F.col("time_to_station_s") <= ARRIVING_WITHIN_S)

    with_key = near.withColumn(
        "train_key",
        F.coalesce(
            F.col("vehicle_id"),
            F.concat_ws("|", "destination_naptan_id", "prediction_id"),
        ),
    )
    w = Window.partitionBy(
        "line_id", "naptan_id", "platform_name", "train_key"
    ).orderBy("expected_arrival")
    # Window functions can't be used in a filter directly: store the result as a column first.
    with_prev = with_key.withColumn("prev_arrival", F.lag("expected_arrival").over(w))
    gap_s = F.col("expected_arrival").cast("long") - F.col("prev_arrival").cast("long")

    return with_prev.where(
        F.col("prev_arrival").isNull() | (gap_s > SAME_ARRIVAL_GAP_S)
    ).drop("prev_arrival")


def transform(silver: DataFrame) -> DataFrame:
    """Aggregate arrival events to one row per station per hour."""
    events = arrival_events(silver).withColumn("hour", F.hour("expected_arrival"))

    return events.groupBy("dt", "hour", "naptan_id", "station_name").agg(
        F.count("*").alias("train_arrivals"),
        F.countDistinct("line_id").alias("line_count"),
        F.array_sort(F.collect_set("line_id")).alias("lines"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", required=True, help="day to process, e.g. 2026-10-19")
    parser.add_argument("--lake", default="./data/lake", help="lake root folder")
    args = parser.parse_args()

    out_path = f"{args.lake}/gold/station_hourly"

    spark = get_spark("gold_station_hourly")
    silver = read_silver(spark, args.lake, "arrivals", args.dt)
    print("Silver rows:", silver.count())

    gold = transform(silver)
    write_partitioned(gold, out_path)
    written = spark.read.parquet(out_path).where(F.col("dt") == args.dt)
    print(f"gold rows written for {args.dt}:", written.count())
    written.orderBy(F.desc("train_arrivals")).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
