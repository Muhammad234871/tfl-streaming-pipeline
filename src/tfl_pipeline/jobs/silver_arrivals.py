"""Bronze -> silver for arrivals: flatten, type, clean and deduplicate one day of data.

Usage:
    python -m tfl_pipeline.jobs.silver_arrivals --dt 2026-10-19 [--lake ./data/lake]
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tfl_pipeline.jobs.common import get_spark, read_bronze, write_partitioned


def transform(raw: DataFrame) -> DataFrame:
    """Turn raw bronze arrivals into clean, typed silver rows."""
    df = raw.select(
        F.col("poll_id"),
        F.col("ingested_at"),
        F.col("source"),
        F.col("data.modeName").alias("mode"),
        F.col("data.lineId").alias("line_id"),
        F.col("data.lineName").alias("line_name"),
        F.col("data.naptanId").alias("naptan_id"),
        F.col("data.stationName").alias("station_name"),
        F.col("data.platformName").alias("platform_name"),
        F.col("data.direction").alias("direction"),
        F.col("data.destinationNaptanId").alias("destination_naptan_id"),
        F.col("data.destinationName").alias("destination_name"),
        F.col("data.towards").alias("towards"),
        F.col("data.currentLocation").alias("current_location"),
        F.col("data.vehicleId").alias("vehicle_id"),
        F.col("data.id").alias("prediction_id"),
        F.col("data.timestamp").alias("prediction_ts"),
        F.col("data.expectedArrival").alias("expected_arrival"),
        F.col("dt"),
    )
    df = (
        df.withColumn("ingested_at", F.to_timestamp("ingested_at"))
        .withColumn("prediction_ts", F.to_timestamp("prediction_ts"))
        .withColumn("expected_arrival", F.to_timestamp("expected_arrival"))
        .withColumn(
            "time_to_station_s",
            F.round(
                F.col("expected_arrival").cast("double")
                - F.col("prediction_ts").cast("double")
            ).cast("int"),
        )
        .withColumn(
            "vehicle_id",
            F.when(F.col("vehicle_id").isin("", "000"), None).otherwise(
                F.col("vehicle_id")
            ),
        )
    )

    return df.where(
        F.col("line_id").isNotNull()
        & F.col("naptan_id").isNotNull()
        & F.col("expected_arrival").isNotNull()
    ).dropDuplicates()


# def write_silver(df: DataFrame, lake: str) -> None:
#     """Write silver arrival as a Parquet, replacing only the days present in df"""

#     path = f"{lake}/silver/arrivals"
#     (df.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(path))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", required=True, help="day to process, e.g. 2026-10-19")
    parser.add_argument("--lake", default="./data/lake", help="lake root folder")
    args = parser.parse_args()

    spark = get_spark("silver_arrivals")
    raw = read_bronze(spark, args.lake, "arrivals", args.dt)
    print("bronze rows:", raw.count())

    silver = transform(raw)
    write_partitioned(silver, f"{args.lake}/silver/arrivals")

    written = spark.read.parquet(f"{args.lake}/silver/arrivals").where(
        F.col("dt") == args.dt
    )
    print(f"silver rows written for {args.dt}:", written.count())
    spark.stop()


if __name__ == "__main__":
    main()
