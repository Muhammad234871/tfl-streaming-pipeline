"""Bronze -> silver for line status: one row per line per status, for one day.

Usage:
    python -m tfl_pipeline.jobs.silver_line_status --dt 2026-10-19 [--lake ./data/lake]
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from tfl_pipeline.jobs.common import get_spark, read_bronze, write_partitioned


def transform(raw: DataFrame) -> DataFrame:
    """One row per line per status, with typed and renamed columns"""
    exploded = raw.select(
        F.col("poll_id"),
        F.to_timestamp("ingested_at").alias("ingested_at"),
        F.col("data.id").alias("line_id"),
        F.col("data.name").alias("line_name"),
        F.col("data.modeName").alias("mode"),
        F.explode_outer("data.lineStatuses").alias("status"),
        F.col("dt"),
    )

    return exploded.select(
        "poll_id",
        "ingested_at",
        "line_id",
        "line_name",
        "mode",
        F.col("status.statusSeverity").cast("int").alias("status_severity"),
        F.col("status.statusSeverityDescription").alias("status_description"),
        F.col("status.reason").alias("reason"),
        F.col("status.disruption.category").alias("disruption_category"),
        "dt",
    ).dropDuplicates()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", required=True, help="day to process, e.g. 2026-10-19")
    parser.add_argument("--lake", default="./data/lake", help="lake root folder")
    args = parser.parse_args()

    spark = get_spark("silver_line_status")
    raw = read_bronze(spark, args.lake, "line_status", args.dt)
    print("bronze rows:", raw.count())

    silver = transform(raw)
    write_partitioned(silver, f"{args.lake}/silver/line_status")

    written = spark.read.parquet(f"{args.lake}/silver/line_status").where(
        F.col("dt") == args.dt
    )
    print(f"silver rows written for {args.dt}:", written.count())
    spark.stop()


if __name__ == "__main__":
    main()
