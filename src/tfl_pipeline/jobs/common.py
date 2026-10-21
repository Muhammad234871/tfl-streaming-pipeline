"""Shared helpers for the Spark jobs."""

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_spark(app_name: str) -> SparkSession:
    """Create (or reuse) a local Spark session."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_partitioned(df: DataFrame, path: str, partition_col: str = "dt") -> None:
    """Write df as Parquet partitioned by partition_col, replacing only the partitions present in df.

    Requires spark.sql.sources.partitionOverwriteMode=dynamic (set in get_spark).
    """
    (
        df.repartition(partition_col)
        .write.mode("overwrite")
        .partitionBy(partition_col)
        .parquet(path)
    )


def read_bronze(spark: SparkSession, lake: str, dataset: str, dt: str) -> DataFrame:
    """Read one day of a bronze dataset (e.g. 'arrivals', 'line_status').

    Reads the dataset root and filters on the dt partition, so Spark keeps the dt
    column and only opens that day's files (partition pruning).
    """
    return spark.read.json(f"{lake}/bronze/{dataset}").where(F.col("dt") == dt)
