"""Shared pytest fixtures."""

import os
import sys
from datetime import date, datetime

import pytest


@pytest.fixture(scope="session")
def spark():
    """One small local Spark session for the whole test run."""
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


SILVER_ARRIVALS_SCHEMA = (
    "line_id string, naptan_id string, station_name string, platform_name string, "
    "vehicle_id string, destination_naptan_id string, prediction_id string, "
    "expected_arrival timestamp, time_to_station_s int, dt date"
)


@pytest.fixture
def silver_arrivals(spark):
    """Build a tiny silver arrivals table at one platform: rows of (vehicle_id, 'HH:MM:SS', time_to_station_s)."""

    def build(rows):
        data = [
            (
                "victoria",
                "940GZZLUOXC",
                "Oxford Circus",
                "Northbound - Platform 6",
                vehicle_id,
                "940GZZLUWWL",
                f"p{i}",
                datetime.fromisoformat(f"2026-10-19T{hhmmss}"),
                time_to_station_s,
                date(2026, 10, 19),
            )
            for i, (vehicle_id, hhmmss, time_to_station_s) in enumerate(rows)
        ]
        return spark.createDataFrame(data, SILVER_ARRIVALS_SCHEMA)

    return build
