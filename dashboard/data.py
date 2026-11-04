"""Where the dashboard gets its data: local gold Parquet files, or Athena (step 6.3)."""

import os

import pandas as pd

LAKE = os.environ.get("TFL_LAKE", "./data/lake")


def read_gold(table: str, dt: str) -> pd.DataFrame:
    """One day of a gold table, e.g. read_gold("station_hourly", "2026-11-04")."""
    return pd.read_parquet(
        f"{LAKE}/gold/{table}",
        filters=[("dt", "==", dt)],
    )


def available_days(table: str = "line_status_daily") -> list[str]:
    """The days that have gold data, newest first."""
    folders = os.listdir(f"{LAKE}/gold/{table}")
    days = [f.removeprefix("dt=") for f in folders if f.startswith("dt=")]
    return sorted(days, reverse=True)
