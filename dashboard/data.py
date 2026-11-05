"""Where the dashboard gets its data: local gold Parquet files, or Athena.

Set DASHBOARD_SOURCE=athena to query the lake in S3 instead of ./data/lake.
"""

import os

import pandas as pd

SOURCE = os.environ.get("DASHBOARD_SOURCE", "local")
LAKE = os.environ.get("TFL_LAKE", "./data/lake")

ATHENA_DATABASE = "tfl_lake"
ATHENA_WORKGROUP = "tfl"

# The only tables the dashboard may read. A table name can't be passed as a SQL
# parameter, so it goes into the SQL text; this list makes sure it's one of ours.
GOLD_TABLES = {
    "station_hourly",
    "line_status_daily",
    "line_headways",
    "collection_coverage",
}


# --- Public functions: app.py only calls these ---------------------------------


def read_gold(table: str, dt: str) -> pd.DataFrame:
    """One day of a gold table, e.g. read_gold("station_hourly", "2026-11-04")."""
    if table not in GOLD_TABLES:
        raise ValueError(f"Unknown table {table!r}, must be one of {GOLD_TABLES}")
    if SOURCE == "local":
        return _read_gold_local(table, dt)
    elif SOURCE == "athena":
        return _read_gold_athena(table, dt)
    else:
        raise ValueError(f"Unknown DASHBOARD_SOURCE={SOURCE!r}")


def available_days(table: str = "line_status_daily") -> list[str]:
    """The days that have gold data, newest first."""
    if table not in GOLD_TABLES:
        raise ValueError(f"Unknown table {table!r}, must be one of {GOLD_TABLES}")
    if SOURCE == "local":
        return _available_days_local(table)
    elif SOURCE == "athena":
        return _available_days_athena(table)
    else:
        raise ValueError(f"Unknown DASHBOARD_SOURCE={SOURCE!r}")


# --- Local Parquet files -------------------------------------------------------


def _read_gold_local(table: str, dt: str) -> pd.DataFrame:
    """One day of a gold table, e.g. read_gold("station_hourly", "2026-11-04")."""
    return pd.read_parquet(
        f"{LAKE}/gold/{table}",
        filters=[("dt", "==", dt)],
    )


def _available_days_local(table: str = "line_status_daily") -> list[str]:
    """The days that have gold data, newest first."""
    folders = os.listdir(f"{LAKE}/gold/{table}")
    days = [f.removeprefix("dt=") for f in folders if f.startswith("dt=")]
    return sorted(days, reverse=True)


# --- Athena --------------------------------------------------------------------


def _athena(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run one query in the tfl workgroup and return the result."""
    import awswrangler as wr  # imported here so local mode doesn't need AWS libraries

    return wr.athena.read_sql_query(
        sql=sql,
        database=ATHENA_DATABASE,
        workgroup=ATHENA_WORKGROUP,
        params=params,
        ctas_approach=False,  # read Athena's result file directly; no temporary table (needs only read permissions)
    )


def _read_gold_athena(table: str, dt: str) -> pd.DataFrame:
    """One day of a gold table, e.g. read_gold("station_hourly", "2026-11-04")."""
    sql = f"""
        SELECT *
        FROM {ATHENA_DATABASE}.gold_{table}
        WHERE dt =:dt
    """
    return _athena(sql, params={"dt": dt})


def _available_days_athena(table: str) -> list[str]:
    """The days that have gold data, newest first."""
    sql = f"""
        SELECT DISTINCT dt
        FROM {ATHENA_DATABASE}.gold_{table}
        ORDER BY dt DESC
    """
    df = _athena(sql)
    return df["dt"].tolist()
