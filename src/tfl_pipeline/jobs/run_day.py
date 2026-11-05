"""Run the whole daily batch for one day, in the same order as the Airflow DAG.

For machines without Airflow (the EC2 timer in Phase 7.2):
    python -m tfl_pipeline.jobs.run_day --lake s3a://<bucket>          # yesterday (UTC)
    python -m tfl_pipeline.jobs.run_day --lake ./data/lake --dt 2026-11-03
"""

import argparse
import subprocess
import sys
from datetime import UTC, datetime, timedelta

from tfl_pipeline.jobs.bronze_exists import NO_DATA_EXIT_CODE

# The same order as dags/tfl_daily.py: silver before gold, quality last.
STEPS = [
    "bronze_exists",
    "silver.arrivals",
    "silver.line_status",
    "gold.line_status_daily",
    "gold.station_hourly",
    "gold.line_headways",
    "gold.collection_coverage",
    "quality",
]


def yesterday() -> str:
    """Yesterday's date in UTC as YYYY-MM-DD: the last complete day."""
    return (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")


def run_step(step: str, lake: str, dt: str) -> int:
    """Run one job as its own process and return its exit code."""
    command = [sys.executable, "-m", f"tfl_pipeline.jobs.{step}"]
    command += ["--lake", lake, "--dt", dt]
    print(f"==> {step} (dt={dt})", flush=True)
    return subprocess.run(command, check=False).returncode


def run_day(lake: str, dt: str) -> int:
    """Run every step in order. Returns 0 on success, 99 if skipped, else the failing code."""
    for step in STEPS:
        code = run_step(step, lake, dt)
        if step == "bronze_exists" and code == NO_DATA_EXIT_CODE:
            print(f"day {dt} skipped: no bronze data")
            return NO_DATA_EXIT_CODE
        if code != 0:
            print(f"day {dt} failed at {step} (exit code {code})")
            return code
    print(f"day {dt} done: all {len(STEPS)} steps succeeded")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lake", default="./data/lake", help="lake root (folder or s3a://bucket)"
    )
    parser.add_argument(
        "--dt", default=yesterday(), help="day to process (default: yesterday, UTC)"
    )
    args = parser.parse_args()
    sys.exit(run_day(args.lake, args.dt))


if __name__ == "__main__":
    main()
