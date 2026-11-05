"""Exit 0 if bronze arrivals exist for a day, or 99 (Airflow's "skip" code) if not.

Usage:
    python -m tfl_pipeline.jobs.bronze_exists --dt 2026-10-21 [--lake ./data/lake | s3a://bucket]
"""

import argparse
import sys
from pathlib import Path

NO_DATA_EXIT_CODE = 99  # BashOperator marks the task as skipped on this exit code


def bronze_exists(lake: str, dt: str) -> bool:
    """True if at least one bronze arrivals file exists for dt."""
    prefix = f"bronze/arrivals/dt={dt}/"
    if lake.startswith(("s3://", "s3a://")):
        import boto3

        bucket, _, root = lake.split("://", 1)[1].partition("/")
        key_prefix = f"{root.strip('/')}/{prefix}" if root.strip("/") else prefix
        response = boto3.client("s3").list_objects_v2(
            Bucket=bucket, Prefix=key_prefix, MaxKeys=1
        )
        return response.get("KeyCount", 0) > 0
    return any(Path(lake, prefix).glob("hour=*/*.jsonl.gz"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dt", required=True, help="day to check, e.g. 2026-10-21")
    parser.add_argument(
        "--lake", default="./data/lake", help="lake root (folder or s3a://bucket)"
    )
    args = parser.parse_args()

    if bronze_exists(args.lake, args.dt):
        print(f"bronze arrivals found for {args.dt}")
        return
    print(f"no bronze arrivals for {args.dt}: skipping the rest of the run")
    sys.exit(NO_DATA_EXIT_CODE)


if __name__ == "__main__":
    main()
