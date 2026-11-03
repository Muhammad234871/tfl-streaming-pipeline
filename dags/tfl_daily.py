"""Daily TfL batch pipeline: bronze -> silver -> gold for one day (the run's logical date)."""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

REPO = os.environ.get(
    "TFL_REPO", "/home/usama/Python/Data Engineering Projects/tfl-streaming-pipeline"
)

PYTHON = f"{REPO}/.venv/bin/python"
LAKE = os.environ.get("TFL_LAKE", f"{REPO}/data/lake")


def spark_job(task_id: str, module: str) -> BashOperator:
    """A task that runs one Spark job for the run's day: --dt {{ ds }}."""
    return BashOperator(
        task_id=task_id,
        bash_command=f'"{PYTHON}" -m tfl_pipeline.jobs.{module} --lake "{LAKE}" --dt '
        + "{{ ds }}",
        cwd=REPO,
        env={"SPARK_CONF_DIR": f"{REPO}/conf"},
        append_env=True,
    )


with DAG(
    dag_id="tfl_daily",
    schedule="@daily",
    start_date=datetime(2026, 10, 15),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["tfl"],
) as dag:
    silver_arrivals = spark_job("silver_arrivals", "silver.arrivals")
    silver_line_status = spark_job("silver_line_status", "silver.line_status")
    gold_line_status_daily = spark_job(
        "gold_line_status_daily", "gold.line_status_daily"
    )
    gold_station_hourly = spark_job("gold_station_hourly", "gold.station_hourly")
    gold_line_headways = spark_job("gold_line_headways", "gold.line_headways")
    check_quality = spark_job("check_quality", "quality")

    silver_arrivals >> [gold_station_hourly, gold_line_headways]
    silver_line_status >> [gold_line_status_daily]

    [gold_line_status_daily, gold_station_hourly, gold_line_headways] >> check_quality
