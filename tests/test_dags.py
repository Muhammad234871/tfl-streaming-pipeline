"""Structure tests for the Airflow DAG. Skipped automatically where Airflow isn't installed."""

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags")


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags["tfl_daily"]


def test_dags_import_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_dag_has_expected_tasks(dag):
    assert set(dag.task_ids) == {
        "check_bronze",
        "silver_arrivals",
        "silver_line_status",
        "gold_line_status_daily",
        "gold_station_hourly",
        "gold_line_headways",
        "check_quality",
    }


def test_dependencies(dag):
    downstream = {t.task_id: set(t.downstream_task_ids) for t in dag.tasks}
    assert downstream["check_bronze"] == {"silver_arrivals", "silver_line_status"}
    assert downstream["silver_arrivals"] == {
        "gold_station_hourly",
        "gold_line_headways",
    }
    assert downstream["silver_line_status"] == {"gold_line_status_daily"}
    assert dag.get_task("check_quality").upstream_task_ids == {
        "gold_line_status_daily",
        "gold_station_hourly",
        "gold_line_headways",
    }


def test_jobs_run_for_the_runs_logical_date(dag):
    for task in dag.tasks:
        assert "--dt {{ ds }}" in task.bash_command


def test_checks_do_not_retry_and_jobs_do(dag):
    assert dag.get_task("check_bronze").retries == 0
    assert dag.get_task("check_quality").retries == 0
    assert dag.get_task("silver_arrivals").retries == 2


def test_dag_settings_protect_a_small_machine(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.max_active_tasks == 1
