"""Unit tests for DAG ``nascar.cup.dfs.lineup_generator``."""

from __future__ import annotations

import nascar.cup.dfs.lineup_generator as lineup_dag
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON
from nascar.cup.scraper.dag_factories import (
    CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
    CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
)


def test_dag_metadata() -> None:
    dag = lineup_dag.dag
    assert dag.dag_id == "nascar.cup.dfs.lineup_generator"
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON


def test_wait_sensor_targets_ml_predict() -> None:
    dag = lineup_dag.dag
    wait = dag.get_task("wait_ml_predict_complete")
    assert wait.external_dag_id == "nascar.cup.machine_learning"
    assert wait.external_task_id == "predict_upcoming_week"
    assert wait.poke_interval == CUP_EXTERNAL_SENSOR_POKE_INTERVAL.total_seconds()
    assert wait.timeout == CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT.total_seconds()


def test_wait_sensor_targets_dk_salary_import() -> None:
    dag = lineup_dag.dag
    wait = dag.get_task("wait_dk_salaries_complete")
    assert wait.external_dag_id == "nascar.cup.dfs.salary_importer"
    assert wait.external_task_id == "import_draftkings_salaries"


def test_task_chain() -> None:
    dag = lineup_dag.dag
    wait_ml = dag.get_task("wait_ml_predict_complete")
    wait_salaries = dag.get_task("wait_dk_salaries_complete")
    generate = dag.get_task("generate_draftkings_lineups")
    assert wait_ml.downstream_task_ids == {generate.task_id}
    assert wait_salaries.downstream_task_ids == {generate.task_id}
