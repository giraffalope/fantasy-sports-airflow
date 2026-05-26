"""Unit tests for DAG ``nascar.cup.machine_learning.models.backtest``."""

from __future__ import annotations

import nascar.cup.machine_learning_models_backtest as ml_backtest_dag
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON
from nascar.cup.scraper.dag_factories import (
    CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
    CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
)

_EXPECTED_TASK_IDS = frozenset(
    {
        "wait_ml_predict_complete",
        "run_ml_backtest",
    }
)


def test_dag_metadata() -> None:
    dag = ml_backtest_dag.dag
    assert dag.dag_id == "nascar.cup.machine_learning.models.backtest"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON


def test_task_ids() -> None:
    dag = ml_backtest_dag.dag
    assert set(dag.task_dict.keys()) == _EXPECTED_TASK_IDS


def test_wait_sensor_targets_ml_predict() -> None:
    dag = ml_backtest_dag.dag
    wait = dag.get_task("wait_ml_predict_complete")
    assert wait.external_dag_id == "nascar.cup.machine_learning.models"
    assert wait.external_task_id == "predict_upcoming_week"
    assert wait.mode == "reschedule"
    assert wait.poke_interval == CUP_EXTERNAL_SENSOR_POKE_INTERVAL.total_seconds()
    assert wait.timeout == CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT.total_seconds()


def test_linear_task_chain() -> None:
    dag = ml_backtest_dag.dag
    wait = dag.get_task("wait_ml_predict_complete")
    backtest = dag.get_task("run_ml_backtest")

    assert wait.downstream_task_ids == {backtest.task_id}
    assert backtest.upstream_task_ids == {wait.task_id}
    assert backtest.downstream_task_ids == set()
