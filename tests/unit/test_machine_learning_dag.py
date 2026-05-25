"""Unit tests for DAG ``nascar.cup.machine_learning``."""

from __future__ import annotations

import nascar.cup.machine_learning as ml_dag
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON
from nascar.cup.scraper.dag_factories import (
    CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
    CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
)

_EXPECTED_TASK_IDS = frozenset(
    {
        "wait_aggregate_complete",
        "build_ml_training_snapshots",
        "train_models_if_needed",
        "predict_upcoming_week",
        "run_ml_backtest",
    }
)


def test_dag_metadata() -> None:
    dag = ml_dag.dag
    assert dag.dag_id == "nascar.cup.machine_learning"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON


def test_task_ids() -> None:
    dag = ml_dag.dag
    assert set(dag.task_dict.keys()) == _EXPECTED_TASK_IDS


def test_wait_sensor_targets_aggregate_done() -> None:
    dag = ml_dag.dag
    wait = dag.get_task("wait_aggregate_complete")
    assert wait.external_dag_id == "nascar.cup.aggregate"
    assert wait.external_task_id == "aggregate_done"
    assert wait.mode == "reschedule"
    assert wait.poke_interval == CUP_EXTERNAL_SENSOR_POKE_INTERVAL.total_seconds()
    assert wait.timeout == CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT.total_seconds()


def test_linear_task_chain() -> None:
    dag = ml_dag.dag
    wait = dag.get_task("wait_aggregate_complete")
    build = dag.get_task("build_ml_training_snapshots")
    train = dag.get_task("train_models_if_needed")
    predict = dag.get_task("predict_upcoming_week")
    backtest = dag.get_task("run_ml_backtest")

    assert wait.downstream_task_ids == {build.task_id}
    assert build.upstream_task_ids == {wait.task_id}
    assert build.downstream_task_ids == {train.task_id}
    assert train.upstream_task_ids == {build.task_id}
    assert train.downstream_task_ids == {predict.task_id}
    assert predict.upstream_task_ids == {train.task_id}
    assert predict.downstream_task_ids == {backtest.task_id}
    assert backtest.upstream_task_ids == {predict.task_id}
    assert backtest.downstream_task_ids == set()
