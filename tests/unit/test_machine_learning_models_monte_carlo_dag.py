"""Unit tests for DAG ``nascar.cup.machine_learning.models.monte_carlo``."""

from __future__ import annotations

import nascar.cup.machine_learning_models_monte_carlo as mc_dag
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON
from nascar.cup.scraper.dag_factories import (
    CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
    CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
)

_EXPECTED_TASK_IDS = frozenset(
    {
        "wait_ml_predict_complete",
        "run_monte_carlo_simulation",
        "validate_simulation_batch",
    }
)


def test_dag_metadata() -> None:
    dag = mc_dag.dag
    assert dag.dag_id == "nascar.cup.machine_learning.models.monte_carlo"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON


def test_task_ids() -> None:
    dag = mc_dag.dag
    assert set(dag.task_dict.keys()) == _EXPECTED_TASK_IDS


def test_wait_sensor_targets_ml_predict() -> None:
    dag = mc_dag.dag
    wait = dag.get_task("wait_ml_predict_complete")
    assert wait.external_dag_id == "nascar.cup.machine_learning.models"
    assert wait.external_task_id == "predict_upcoming_week"
    assert wait.mode == "reschedule"
    assert wait.poke_interval == CUP_EXTERNAL_SENSOR_POKE_INTERVAL.total_seconds()
    assert wait.timeout == CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT.total_seconds()


def test_linear_task_chain() -> None:
    dag = mc_dag.dag
    wait = dag.get_task("wait_ml_predict_complete")
    run = dag.get_task("run_monte_carlo_simulation")
    validate = dag.get_task("validate_simulation_batch")

    assert wait.downstream_task_ids == {run.task_id}
    assert run.upstream_task_ids == {wait.task_id}
    assert run.downstream_task_ids == {validate.task_id}
    assert validate.upstream_task_ids == {run.task_id}
    assert validate.downstream_task_ids == set()
