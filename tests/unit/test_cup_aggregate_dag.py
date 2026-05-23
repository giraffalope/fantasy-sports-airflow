"""Unit tests for DAG ``nascar.cup.aggregate``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import nascar.cup.aggregate as aggregate_dag
from include.nascar.cup.aggregate_load import (
    WAVE1_LOAD_TASKS,
    WAVE2_PREV_TRACK_LOAD_TASKS,
    aggregate_load_parameters,
)
from include.nascar.enums import NascarSeries
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON
from nascar.cup.scraper.dag_factories import (
    CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
    CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
)

_EXPECTED_TASK_IDS = frozenset(
    {
        "wait_importer_complete",
        "load_agg_week_results",
        "build_ml_training_snapshots",
        *WAVE1_LOAD_TASKS,
        *WAVE2_PREV_TRACK_LOAD_TASKS,
    }
)


def test_dag_metadata() -> None:
    dag = aggregate_dag.dag
    assert dag.dag_id == "nascar.cup.aggregate"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON


def test_wait_sensor_targets_importer_cleanup_task() -> None:
    dag = aggregate_dag.dag
    wait_importer_complete = dag.get_task("wait_importer_complete")

    assert wait_importer_complete.external_dag_id == "nascar.cup.importer"
    assert wait_importer_complete.external_task_id == "cleanup_importer_staging_and_temp_tables"
    assert wait_importer_complete.mode == "reschedule"
    assert wait_importer_complete.poke_interval == CUP_EXTERNAL_SENSOR_POKE_INTERVAL.total_seconds()
    assert wait_importer_complete.timeout == CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT.total_seconds()


def test_task_ids() -> None:
    dag = aggregate_dag.dag
    assert set(dag.task_dict.keys()) == _EXPECTED_TASK_IDS
    assert len(dag.task_dict) == 39


def test_wait_then_base_load() -> None:
    dag = aggregate_dag.dag
    wait_importer_complete = dag.get_task("wait_importer_complete")
    load_agg_week_results = dag.get_task("load_agg_week_results")

    assert wait_importer_complete.downstream_task_ids == {load_agg_week_results.task_id}


def test_base_load_upstream_of_wave1() -> None:
    dag = aggregate_dag.dag
    load_agg_week_results = dag.get_task("load_agg_week_results")

    assert load_agg_week_results.downstream_task_ids == set(WAVE1_LOAD_TASKS)
    for task_id in WAVE1_LOAD_TASKS:
        assert load_agg_week_results.task_id in dag.get_task(task_id).upstream_task_ids


def test_wave1_upstream_of_every_wave2_task() -> None:
    dag = aggregate_dag.dag
    expected_wave2 = set(WAVE2_PREV_TRACK_LOAD_TASKS)

    for task_id in WAVE1_LOAD_TASKS:
        assert dag.get_task(task_id).downstream_task_ids == expected_wave2

    for wave2_id in WAVE2_PREV_TRACK_LOAD_TASKS:
        assert dag.get_task(wave2_id).upstream_task_ids == set(WAVE1_LOAD_TASKS)


def test_wave2_upstream_of_ml_training_snapshots() -> None:
    dag = aggregate_dag.dag
    build_ml = dag.get_task("build_ml_training_snapshots")

    assert build_ml.upstream_task_ids == set(WAVE2_PREV_TRACK_LOAD_TASKS)
    for wave2_id in WAVE2_PREV_TRACK_LOAD_TASKS:
        assert build_ml.task_id in dag.get_task(wave2_id).downstream_task_ids


@patch("include.nascar.cup.aggregate_load.NascarMySqlHook")
def test_aggregate_load_parameters_returns_weeks(mock_hook_cls: MagicMock) -> None:
    mock_hook_cls.return_value.get_records.return_value = [
        (NascarSeries.CUP.series_id, 10),
        (NascarSeries.CUP.series_id, 20),
    ]

    params = aggregate_load_parameters({})

    assert params == {
        "series_id": NascarSeries.CUP.series_id,
        "weeks": [{"week_id": 10}, {"week_id": 20}],
    }
    mock_hook_cls.assert_called_once_with(mysql_conn_id="nascar_conn")
    mock_hook_cls.return_value.get_records.assert_called_once()
