"""Unit tests for DAG ``nascar.cup.aggregate``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import nascar.cup.aggregate as aggregate_dag
from include.nascar.cup.aggregate_load import (
    FINISHED_AGG_WEEKS_SQL,
    WAVE1_LOAD_TASKS,
    WAVE2_PREV_TRACK_LOAD_TASKS,
    WEEKS_TO_AGGREGATE_SQL,
    aggregate_base_load_parameters,
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
        "aggregate_done",
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


def test_wave2_upstream_of_aggregate_done_sentinel() -> None:
    dag = aggregate_dag.dag
    aggregate_done = dag.get_task("aggregate_done")

    assert aggregate_done.upstream_task_ids == set(WAVE2_PREV_TRACK_LOAD_TASKS)
    for wave2_id in WAVE2_PREV_TRACK_LOAD_TASKS:
        assert aggregate_done.task_id in dag.get_task(wave2_id).downstream_task_ids


def test_aggregate_base_load_parameters_returns_series_id() -> None:
    assert aggregate_base_load_parameters({}) == {"series_id": NascarSeries.CUP.series_id}


@patch("include.nascar.cup.aggregate_load.NascarMySqlHook")
def test_aggregate_load_parameters_uses_incremental_sql(mock_hook_cls: MagicMock) -> None:
    mock_hook_cls.return_value.get_records.return_value = [
        (NascarSeries.CUP.series_id, 10),
        (NascarSeries.CUP.series_id, 20),
    ]

    params = aggregate_load_parameters({})

    assert params == {
        "series_id": NascarSeries.CUP.series_id,
        "weeks": [{"week_id": 10}, {"week_id": 20}],
    }
    sql = mock_hook_cls.return_value.get_records.call_args[0][0]
    assert sql == WEEKS_TO_AGGREGATE_SQL
    assert mock_hook_cls.return_value.get_records.call_args[1]["parameters"] == (
        NascarSeries.CUP.series_id,
        NascarSeries.CUP.series_id,
    )


@patch("include.nascar.cup.aggregate_load.NascarMySqlHook")
def test_aggregate_load_parameters_empty_when_nothing_needed(
    mock_hook_cls: MagicMock,
) -> None:
    mock_hook_cls.return_value.get_records.return_value = []

    params = aggregate_load_parameters({})

    assert params == {"series_id": NascarSeries.CUP.series_id, "weeks": []}


@patch("include.nascar.cup.aggregate_load.NascarMySqlHook")
def test_aggregate_load_parameters_rebuild_all_uses_finished_weeks_sql(
    mock_hook_cls: MagicMock,
    monkeypatch,
) -> None:
    monkeypatch.setenv("FS_AGG_REBUILD_ALL_WEEKS", "1")
    mock_hook_cls.return_value.get_records.return_value = [
        (NascarSeries.CUP.series_id, 5),
    ]

    params = aggregate_load_parameters({})

    assert params["weeks"] == [{"week_id": 5}]
    sql = mock_hook_cls.return_value.get_records.call_args[0][0]
    assert sql == FINISHED_AGG_WEEKS_SQL
