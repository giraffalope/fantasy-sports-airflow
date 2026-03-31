"""Unit tests for DAG ``nascar.cup.scraper.racing_reference.race_week``."""

from __future__ import annotations

import nascar.cup.scraper.racing_reference.race_week as race_week_dag
import nascar.cup.scraper.racing_reference.schedule as schedule_dag

from include.nascar.data_sources.racing_reference_race_week import (
    cup_series_sync_caution_flags,
    cup_series_sync_entrylists,
    cup_series_sync_penalties_and_lap_leaders,
    cup_series_sync_practices,
    cup_series_sync_prerace_penalties,
    cup_series_sync_qualifyings,
    cup_series_sync_race_results,
    cup_series_sync_weeks,
)


def test_dag_metadata() -> None:
    dag = race_week_dag.dag
    assert dag.dag_id == "nascar.cup.scraper.racing_reference.race_week"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert schedule_dag.NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET in dag.schedule


def test_task_ids_and_count() -> None:
    dag = race_week_dag.dag
    assert set(dag.task_dict.keys()) == {
        "wait_random_time",
        "sync_weeks",
        "sync_entrylists",
        "sync_practices",
        "sync_qualifyings",
        "sync_prerace_penalties",
        "sync_caution_flags",
        "sync_race_results",
        "sync_penalties_and_lap_leaders",
    }
    assert len(dag.tasks) == 9


def test_linear_dependencies() -> None:
    dag = race_week_dag.dag
    order = [
        "wait_random_time",
        "sync_weeks",
        "sync_entrylists",
        "sync_practices",
        "sync_qualifyings",
        "sync_prerace_penalties",
        "sync_caution_flags",
        "sync_penalties_and_lap_leaders",
        "sync_race_results",
    ]
    for i, tid in enumerate(order[:-1]):
        t = dag.get_task(tid)
        assert t.downstream_task_ids == {order[i + 1]}


def test_python_callables_match_operators() -> None:
    dag = race_week_dag.dag
    assert (
        dag.get_task("sync_weeks").python_callable is cup_series_sync_weeks
    )
    assert dag.get_task("sync_entrylists").python_callable is cup_series_sync_entrylists
    assert dag.get_task("sync_practices").python_callable is cup_series_sync_practices
    assert (
        dag.get_task("sync_qualifyings").python_callable is cup_series_sync_qualifyings
    )
    assert (
        dag.get_task("sync_prerace_penalties").python_callable
        is cup_series_sync_prerace_penalties
    )
    assert (
        dag.get_task("sync_caution_flags").python_callable is cup_series_sync_caution_flags
    )
    assert (
        dag.get_task("sync_race_results").python_callable is cup_series_sync_race_results
    )
    assert (
        dag.get_task("sync_penalties_and_lap_leaders").python_callable
        is cup_series_sync_penalties_and_lap_leaders
    )
