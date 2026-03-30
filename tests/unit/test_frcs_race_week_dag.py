"""Unit tests for DAG ``nascar_scraper_frcs_race_week``."""

from __future__ import annotations

import nascar.scraper.frcs.race_week as race_week_dag
import nascar.scraper.frcs.schedule as schedule_dag

from include.nascar.data_sources.frcs_race_week import (
    cup_series_sync_draftkings_points,
    cup_series_sync_entrylists,
    cup_series_sync_fanduel_points,
    cup_series_sync_lineups,
    cup_series_sync_penalties,
    cup_series_sync_pit_stall_selections,
    cup_series_sync_pit_stops,
    cup_series_sync_practices,
    cup_series_sync_prerace_penalties,
    cup_series_sync_qualifyings,
    cup_series_sync_race_results,
    cup_series_sync_weeks,
)


def test_dag_metadata() -> None:
    dag = race_week_dag.dag
    assert dag.dag_id == "nascar_scraper_frcs_race_week"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert schedule_dag.NASCAR_FRCS_CUP_SCHEDULE_ASSET in dag.schedule


def test_task_ids_and_count() -> None:
    dag = race_week_dag.dag
    assert set(dag.task_dict.keys()) == {
        "wait_random_time",
        "cup_series_sync_weeks",
        "cup_series_sync_entrylists",
        "cup_series_sync_practices",
        "cup_series_sync_qualifyings",
        "cup_series_sync_lineups",
        "cup_series_sync_pit_stops",
        "cup_series_sync_draftkings_points",
        "cup_series_sync_fanduel_points",
        "cup_series_sync_pit_stall_selections",
        "cup_series_sync_prerace_penalties",
        "cup_series_sync_penalties",
        "cup_series_sync_race_results",
    }
    assert len(dag.tasks) == 13


def test_linear_dependencies() -> None:
    dag = race_week_dag.dag
    order = [
        "wait_random_time",
        "cup_series_sync_weeks",
        "cup_series_sync_entrylists",
        "cup_series_sync_practices",
        "cup_series_sync_qualifyings",
        "cup_series_sync_lineups",
        "cup_series_sync_pit_stops",
        "cup_series_sync_draftkings_points",
        "cup_series_sync_fanduel_points",
        "cup_series_sync_pit_stall_selections",
        "cup_series_sync_prerace_penalties",
        "cup_series_sync_penalties",
        "cup_series_sync_race_results",
    ]
    for i, tid in enumerate(order[:-1]):
        t = dag.get_task(tid)
        assert t.downstream_task_ids == {order[i + 1]}


def test_python_callables_match_operators() -> None:
    dag = race_week_dag.dag
    assert dag.get_task("cup_series_sync_weeks").python_callable is cup_series_sync_weeks
    assert dag.get_task("cup_series_sync_entrylists").python_callable is cup_series_sync_entrylists
    assert dag.get_task("cup_series_sync_practices").python_callable is cup_series_sync_practices
    assert (
        dag.get_task("cup_series_sync_qualifyings").python_callable is cup_series_sync_qualifyings
    )
    assert dag.get_task("cup_series_sync_lineups").python_callable is cup_series_sync_lineups
    assert dag.get_task("cup_series_sync_pit_stops").python_callable is cup_series_sync_pit_stops
    assert (
        dag.get_task("cup_series_sync_draftkings_points").python_callable
        is cup_series_sync_draftkings_points
    )
    assert (
        dag.get_task("cup_series_sync_fanduel_points").python_callable
        is cup_series_sync_fanduel_points
    )
    assert (
        dag.get_task("cup_series_sync_pit_stall_selections").python_callable
        is cup_series_sync_pit_stall_selections
    )
    assert (
        dag.get_task("cup_series_sync_prerace_penalties").python_callable
        is cup_series_sync_prerace_penalties
    )
    assert dag.get_task("cup_series_sync_race_results").python_callable is cup_series_sync_race_results
    assert dag.get_task("cup_series_sync_penalties").python_callable is cup_series_sync_penalties
