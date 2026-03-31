"""Unit tests for DAG ``nascar.cup.scraper.frcs.race_week``."""

from __future__ import annotations

from pathlib import Path

import nascar.cup.scraper.frcs.race_week as race_week_dag
import nascar.cup.scraper.frcs.schedule as schedule_dag

from include.nascar.data_sources.frcs_race_week import (
    _frcs_week_loop_data_csv_has_data_rows,
    cup_series_sync_draftkings,
    cup_series_sync_entrylists,
    cup_series_sync_fanduel,
    cup_series_sync_lineups,
    cup_series_sync_penalties,
    cup_series_sync_pit_stall_selections,
    cup_series_sync_pit_stops,
    cup_series_sync_practices,
    cup_series_sync_qualifyings,
    cup_series_sync_race_results,
    cup_series_sync_weeks,
)


def test_loop_data_csv_skip_requires_data_rows(tmp_path: Path) -> None:
    week_dir = tmp_path / "cup" / "2025" / "01"
    week_dir.mkdir(parents=True)
    loop_csv = week_dir / "loop-data.csv"
    assert _frcs_week_loop_data_csv_has_data_rows(str(week_dir)) is False
    loop_csv.write_text("driver_name,start\n", encoding="utf-8")
    assert _frcs_week_loop_data_csv_has_data_rows(str(week_dir)) is False
    loop_csv.write_text("driver_name,start\nA. Driver,1\n", encoding="utf-8")
    assert _frcs_week_loop_data_csv_has_data_rows(str(week_dir)) is True


def test_dag_metadata() -> None:
    dag = race_week_dag.dag
    assert dag.dag_id == "nascar.cup.scraper.frcs.race_week"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert schedule_dag.NASCAR_FRCS_CUP_SCHEDULE_ASSET in dag.schedule


def test_task_ids_and_count() -> None:
    dag = race_week_dag.dag
    assert set(dag.task_dict.keys()) == {
        "wait_random_time",
        "sync_weeks",
        "sync_entrylists",
        "sync_practices",
        "sync_qualifyings",
        "sync_lineups",
        "sync_pit_stops",
        "sync_draftkings",
        "sync_fanduel",
        "sync_pit_stall_selections",
        "sync_penalties",
        "sync_race_results",
    }
    assert len(dag.tasks) == 12


def test_linear_dependencies() -> None:
    dag = race_week_dag.dag
    order = [
        "wait_random_time",
        "sync_weeks",
        "sync_entrylists",
        "sync_practices",
        "sync_qualifyings",
        "sync_lineups",
        "sync_pit_stall_selections",
        "sync_pit_stops",
        "sync_penalties",
        "sync_draftkings",
        "sync_fanduel",
        "sync_race_results",
    ]
    for i, tid in enumerate(order[:-1]):
        t = dag.get_task(tid)
        assert t.downstream_task_ids == {order[i + 1]}


def test_python_callables_match_operators() -> None:
    dag = race_week_dag.dag
    assert dag.get_task("sync_weeks").python_callable is cup_series_sync_weeks
    assert dag.get_task("sync_entrylists").python_callable is cup_series_sync_entrylists
    assert dag.get_task("sync_practices").python_callable is cup_series_sync_practices
    assert (
        dag.get_task("sync_qualifyings").python_callable is cup_series_sync_qualifyings
    )
    assert dag.get_task("sync_lineups").python_callable is cup_series_sync_lineups
    assert dag.get_task("sync_pit_stops").python_callable is cup_series_sync_pit_stops
    assert (
        dag.get_task("sync_draftkings").python_callable
        is cup_series_sync_draftkings
    )
    assert (
        dag.get_task("sync_fanduel").python_callable
        is cup_series_sync_fanduel
    )
    assert (
        dag.get_task("sync_pit_stall_selections").python_callable
        is cup_series_sync_pit_stall_selections
    )
    assert dag.get_task("sync_race_results").python_callable is cup_series_sync_race_results
    assert dag.get_task("sync_penalties").python_callable is cup_series_sync_penalties
