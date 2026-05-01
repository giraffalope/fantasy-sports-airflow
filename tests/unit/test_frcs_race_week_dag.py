"""Unit tests for DAG ``nascar.cup.scraper.frcs.race_week``."""

from __future__ import annotations

from pathlib import Path

import nascar.cup.scraper.frcs.race_week as race_week_dag
from nascar.cup.scraper.backfill_assets import (
    BACKFILL_YEARLY_SCHEDULE,
    CUP_SCRAPER_PROD_HOURLY_CRON,
)

from include.nascar.data_sources.frcs_race_week import (
    _frcs_purge_week_scrape_artifacts_if_loop_incomplete,
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


def test_purge_skips_when_loop_data_csv_has_data_rows(tmp_path: Path) -> None:
    week_dir = tmp_path / "w"
    week_dir.mkdir()
    (week_dir / "loop-data.csv").write_text("h\nrow\n", encoding="utf-8")
    (week_dir / "race-results.html").write_text("<html/>", encoding="utf-8")
    (week_dir / "practice-01.html").write_text("<html/>", encoding="utf-8")
    p = str(week_dir)
    _frcs_purge_week_scrape_artifacts_if_loop_incomplete("t", 2025, 3, p)
    assert (week_dir / "race-results.html").is_file()
    assert (week_dir / "practice-01.html").is_file()
    assert (week_dir / "loop-data.csv").is_file()


def test_purge_removes_week_artifacts_when_loop_incomplete(tmp_path: Path) -> None:
    week_dir = tmp_path / "w"
    week_dir.mkdir()
    names = [
        "race-results.html",
        "entrylist.html",
        "qualifying.html",
        "lineup.html",
        "pit-stops.html",
        "loop-data.html",
        "draftkings.html",
        "fanduel.html",
        "pit-stall-selections.pdf",
        "practice-01.html",
        "practice-02.html",
    ]
    for n in names:
        (week_dir / n).write_text("x", encoding="utf-8")
    (week_dir / "loop-data.csv").write_text("header only\n", encoding="utf-8")
    p = str(week_dir)
    _frcs_purge_week_scrape_artifacts_if_loop_incomplete("t", 2026, 10, p)
    for n in names:
        assert not (week_dir / n).is_file(), n
    assert (week_dir / "loop-data.csv").is_file()


def test_purge_removes_artifacts_when_no_loop_csv(tmp_path: Path) -> None:
    week_dir = tmp_path / "w"
    week_dir.mkdir()
    (week_dir / "race-results.html").write_text("old", encoding="utf-8")
    p = str(week_dir)
    _frcs_purge_week_scrape_artifacts_if_loop_incomplete("t", 2026, 1, p)
    assert not (week_dir / "race-results.html").is_file()


def test_dag_metadata() -> None:
    dag = race_week_dag.dag
    assert dag.dag_id == "nascar.cup.scraper.frcs.race_week"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON


def test_backfill_dag_metadata() -> None:
    dag = race_week_dag.backfill_dag
    assert dag.dag_id == "backfill.nascar.cup.scraper.frcs.race_week"
    assert dag.catchup is True
    assert dag.schedule == BACKFILL_YEARLY_SCHEDULE


def test_wait_upstream_schedule_sensor_targets_schedule_dag() -> None:
    prod = race_week_dag.dag.get_task("wait_upstream_schedule_convert")
    assert prod.external_dag_id == "nascar.cup.scraper.frcs.schedule"
    assert prod.external_task_id == "convert_year_schedule_html_to_csv"
    backfill = race_week_dag.backfill_dag.get_task("wait_upstream_schedule_convert")
    assert backfill.external_dag_id == "backfill.nascar.cup.scraper.frcs.schedule"
    assert backfill.external_task_id == "convert_year_schedule_html_to_csv"


def test_task_ids_and_count() -> None:
    dag = race_week_dag.dag
    assert set(dag.task_dict.keys()) == {
        "wait_random_time",
        "wait_upstream_schedule_convert",
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
    assert len(dag.tasks) == 13


def test_linear_dependencies() -> None:
    dag = race_week_dag.dag
    order = [
        "wait_random_time",
        "wait_upstream_schedule_convert",
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
