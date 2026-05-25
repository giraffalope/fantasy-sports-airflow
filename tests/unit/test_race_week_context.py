"""Tests for race-week DAG context helpers and loop-data skip behavior."""

from __future__ import annotations

from pathlib import Path

from include.nascar.data_sources import frcs_race_week as frcs_rw
from include.nascar.data_sources import racing_reference_race_week as rr_rw
from include.nascar.data_sources.race_week_context import is_backfill_race_week_dag


class _FakeDag:
    def __init__(self, dag_id: str) -> None:
        self.dag_id = dag_id


def test_is_backfill_race_week_dag_true() -> None:
    assert is_backfill_race_week_dag(
        {"dag": _FakeDag("backfill.nascar.cup.scraper.frcs.race_week")}
    )


def test_is_backfill_race_week_dag_false_prod() -> None:
    assert not is_backfill_race_week_dag({"dag": _FakeDag("nascar.cup.scraper.frcs.race_week")})


def test_is_backfill_race_week_dag_false_no_dag() -> None:
    assert not is_backfill_race_week_dag({})


def _write_loop_data_with_rows(week_dir: Path) -> None:
    week_dir.mkdir(parents=True, exist_ok=True)
    (week_dir / "loop-data.csv").write_text("h1,h2\nv1,v2\n", encoding="utf-8")


def test_frcs_week_skip_bypassed_for_backfill_dag(tmp_path: Path) -> None:
    week = tmp_path / "03"
    _write_loop_data_with_rows(week)
    ctx = {"dag": _FakeDag("backfill.nascar.cup.scraper.frcs.race_week")}
    assert not frcs_rw._frcs_week_skip_if_loop_csv_present("t", 2026, 3, str(week), ctx)


def test_frcs_week_skip_when_loop_csv_present_and_prod_dag(tmp_path: Path) -> None:
    week = tmp_path / "03"
    _write_loop_data_with_rows(week)
    ctx = {"dag": _FakeDag("nascar.cup.scraper.frcs.race_week")}
    assert frcs_rw._frcs_week_skip_if_loop_csv_present("t", 2026, 3, str(week), ctx)


def test_rr_week_skip_bypassed_for_backfill_dag(tmp_path: Path) -> None:
    week = tmp_path / "03"
    _write_loop_data_with_rows(week)
    ctx = {"dag": _FakeDag("backfill.nascar.cup.scraper.racing_reference.race_week")}
    assert not rr_rw._rr_week_skip_if_loop_csv_present("t", 2026, 3, str(week), ctx)


def test_rr_week_skip_when_loop_csv_present_and_prod_dag(tmp_path: Path) -> None:
    week = tmp_path / "03"
    _write_loop_data_with_rows(week)
    ctx = {"dag": _FakeDag("nascar.cup.scraper.racing_reference.race_week")}
    assert rr_rw._rr_week_skip_if_loop_csv_present("t", 2026, 3, str(week), ctx)
