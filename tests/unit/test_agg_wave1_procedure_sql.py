"""Contract tests for wave-1 aggregate stored procedures (delete-then-insert)."""

from __future__ import annotations

from pathlib import Path

_PROC_ROOT = (
    Path(__file__).resolve().parents[2].parent
    / "fantasy-sports-db"
    / "nascar"
    / "ddl"
    / "stored_procedures"
)


def _proc_text(name: str) -> str:
    return (_PROC_ROOT / name).read_text(encoding="utf-8")


def test_load_agg_week_teams_results_deletes_week_before_insert() -> None:
    sql = _proc_text("6022-create_load_agg_week_teams_results_stored_procedure.sql")
    assert "DELETE FROM nascar.agg_week_teams_results" in sql
    assert "WHERE series_id = p_series_id" in sql
    assert "AND week_id = p_week_id" in sql
    assert sql.index("DELETE FROM nascar.agg_week_teams_results") < sql.index(
        "insert into nascar.agg_week_teams_results"
    )


def test_load_agg_week_manufacturers_results_deletes_week_before_insert() -> None:
    sql = _proc_text("6021-create_load_agg_week_manufacturers_results_stored_procedure.sql")
    assert "DELETE FROM nascar.agg_week_manufacturers_results" in sql
    assert "WHERE series_id = p_series_id" in sql
    assert "AND week_id = p_week_id" in sql
    assert sql.index("DELETE FROM nascar.agg_week_manufacturers_results") < sql.index(
        "insert into nascar.agg_week_manufacturers_results"
    )
