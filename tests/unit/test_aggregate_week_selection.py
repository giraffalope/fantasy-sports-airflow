"""Unit tests for incremental aggregate week selection SQL."""

from __future__ import annotations

from include.nascar.cup.aggregate_load import (
    FINISHED_AGG_WEEKS_SQL,
    WEEKS_TO_AGGREGATE_SQL,
    WEEKS_TO_LOAD_SQL,
)


def test_weeks_to_aggregate_uses_current_week_and_race_results() -> None:
    sql = WEEKS_TO_AGGREGATE_SQL
    assert "current_week" in sql
    assert "race_results" in sql
    assert "weeks_with_race" in sql
    assert "w.id <= cw.week_id" in sql
    assert "agg_week_teams_results" in sql
    assert "agg_week_manufacturers_results" in sql
    assert "agg_week_prev_track_cars_drivers_results" in sql


def test_weeks_to_aggregate_anchors_on_race_results_not_only_awr() -> None:
    sql = WEEKS_TO_AGGREGATE_SQL
    assert "weeks_with_race AS" in sql
    # Final select drives from weeks_with_race, not FROM agg_week_results alone.
    final_select_idx = sql.index("SELECT DISTINCT wr.series_id, wr.week_id")
    assert "FROM weeks_with_race wr" in sql[final_select_idx:]


def test_weeks_to_aggregate_history_coverage_complete_gate() -> None:
    sql = WEEKS_TO_AGGREGATE_SQL
    assert "history_coverage_complete" in sql
    assert "wc.week_id < cw.week_id" in sql


def test_weeks_to_aggregate_staleness_only_on_current_week() -> None:
    sql = WEEKS_TO_AGGREGATE_SQL
    # Staleness refresh must be gated to current week in steady-state, not backfill.
    assert "wr.week_id = cw.week_id" in sql
    assert "awr.updated_at >" in sql
    stale_idx = sql.index("awr.updated_at >")
    current_week_guard_idx = sql.rindex("wr.week_id = cw.week_id")
    assert current_week_guard_idx < stale_idx
    # Backfill branch must not use staleness on historical weeks.
    backfill_branch_idx = sql.index("hcc.is_complete = 0")
    assert backfill_branch_idx < stale_idx


def test_weeks_to_aggregate_two_mode_branches() -> None:
    sql = WEEKS_TO_AGGREGATE_SQL
    assert "hcc.is_complete = 0" in sql
    assert "hcc.is_complete = 1" in sql
    assert "week_completeness" in sql


def test_weeks_to_aggregate_placeholder_count() -> None:
    assert WEEKS_TO_AGGREGATE_SQL.count("%s") == 2


def test_weeks_to_aggregate_differs_from_finished_weeks() -> None:
    assert WEEKS_TO_AGGREGATE_SQL != FINISHED_AGG_WEEKS_SQL


def test_finished_agg_weeks_alias() -> None:
    assert WEEKS_TO_LOAD_SQL == FINISHED_AGG_WEEKS_SQL
