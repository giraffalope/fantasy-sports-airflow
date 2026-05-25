"""Unit tests for incremental aggregate week selection SQL."""

from __future__ import annotations

import pytest

from include.nascar.cup.aggregate_load import (
    AGG_TABLE_BY_TASK,
    FINISHED_AGG_WEEKS_SQL,
    WEEKS_TO_AGGREGATE_SQL,
    WEEKS_TO_LOAD_SQL,
    build_weeks_to_aggregate_sql,
)


def test_build_weeks_to_aggregate_uses_v_series_current_week() -> None:
    sql = build_weeks_to_aggregate_sql("agg_week_teams_results")
    assert "v_series_current_week" in sql
    assert "week_completeness" not in sql


def test_build_weeks_to_aggregate_per_table_not_exists() -> None:
    sql = build_weeks_to_aggregate_sql("agg_week_manufacturers_results")
    assert "nascar.agg_week_manufacturers_results t" in sql
    assert "NOT EXISTS" in sql


def test_build_weeks_to_aggregate_scope_uses_agg_week_results() -> None:
    sql = build_weeks_to_aggregate_sql("agg_week_teams_results")
    scope_idx = sql.index("weeks_in_scope AS")
    scope_block = sql[scope_idx : sql.index("SELECT DISTINCT wr.series_id")]
    assert "nascar.agg_week_results awr" in scope_block
    assert "awr.week_id < cw.week_id" in scope_block
    assert "race_results" not in scope_block


def test_build_weeks_to_aggregate_final_filter_current_or_missing() -> None:
    sql = build_weeks_to_aggregate_sql("agg_week_teams_results")
    assert "wr.week_id = cw.week_id" in sql
    assert "wr.week_id < cw.week_id" in sql
    assert "NOT EXISTS" in sql


def test_build_weeks_to_aggregate_placeholder_count() -> None:
    sql = build_weeks_to_aggregate_sql("agg_week_teams_results")
    assert sql.count("%s") == 2


def test_build_weeks_to_aggregate_unknown_table() -> None:
    with pytest.raises(ValueError, match="unknown aggregate table"):
        build_weeks_to_aggregate_sql("agg_week_unknown_results")


def test_agg_table_by_task_covers_all_loaders() -> None:
    assert len(AGG_TABLE_BY_TASK) == 36
    for task_id, agg_table in AGG_TABLE_BY_TASK.items():
        assert task_id.startswith("load_")
        assert agg_table == task_id.removeprefix("load_")


def test_weeks_to_aggregate_sql_default_is_teams_table() -> None:
    assert WEEKS_TO_AGGREGATE_SQL == build_weeks_to_aggregate_sql("agg_week_teams_results")


def test_weeks_to_aggregate_differs_from_finished_weeks() -> None:
    assert WEEKS_TO_AGGREGATE_SQL != FINISHED_AGG_WEEKS_SQL


def test_finished_agg_weeks_alias() -> None:
    assert WEEKS_TO_LOAD_SQL == FINISHED_AGG_WEEKS_SQL
