"""Unit tests for lineup generator skip gates."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from include.nascar.dfs.lineup_generator_task import generate_draftkings_lineups_task


def test_skips_without_predict_snapshot() -> None:
    hook = MagicMock()
    with (
        patch(
            "include.nascar.dfs.lineup_generator_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_predict_snapshot",
            return_value=None,
        ),
    ):
        result = generate_draftkings_lineups_task()
    assert result == {"skipped": True, "reason": "no_predict_snapshot"}


def test_skips_without_ml_predictions() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(0,)]
    with (
        patch(
            "include.nascar.dfs.lineup_generator_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5},
        ),
    ):
        result = generate_draftkings_lineups_task()
    assert result["skipped"] is True
    assert result["reason"] == "no_ml_predictions"
