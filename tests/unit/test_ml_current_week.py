"""Unit tests for ML week selection via v_series_current_week views."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from include.nascar.cup.current_week import (
    _CURRENT_WEEK_TRACK_CONTEXT_SQL,
    _LATEST_TRAIN_WEEK_FOR_RETRAIN_SQL,
    _WEEK_ELIGIBLE_FOR_PREDICT_SQL,
    latest_train_week_for_retrain,
    week_eligible_for_predict_snapshot,
)
from include.nascar.ml.predict_task import _latest_predict_snapshot
from include.nascar.ml.snapshot_build import _current_week_predict_week_id
from include.nascar.ml.snapshot_config import SNAPSHOT_KIND_TRAIN


def test_week_eligible_for_predict_sql_shape() -> None:
    """Gate keeps the pre-race clause AND adds the post-race self-healing branch."""
    assert "r_start_position IS NOT NULL" in _WEEK_ELIGIBLE_FOR_PREDICT_SQL
    assert "r_finish_position IS NULL" in _WEEK_ELIGIBLE_FOR_PREDICT_SQL
    # Self-healing branch references both the predictions and snapshot tables
    # via a NOT EXISTS clause so a missing predictions row re-opens the gate.
    assert "NOT EXISTS" in _WEEK_ELIGIBLE_FOR_PREDICT_SQL
    assert "ml_predictions" in _WEEK_ELIGIBLE_FOR_PREDICT_SQL
    assert "ml_training_snapshots" in _WEEK_ELIGIBLE_FOR_PREDICT_SQL
    assert "kind = 'predict'" in _WEEK_ELIGIBLE_FOR_PREDICT_SQL


def test_week_eligible_for_predict_pre_race() -> None:
    """``EXISTS`` row returned from the hook means the gate is open."""
    hook = MagicMock()
    hook.get_records.return_value = [(1,)]
    assert week_eligible_for_predict_snapshot(hook, week_id=42) is True


def test_week_eligible_for_predict_post_race_with_predictions() -> None:
    """Empty rows from the hook -> race is finished AND predictions exist."""
    hook = MagicMock()
    hook.get_records.return_value = []
    assert week_eligible_for_predict_snapshot(hook, week_id=42) is False


def test_week_eligible_for_predict_post_race_missing_predictions() -> None:
    """Hook returning a row even post-race means we're in the recovery branch."""
    hook = MagicMock()
    hook.get_records.return_value = [(1,)]
    assert week_eligible_for_predict_snapshot(hook, week_id=42) is True
    # Confirm the call shape: single positional ``week_id`` parameter.
    assert hook.get_records.call_args.kwargs["parameters"] == (42,)


def test_current_week_track_context_sql_uses_view() -> None:
    assert "v_series_current_week" in _CURRENT_WEEK_TRACK_CONTEXT_SQL


def test_latest_train_week_for_retrain_sql_uses_both_views() -> None:
    assert "v_series_before_current_week" in _LATEST_TRAIN_WEEK_FOR_RETRAIN_SQL
    assert "v_series_current_week" in _LATEST_TRAIN_WEEK_FOR_RETRAIN_SQL


def test_latest_train_week_for_retrain_returns_int() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(1047,)]
    result = latest_train_week_for_retrain(
        hook, rules_id=1, series_id=1, train_kind=SNAPSHOT_KIND_TRAIN
    )
    assert result == 1047


def test_latest_predict_snapshot_sql_joins_current_week_view() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [
        (10, 1, 1048, 5, '["f1"]'),
    ]
    result = _latest_predict_snapshot(hook, rules_id=1, series_id=1)
    assert result is not None
    assert result["week_id"] == 1048
    assert result["track_id"] == 5
    assert "track_type_id" not in result
    sql = hook.get_records.call_args[0][0]
    assert "v_series_current_week" in sql
    assert "ORDER BY s.week_id DESC" not in sql


def test_current_week_predict_week_id_none_without_current() -> None:
    hook = MagicMock()
    with patch(
        "include.nascar.cup.current_week.resolve_series_current_week",
        return_value=None,
    ):
        assert _current_week_predict_week_id(hook, series_id=1) is None
