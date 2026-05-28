"""Unit tests for predict snapshot lookup and transactional prediction writes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np

from include.nascar.ml import predict_task


def test_latest_predict_snapshot_orders_by_snapshot_id_desc() -> None:
    hook = MagicMock()
    hook.get_records.return_value = []

    predict_task._latest_predict_snapshot(hook, rules_id=7)

    sql = hook.get_records.call_args.args[0]
    assert "ORDER BY s.id DESC" in sql
    assert hook.get_records.call_args.kwargs["parameters"] == (
        predict_task.SNAPSHOT_KIND_PREDICT,
        7,
        1,
    )


@patch(
    "include.nascar.ml.predict_task._post_race_predictions_already_present",
    return_value=False,
)
@patch("xgboost.XGBRegressor")
@patch("include.nascar.ml.predict_task._predict_values_for_target")
@patch("include.nascar.ml.predict_task.track_type_ids_for_track")
@patch("include.nascar.ml.predict_task._predict_rows")
@patch("include.nascar.ml.predict_task._latest_predict_snapshot")
@patch("include.nascar.ml.predict_task._active_rules")
@patch("include.nascar.ml.predict_task.NascarMySqlHook")
def test_predict_upcoming_week_uses_run_in_transaction(
    mock_hook_cls: MagicMock,
    mock_rules: MagicMock,
    mock_snapshot: MagicMock,
    mock_rows: MagicMock,
    mock_type_ids: MagicMock,
    mock_blend: MagicMock,
    mock_xgb: MagicMock,
    mock_post_race: MagicMock,
) -> None:
    hook = MagicMock()
    mock_hook_cls.return_value = hook
    mock_rules.return_value = (1, {"components": []})

    mock_snapshot.return_value = {
        "id": 50,
        "series_id": 1,
        "week_id": 100,
        "track_id": 7,
    }
    mock_rows.return_value = [
        {"car_driver_id": 101, "r_start_position": 5, "features": {}},
    ]
    mock_type_ids.return_value = [3, 5]
    blend_meta = {
        "series_id": 1,
        "model_scope": "track_type",
        "track_id": 7,
        "track_type_ids": [3, 5],
        "model_ids": [9, 11],
        "weights": [0.5, 0.5],
        "blend_strategy_used": "equal",
        "blend_strategy_requested": "equal",
        "maes": [None, None],
        "cohort_n_weeks": [4, 6],
        "skipped_track_type_ids": [],
    }
    mock_blend.return_value = (np.array([12.0]), blend_meta, 9)

    booster = MagicMock()
    booster.predict.return_value = np.array([12.0])
    mock_xgb.return_value = booster

    with patch.object(predict_task, "DEFAULT_TARGETS", ("r_finish_position",)):
        result = predict_task.predict_upcoming_week_task()

    hook.run_in_transaction.assert_called_once()
    assert result["skipped"] is None
    assert result["primary_model_id"] == 9
    assert result["track_type_ids"] == [3, 5]


@patch("include.nascar.ml.predict_task._predict_rows")
@patch("include.nascar.ml.predict_task._latest_predict_snapshot")
@patch("include.nascar.ml.predict_task._active_rules")
@patch("include.nascar.ml.predict_task.NascarMySqlHook")
def test_predict_upcoming_week_skips_when_no_track_type_mapping(
    mock_hook_cls: MagicMock,
    mock_rules: MagicMock,
    mock_snapshot: MagicMock,
    mock_rows: MagicMock,
) -> None:
    hook = MagicMock()
    hook.get_records.return_value = []  # track_type_ids_for_track returns empty
    mock_hook_cls.return_value = hook
    mock_rules.return_value = (1, {"components": []})
    mock_snapshot.return_value = {
        "id": 50,
        "series_id": 1,
        "week_id": 100,
        "track_id": 7,
    }
    mock_rows.return_value = [
        {"car_driver_id": 101, "r_start_position": 5, "features": {}},
    ]

    result = predict_task.predict_upcoming_week_task()
    assert result["skipped"] == "no_track_type_mapping"
    assert result["track_id"] == 7


def test_post_race_predictions_already_present_helper() -> None:
    """Helper returns True only when race is finished AND predictions exist."""
    hook = MagicMock()

    hook.get_records.return_value = [(1, 1)]
    assert predict_task._post_race_predictions_already_present(
        hook, week_id=42, snapshot_id=99
    ) is True

    hook.get_records.return_value = [(0, 1)]
    assert predict_task._post_race_predictions_already_present(
        hook, week_id=42, snapshot_id=99
    ) is False

    hook.get_records.return_value = [(1, 0)]
    assert predict_task._post_race_predictions_already_present(
        hook, week_id=42, snapshot_id=99
    ) is False

    hook.get_records.return_value = []
    assert predict_task._post_race_predictions_already_present(
        hook, week_id=42, snapshot_id=99
    ) is False


@patch(
    "include.nascar.ml.predict_task._post_race_predictions_already_present",
    return_value=True,
)
@patch("include.nascar.ml.predict_task._latest_predict_snapshot")
@patch("include.nascar.ml.predict_task._active_rules")
@patch("include.nascar.ml.predict_task.NascarMySqlHook")
def test_predict_upcoming_week_skips_post_race_when_predictions_present(
    mock_hook_cls: MagicMock,
    mock_rules: MagicMock,
    mock_snapshot: MagicMock,
    mock_post_race: MagicMock,
) -> None:
    """Post-race self-healing: skip cleanly when predictions already landed."""
    hook = MagicMock()
    mock_hook_cls.return_value = hook
    mock_rules.return_value = (1, {"components": []})
    mock_snapshot.return_value = {
        "id": 50,
        "series_id": 1,
        "week_id": 100,
        "track_id": 7,
    }

    result = predict_task.predict_upcoming_week_task()

    assert result["skipped"] is True
    assert result["reason"] == "predictions_already_present_post_race"
    assert result["snapshot_id"] == 50
    assert result["week_id"] == 100
    mock_post_race.assert_called_once()
    hook.run_in_transaction.assert_not_called()
