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


@patch("xgboost.XGBRegressor")
@patch("include.nascar.ml.predict_task._resolve_model_for_target")
@patch("include.nascar.ml.predict_task._predict_rows")
@patch("include.nascar.ml.predict_task._latest_predict_snapshot")
@patch("include.nascar.ml.predict_task._active_rules")
@patch("include.nascar.ml.predict_task.NascarMySqlHook")
def test_predict_upcoming_week_uses_run_in_transaction(
    mock_hook_cls: MagicMock,
    mock_rules: MagicMock,
    mock_snapshot: MagicMock,
    mock_rows: MagicMock,
    mock_resolve: MagicMock,
    mock_xgb: MagicMock,
) -> None:
    hook = MagicMock()
    mock_hook_cls.return_value = hook
    mock_rules.return_value = (1, {"components": []})

    mock_snapshot.return_value = {
        "id": 50,
        "series_id": 1,
        "week_id": 100,
        "track_id": 7,
        "track_type_id": 3,
    }
    mock_rows.return_value = [
        {"car_driver_id": 101, "r_start_position": 5, "features": {}},
    ]
    mock_resolve.return_value = {
        "id": 9,
        "artifact_uri": "/tmp/model.json",
        "selected_features": ["f1"],
        "series_id": 1,
        "model_scope": "global",
        "track_id": None,
        "track_type_id": None,
    }

    booster = MagicMock()
    booster.predict.return_value = np.array([12.0])
    mock_xgb.return_value = booster

    with patch.object(predict_task, "DEFAULT_TARGETS", ("r_finish_position",)):
        predict_task.predict_upcoming_week_task()

    hook.run_in_transaction.assert_called_once()
