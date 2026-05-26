"""Unit tests for the predict-time cohort blend.

Replaces the old track / track_type / global routing chain with assertions
that ``_predict_values_for_target`` blends per-cohort models for the current
track's ``track_type_id`` set per ``FS_ML_BLEND_WEIGHTS``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest

from include.nascar.ml import predict_task
from include.nascar.ml.model_scope import SCOPE_TRACK_TYPE

_CUP_SERIES_ID = 1


def _make_hook(
    catalog: dict[tuple[str, int, int], dict[str, Any] | None],
    *,
    holdout_mae: dict[int, float] | None = None,
) -> MagicMock:
    """Build a hook whose ``get_records`` answers ``_scope_lookup`` lookups.

    ``catalog`` is keyed by ``(target, series_id, track_type_id)``.
    """
    holdout_mae = holdout_mae or {}
    hook = MagicMock()

    def get_records(sql: str, parameters: tuple = ()) -> list[tuple]:
        if "ml_model_evaluations" in sql:
            model_id = int(parameters[0])
            mae = holdout_mae.get(model_id)
            return [] if mae is None else [(mae,)]
        # _scope_lookup
        series_id = int(parameters[0])
        target = parameters[3]
        track_type_id = int(parameters[2])
        entry = catalog.get((target, series_id, track_type_id))
        if entry is None:
            return []
        return [
            (
                entry["id"],
                entry["artifact_uri"],
                "{}",
                entry.get("series_id", series_id),
                SCOPE_TRACK_TYPE,
                None,
                entry.get("track_type_id", track_type_id),
                entry.get("cohort_n_weeks", 0),
                "[]",
            )
        ]

    hook.get_records.side_effect = get_records
    return hook


def _patch_predict(monkeypatch: pytest.MonkeyPatch, predictions_by_model: dict[int, np.ndarray]) -> None:
    def _fake_predict(
        _rows: list[dict[str, Any]],
        entry: dict[str, Any],
        *,
        booster_cls: type,
    ) -> np.ndarray:
        return predictions_by_model[entry["id"]]

    monkeypatch.setattr(predict_task, "_predict_with_model", _fake_predict)


def test_multi_type_track_blends_with_equal_weights(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_BLEND_WEIGHTS", "equal")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, 3): {
                "id": 22,
                "artifact_uri": "/tmp/c3.json",
                "track_type_id": 3,
                "cohort_n_weeks": 4,
            },
            ("r_finish_position", _CUP_SERIES_ID, 5): {
                "id": 25,
                "artifact_uri": "/tmp/c5.json",
                "track_type_id": 5,
                "cohort_n_weeks": 6,
            },
        }
    )
    rows = [{"car_driver_id": 1, "features": {}}, {"car_driver_id": 2, "features": {}}]
    _patch_predict(
        monkeypatch,
        {22: np.array([10.0, 12.0]), 25: np.array([20.0, 24.0])},
    )

    preds, meta, model_id = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        rows=rows,
        booster_cls=MagicMock,
        type_ids=[3, 5],
        strategy="equal",
    )
    assert preds is not None
    np.testing.assert_allclose(preds, [15.0, 18.0])
    assert meta is not None
    assert meta["model_scope"] == SCOPE_TRACK_TYPE
    assert meta["track_type_ids"] == [3, 5]
    assert meta["model_ids"] == [22, 25]
    assert meta["weights"] == [0.5, 0.5]
    assert meta["blend_strategy_used"] == "equal"
    assert model_id == 22


def test_inverse_mae_strategy_weights_lower_mae_higher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_BLEND_WEIGHTS", "inverse_mae")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, 3): {
                "id": 22,
                "artifact_uri": "/tmp/c3.json",
                "track_type_id": 3,
                "cohort_n_weeks": 4,
            },
            ("r_finish_position", _CUP_SERIES_ID, 5): {
                "id": 25,
                "artifact_uri": "/tmp/c5.json",
                "track_type_id": 5,
                "cohort_n_weeks": 6,
            },
        },
        holdout_mae={22: 2.0, 25: 1.0},
    )
    rows = [{"car_driver_id": 1, "features": {}}]
    _patch_predict(monkeypatch, {22: np.array([10.0]), 25: np.array([20.0])})

    preds, meta, _ = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        rows=rows,
        booster_cls=MagicMock,
        type_ids=[3, 5],
        strategy="inverse_mae",
    )
    assert preds is not None
    # weights = (1/2, 1/1)/(1/2+1/1) = (1/3, 2/3); blend = 10*1/3 + 20*2/3
    np.testing.assert_allclose(preds, [10.0 * (1 / 3) + 20.0 * (2 / 3)])
    assert meta is not None
    np.testing.assert_allclose(meta["weights"], [1 / 3, 2 / 3], atol=1e-6)
    assert meta["maes"] == [2.0, 1.0]
    assert meta["blend_strategy_used"] == "inverse_mae"


def test_inverse_mae_missing_falls_back_to_equal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_BLEND_WEIGHTS", "inverse_mae")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, 3): {
                "id": 22,
                "artifact_uri": "/tmp/c3.json",
                "track_type_id": 3,
            },
            ("r_finish_position", _CUP_SERIES_ID, 5): {
                "id": 25,
                "artifact_uri": "/tmp/c5.json",
                "track_type_id": 5,
            },
        },
        holdout_mae={22: 2.0},  # 25 missing
    )
    rows = [{"car_driver_id": 1, "features": {}}]
    _patch_predict(monkeypatch, {22: np.array([10.0]), 25: np.array([20.0])})

    preds, meta, _ = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        rows=rows,
        booster_cls=MagicMock,
        type_ids=[3, 5],
        strategy="inverse_mae",
    )
    assert preds is not None
    np.testing.assert_allclose(preds, [15.0])
    assert meta is not None
    np.testing.assert_allclose(meta["weights"], [0.5, 0.5])
    assert meta["blend_strategy_used"] == "equal_fallback"


def test_sample_count_strategy_weights_by_cohort_n_weeks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, 3): {
                "id": 22,
                "artifact_uri": "/tmp/c3.json",
                "track_type_id": 3,
                "cohort_n_weeks": 4,
            },
            ("r_finish_position", _CUP_SERIES_ID, 5): {
                "id": 25,
                "artifact_uri": "/tmp/c5.json",
                "track_type_id": 5,
                "cohort_n_weeks": 12,
            },
        }
    )
    rows = [{"car_driver_id": 1, "features": {}}]
    _patch_predict(monkeypatch, {22: np.array([10.0]), 25: np.array([20.0])})

    preds, meta, _ = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        rows=rows,
        booster_cls=MagicMock,
        type_ids=[3, 5],
        strategy="sample_count",
    )
    assert preds is not None
    # weights = (4, 12)/16 = (0.25, 0.75)
    np.testing.assert_allclose(preds, [10.0 * 0.25 + 20.0 * 0.75])
    assert meta is not None
    np.testing.assert_allclose(meta["weights"], [0.25, 0.75])
    assert meta["blend_strategy_used"] == "sample_count"
    assert meta["cohort_n_weeks"] == [4, 12]


def test_single_type_track_passes_predictions_through_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, 3): {
                "id": 22,
                "artifact_uri": "/tmp/c3.json",
                "track_type_id": 3,
                "cohort_n_weeks": 5,
            },
        },
        holdout_mae={22: 1.5},
    )
    rows = [{"car_driver_id": 1, "features": {}}]
    _patch_predict(monkeypatch, {22: np.array([7.5])})

    preds, meta, model_id = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        rows=rows,
        booster_cls=MagicMock,
        type_ids=[3],
        strategy="inverse_mae",
    )
    assert preds is not None
    np.testing.assert_allclose(preds, [7.5])
    assert meta is not None
    assert meta["track_type_ids"] == [3]
    assert meta["weights"] == [1.0]
    assert model_id == 22


def test_no_cohort_models_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    hook = _make_hook({})
    _patch_predict(monkeypatch, {})

    preds, meta, model_id = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        rows=[{"car_driver_id": 1, "features": {}}],
        booster_cls=MagicMock,
        type_ids=[3, 5],
        strategy="inverse_mae",
    )
    assert preds is None
    assert meta is None
    assert model_id is None


def test_no_type_ids_short_circuits(monkeypatch: pytest.MonkeyPatch) -> None:
    hook = _make_hook({})
    preds, meta, model_id = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        rows=[{"car_driver_id": 1, "features": {}}],
        booster_cls=MagicMock,
        type_ids=[],
        strategy="equal",
    )
    assert preds is None
    assert meta is None
    assert model_id is None


class TestCohortTrainTargetMeanFallback:
    """B3: secondaries without a cohort model fall back to ``ml_models.train_target_mean``.

    The fallback chain is ``type-scoped average -> series-wide average -> None``.
    These tests stub ``hook.get_records`` to return realistic shapes for each
    branch.
    """

    def test_returns_type_scoped_average_when_present(self) -> None:
        hook = MagicMock()
        hook.get_records.return_value = [(4.5,)]

        mean = predict_task._cohort_train_target_mean(
            hook,
            target="r_total_laps_led",
            series_id=1,
            track_type_ids=[3, 5],
        )

        assert mean == 4.5
        sql, kwargs = hook.get_records.call_args.args, hook.get_records.call_args.kwargs
        # Type-scoped query gets called first; its IN list must include both type_ids.
        assert "track_type_id IN" in sql[0]
        params = kwargs["parameters"]
        # First call exhausts the type-scoped branch — no fall-through needed.
        assert hook.get_records.call_count == 1
        assert 3 in params and 5 in params

    def test_falls_through_to_series_wide_when_type_scoped_empty(self) -> None:
        hook = MagicMock()
        # First call (type-scoped) -> empty; second call (series-wide) -> 3.2.
        hook.get_records.side_effect = [[(None,)], [(3.2,)]]

        mean = predict_task._cohort_train_target_mean(
            hook,
            target="r_total_laps_led",
            series_id=1,
            track_type_ids=[3, 5],
        )

        assert mean == 3.2
        assert hook.get_records.call_count == 2

    def test_returns_none_when_no_cohort_models_have_train_target_mean(self) -> None:
        hook = MagicMock()
        hook.get_records.side_effect = [[(None,)], [(None,)]]

        mean = predict_task._cohort_train_target_mean(
            hook,
            target="r_total_laps_led",
            series_id=1,
            track_type_ids=[3, 5],
        )

        assert mean is None
        assert hook.get_records.call_count == 2

    def test_track_type_ids_optional_uses_series_wide_only(self) -> None:
        hook = MagicMock()
        hook.get_records.return_value = [(2.7,)]

        mean = predict_task._cohort_train_target_mean(
            hook,
            target="r_total_laps_led",
            series_id=1,
            track_type_ids=None,
        )

        assert mean == 2.7
        # No type-scoped query — only the series-wide one.
        assert hook.get_records.call_count == 1
        assert "track_type_id IN" not in hook.get_records.call_args.args[0]


def test_models_for_unrelated_series_are_invisible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cup cohort artifacts must not resolve when the snapshot series is Xfinity."""
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, 3): {
                "id": 22,
                "artifact_uri": "/tmp/cup.json",
                "track_type_id": 3,
            },
        }
    )
    _patch_predict(monkeypatch, {})

    preds, meta, model_id = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=2,  # Xfinity
        track_id=99,
        rows=[{"car_driver_id": 1, "features": {}}],
        booster_cls=MagicMock,
        type_ids=[3],
        strategy="equal",
    )
    assert preds is None
    assert meta is None
    assert model_id is None
