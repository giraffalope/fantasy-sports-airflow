"""Unit tests for the predict-time scope routing chain.

Exercises ``_resolve_model_for_target`` against a fake hook so we can confirm
that track -> track_type -> global is honored, that the global fallback flag
is respected, that mis-typed snapshots fall through correctly, and that
``series_id`` isolation prevents cross-series leakage.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest

from include.nascar.ml import predict_task
from include.nascar.ml.model_scope import (
    SCOPE_GLOBAL,
    SCOPE_TRACK,
    SCOPE_TRACK_TYPE,
    SCOPE_TRACK_TYPE_BLEND,
)

# Default Cup series id used by most fixtures.
_CUP_SERIES_ID = 1


def _make_hook(
    catalog: dict[tuple[str, int, str, Any], dict[str, Any] | None],
) -> MagicMock:
    """Build a hook whose ``get_records`` returns rows shaped like ``_scope_lookup``.

    ``catalog`` is keyed by ``(target, series_id, scope, scope_key)`` where
    ``scope_key`` is ``track_id`` for track scope, ``track_type_id`` for
    track_type, and ``None`` for global. Values are the model dict the lookup
    should return (or ``None`` to simulate a miss).
    """

    hook = MagicMock()

    def get_records(sql: str, parameters: tuple = ()) -> list[tuple]:
        series_id = int(parameters[0])
        target = parameters[1]
        scope = parameters[2]
        if scope in (SCOPE_TRACK, SCOPE_TRACK_TYPE):
            key: Any = parameters[3]
        else:
            key = None
        entry = catalog.get((target, series_id, scope, key))
        if entry is None:
            return []
        return [
            (
                entry["id"],
                entry["artifact_uri"],
                "{}",
                entry.get("series_id", series_id),
                entry["model_scope"],
                entry.get("track_id"),
                entry.get("track_type_id"),
                "[]",
            )
        ]

    hook.get_records.side_effect = get_records
    return hook


def test_resolver_prefers_track_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK, 7): {
                "id": 11,
                "artifact_uri": "/tmp/track.json",
                "model_scope": SCOPE_TRACK,
                "track_id": 7,
            },
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK_TYPE, 3): {
                "id": 22,
                "artifact_uri": "/tmp/type.json",
                "model_scope": SCOPE_TRACK_TYPE,
                "track_type_id": 3,
            },
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_GLOBAL, None): {
                "id": 33,
                "artifact_uri": "/tmp/global.json",
                "model_scope": SCOPE_GLOBAL,
            },
        }
    )
    resolved = predict_task._resolve_model_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=7,
        track_type_id=3,
    )
    assert resolved is not None
    assert resolved["id"] == 11
    assert resolved["model_scope"] == SCOPE_TRACK
    assert resolved["series_id"] == _CUP_SERIES_ID


def test_resolver_falls_back_to_track_type(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK_TYPE, 3): {
                "id": 22,
                "artifact_uri": "/tmp/type.json",
                "model_scope": SCOPE_TRACK_TYPE,
                "track_type_id": 3,
            },
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_GLOBAL, None): {
                "id": 33,
                "artifact_uri": "/tmp/global.json",
                "model_scope": SCOPE_GLOBAL,
            },
        }
    )
    resolved = predict_task._resolve_model_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        track_type_id=3,
    )
    assert resolved is not None
    assert resolved["id"] == 22
    assert resolved["model_scope"] == SCOPE_TRACK_TYPE


def test_resolver_falls_back_to_global_when_flag_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_GLOBAL, None): {
                "id": 33,
                "artifact_uri": "/tmp/global.json",
                "model_scope": SCOPE_GLOBAL,
            },
        }
    )
    resolved = predict_task._resolve_model_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        track_type_id=42,
    )
    assert resolved is not None
    assert resolved["id"] == 33
    assert resolved["model_scope"] == SCOPE_GLOBAL


def test_resolver_returns_none_when_global_fallback_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "0")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_GLOBAL, None): {
                "id": 33,
                "artifact_uri": "/tmp/global.json",
                "model_scope": SCOPE_GLOBAL,
            },
        }
    )
    resolved = predict_task._resolve_model_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        track_type_id=42,
    )
    assert resolved is None


def test_resolver_skips_track_when_artifact_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A registry row with NULL artifact_uri should not be considered usable."""
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK, 7): {
                "id": 11,
                "artifact_uri": None,
                "model_scope": SCOPE_TRACK,
                "track_id": 7,
            },
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK_TYPE, 3): {
                "id": 22,
                "artifact_uri": "/tmp/type.json",
                "model_scope": SCOPE_TRACK_TYPE,
                "track_type_id": 3,
            },
        }
    )
    resolved = predict_task._resolve_model_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=7,
        track_type_id=3,
    )
    assert resolved is not None
    assert resolved["id"] == 22


def test_resolver_handles_missing_track_type_id_on_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unmapped track => skip track_type lookup, drop straight to global."""
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_GLOBAL, None): {
                "id": 33,
                "artifact_uri": "/tmp/global.json",
                "model_scope": SCOPE_GLOBAL,
            },
        }
    )
    resolved = predict_task._resolve_model_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=None,
        track_type_id=None,
    )
    assert resolved is not None
    assert resolved["model_scope"] == SCOPE_GLOBAL


def test_resolver_does_not_cross_series_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Cup track model must never resolve for an Xfinity predict snapshot.

    The catalog only has Cup (series=1) entries; an Xfinity (series=2)
    lookup at the same track_id / track_type_id must return ``None`` because
    the routing chain filters on ``series_id`` first.
    """
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    cup_only = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK, 7): {
                "id": 11,
                "artifact_uri": "/tmp/cup_track.json",
                "model_scope": SCOPE_TRACK,
                "track_id": 7,
            },
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK_TYPE, 3): {
                "id": 22,
                "artifact_uri": "/tmp/cup_type.json",
                "model_scope": SCOPE_TRACK_TYPE,
                "track_type_id": 3,
            },
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_GLOBAL, None): {
                "id": 33,
                "artifact_uri": "/tmp/cup_global.json",
                "model_scope": SCOPE_GLOBAL,
            },
        }
    )
    resolved = predict_task._resolve_model_for_target(
        cup_only,
        target="r_finish_position",
        series_id=2,  # Xfinity
        track_id=7,
        track_type_id=3,
    )
    assert resolved is None


def test_resolver_uses_series_specific_model_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When both Cup and Xfinity track models exist, route to the right one."""
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    hook = _make_hook(
        {
            ("r_finish_position", 1, SCOPE_TRACK, 7): {
                "id": 11,
                "artifact_uri": "/tmp/cup_track.json",
                "model_scope": SCOPE_TRACK,
                "series_id": 1,
                "track_id": 7,
            },
            ("r_finish_position", 2, SCOPE_TRACK, 7): {
                "id": 211,
                "artifact_uri": "/tmp/xfinity_track.json",
                "model_scope": SCOPE_TRACK,
                "series_id": 2,
                "track_id": 7,
            },
        }
    )

    cup = predict_task._resolve_model_for_target(
        hook,
        target="r_finish_position",
        series_id=1,
        track_id=7,
        track_type_id=None,
    )
    assert cup is not None
    assert cup["id"] == 11
    assert cup["series_id"] == 1

    xfinity = predict_task._resolve_model_for_target(
        hook,
        target="r_finish_position",
        series_id=2,
        track_id=7,
        track_type_id=None,
    )
    assert xfinity is not None
    assert xfinity["id"] == 211
    assert xfinity["series_id"] == 2


def test_cold_track_blend_averages_type_model_predictions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK_TYPE, 3): {
                "id": 22,
                "artifact_uri": "/tmp/type3.json",
                "model_scope": SCOPE_TRACK_TYPE,
                "track_type_id": 3,
            },
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK_TYPE, 5): {
                "id": 25,
                "artifact_uri": "/tmp/type5.json",
                "model_scope": SCOPE_TRACK_TYPE,
                "track_type_id": 5,
            },
        }
    )
    rows = [
        {"car_driver_id": 1, "features": {"a": 1.0}},
        {"car_driver_id": 2, "features": {"a": 2.0}},
    ]

    monkeypatch.setattr(predict_task, "is_cold_track", lambda *a, **k: True)
    monkeypatch.setattr(predict_task, "track_type_ids_for_track", lambda _h, _t: [3, 5])

    def _fake_predict(
        _rows: list[dict[str, Any]],
        entry: dict[str, Any],
        *,
        booster_cls: type,
    ) -> np.ndarray:
        if entry["id"] == 22:
            return np.array([10.0, 12.0])
        return np.array([20.0, 24.0])

    monkeypatch.setattr(predict_task, "_predict_with_model", _fake_predict)

    preds, meta, model_id = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        track_type_id=3,
        rules_id=7,
        rows=rows,
        booster_cls=MagicMock,
    )
    assert preds is not None
    np.testing.assert_allclose(preds, [15.0, 18.0])
    assert meta is not None
    assert meta["model_scope"] == SCOPE_TRACK_TYPE_BLEND
    assert meta["track_type_ids"] == [3, 5]
    assert meta["n_models"] == 2
    assert model_id == 22


def test_warm_track_without_track_model_uses_single_track_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK_TYPE, 3): {
                "id": 22,
                "artifact_uri": "/tmp/type.json",
                "model_scope": SCOPE_TRACK_TYPE,
                "track_type_id": 3,
            },
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_TRACK_TYPE, 5): {
                "id": 25,
                "artifact_uri": "/tmp/type5.json",
                "model_scope": SCOPE_TRACK_TYPE,
                "track_type_id": 5,
            },
        }
    )
    rows = [{"car_driver_id": 1, "features": {}}]

    monkeypatch.setattr(predict_task, "is_cold_track", lambda *a, **k: False)
    monkeypatch.setattr(
        predict_task,
        "_predict_with_model",
        lambda _rows, _entry, booster_cls: np.array([7.0]),
    )

    preds, meta, model_id = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        track_type_id=3,
        rules_id=7,
        rows=rows,
        booster_cls=MagicMock,
    )
    assert preds is not None
    assert float(preds[0]) == 7.0
    assert meta is not None
    assert meta["model_scope"] == SCOPE_TRACK_TYPE
    assert meta["track_type_id"] == 3
    assert model_id == 22


def test_cold_track_blend_falls_back_to_global_when_no_type_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "1")
    hook = _make_hook(
        {
            ("r_finish_position", _CUP_SERIES_ID, SCOPE_GLOBAL, None): {
                "id": 33,
                "artifact_uri": "/tmp/global.json",
                "model_scope": SCOPE_GLOBAL,
            },
        }
    )
    rows = [{"car_driver_id": 1, "features": {}}]

    monkeypatch.setattr(predict_task, "is_cold_track", lambda *a, **k: True)
    monkeypatch.setattr(predict_task, "track_type_ids_for_track", lambda _h, _t: [3, 5])
    monkeypatch.setattr(
        predict_task,
        "_predict_with_model",
        lambda _rows, _entry, booster_cls: np.array([42.0]),
    )

    preds, meta, model_id = predict_task._predict_values_for_target(
        hook,
        target="r_finish_position",
        series_id=_CUP_SERIES_ID,
        track_id=99,
        track_type_id=3,
        rules_id=7,
        rows=rows,
        booster_cls=MagicMock,
    )
    assert preds is not None
    assert float(preds[0]) == 42.0
    assert meta is not None
    assert meta["model_scope"] == SCOPE_GLOBAL
    assert model_id == 33
