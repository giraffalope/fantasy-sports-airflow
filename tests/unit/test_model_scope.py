"""Unit tests for :mod:`include.nascar.ml.model_scope`.

Covers the feature allowlist filter, the per-track / per-track-type grouping
helpers, and the env-var-backed thresholds + global-fallback flag.
"""

from __future__ import annotations

import pytest

from include.nascar.ml import model_scope as ms

_FEATURE_KEYS = [
    "r_start_position",
    "cd_pt_finish_avg",
    "drv_pt_top10_count",
    "cd_tt_3_finish_avg",
    "cd_tt_5_finish_avg",
    "drv_tt_3_top10_count",
    "manufacturer_id",
]


def test_features_for_scope_global_returns_everything() -> None:
    assert ms.features_for_scope(_FEATURE_KEYS, scope=ms.SCOPE_GLOBAL) == _FEATURE_KEYS


def test_features_for_scope_track_keeps_pt_and_anchor() -> None:
    selected = ms.features_for_scope(_FEATURE_KEYS, scope=ms.SCOPE_TRACK)
    assert "cd_pt_finish_avg" in selected
    assert "drv_pt_top10_count" in selected
    assert "r_start_position" in selected
    assert "manufacturer_id" in selected
    assert "cd_tt_3_finish_avg" not in selected
    assert "drv_tt_3_top10_count" not in selected


def test_features_for_scope_track_type_filters_to_matching_type() -> None:
    selected = ms.features_for_scope(
        _FEATURE_KEYS,
        scope=ms.SCOPE_TRACK_TYPE,
        track_type_id=3,
    )
    assert "cd_tt_3_finish_avg" in selected
    assert "drv_tt_3_top10_count" in selected
    assert "cd_tt_5_finish_avg" not in selected
    assert "r_start_position" in selected
    assert "cd_pt_finish_avg" not in selected


def test_features_for_scope_track_type_without_id_drops_all_tt() -> None:
    selected = ms.features_for_scope(_FEATURE_KEYS, scope=ms.SCOPE_TRACK_TYPE)
    assert all("_tt_" not in key for key in selected)
    assert "r_start_position" in selected


def test_features_for_scope_rejects_unknown_scope() -> None:
    with pytest.raises(ValueError):
        ms.features_for_scope(_FEATURE_KEYS, scope="bogus")


def test_group_frame_by_track_and_type() -> None:
    frame = [
        {"track_id": 1, "track_type_id": 3, "week_id": 100},
        {"track_id": 1, "track_type_id": 3, "week_id": 101},
        {"track_id": 2, "track_type_id": 5, "week_id": 102},
        {"track_id": None, "track_type_id": 5, "week_id": 103},
    ]
    by_track = ms.group_frame_by_track(frame)
    assert set(by_track.keys()) == {1, 2}
    assert len(by_track[1]) == 2
    assert len(by_track[2]) == 1

    by_type = ms.group_frame_by_track_type(frame)
    assert set(by_type.keys()) == {3, 5}
    assert len(by_type[5]) == 2


def test_group_frame_by_series_dedupes() -> None:
    frame = [
        {"series_id": 1, "week_id": 1},
        {"series_id": 1, "week_id": 2},
        {"series_id": 2, "week_id": 3},
        {"series_id": None, "week_id": 4},
        {"series_id": "1", "week_id": 5},  # ensure integer coercion
    ]
    by_series = ms.group_frame_by_series(frame)
    assert set(by_series.keys()) == {1, 2}
    assert len(by_series[1]) == 3
    assert len(by_series[2]) == 1


def test_distinct_week_count_dedupes() -> None:
    frame = [
        {"week_id": 1},
        {"week_id": 1},
        {"week_id": 2},
        {"week_id": 3},
    ]
    assert ms.distinct_week_count(frame) == 3


def test_thresholds_and_global_fallback_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FS_ML_MIN_TRACK_WEEKS", raising=False)
    monkeypatch.delenv("FS_ML_MIN_TYPE_WEEKS", raising=False)
    monkeypatch.delenv("FS_ML_ENABLE_GLOBAL_FALLBACK", raising=False)
    monkeypatch.delenv("FS_ML_MAX_TRAIN_WEEKS", raising=False)
    assert ms.min_track_weeks() == 3
    assert ms.min_track_type_weeks() == 3
    assert ms.global_fallback_enabled() is True
    assert ms.max_train_weeks() == 0

    monkeypatch.setenv("FS_ML_MIN_TRACK_WEEKS", "5")
    monkeypatch.setenv("FS_ML_MIN_TYPE_WEEKS", "15")
    monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "0")
    monkeypatch.setenv("FS_ML_MAX_TRAIN_WEEKS", "156")
    assert ms.min_track_weeks() == 5
    assert ms.min_track_type_weeks() == 15
    assert ms.global_fallback_enabled() is False
    assert ms.max_train_weeks() == 156
