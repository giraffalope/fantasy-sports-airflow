"""Unit tests for :mod:`include.nascar.ml.model_scope`.

Covers the cohort-design feature allowlist (strict ``*_pt_*`` + anchors, no
``*_tt_*``), grouping helpers, and the env-var-backed cohort knobs
(``FS_ML_MIN_COHORT_WEEKS`` and ``FS_ML_BLEND_WEIGHTS``).
"""

from __future__ import annotations

from unittest.mock import MagicMock

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
    # C1/C2/C4: new feature families must pass through ``features_for_scope``
    "df_last5_avg_finish",
    "df_ytd_top5_rate",
    "qf_pole_gap_seconds",
    "qf_field_strength_ytd_avg_finish",
    "season_week_number",
    "season_is_playoffs",
    "track_attr_length_miles",
]


def test_features_for_scope_track_type_drops_all_tt_keys_without_cohort() -> None:
    """Legacy behavior (no ``track_type_id`` passed): every ``_tt_`` key is dropped."""
    selected = ms.features_for_scope(_FEATURE_KEYS, scope=ms.SCOPE_TRACK_TYPE)
    assert "cd_pt_finish_avg" in selected
    assert "drv_pt_top10_count" in selected
    assert "r_start_position" in selected
    assert "manufacturer_id" in selected
    assert all("_tt_" not in key for key in selected)


def test_features_for_scope_with_cohort_strips_only_cohort_type() -> None:
    """C3: ``track_type_id=3`` keeps ``_tt_5_`` keys, strips ``_tt_3_`` keys."""
    selected = ms.features_for_scope(
        _FEATURE_KEYS,
        scope=ms.SCOPE_TRACK_TYPE,
        track_type_id=3,
    )
    assert "cd_tt_3_finish_avg" not in selected
    assert "drv_tt_3_top10_count" not in selected
    assert "cd_tt_5_finish_avg" in selected
    assert "cd_pt_finish_avg" in selected
    assert "r_start_position" in selected


def test_features_for_scope_with_cohort_does_not_match_id_prefix() -> None:
    """``track_type_id=3`` must not strip ``_tt_30_`` (digit-boundary safety)."""
    keys = ["cd_tt_3_finish", "cd_tt_30_finish", "cd_tt_31_finish"]
    selected = ms.features_for_scope(keys, scope=ms.SCOPE_TRACK_TYPE, track_type_id=3)
    assert "cd_tt_3_finish" not in selected
    assert "cd_tt_30_finish" in selected
    assert "cd_tt_31_finish" in selected


def test_features_for_scope_keeps_new_feature_families() -> None:
    """C5: ``df_*``, ``qf_*``, ``season_*``, ``track_attr_*`` flow through."""
    selected = ms.features_for_scope(
        _FEATURE_KEYS,
        scope=ms.SCOPE_TRACK_TYPE,
        track_type_id=3,
    )
    for key in (
        "df_last5_avg_finish",
        "df_ytd_top5_rate",
        "qf_pole_gap_seconds",
        "qf_field_strength_ytd_avg_finish",
        "season_week_number",
        "season_is_playoffs",
        "track_attr_length_miles",
    ):
        assert key in selected, f"expected {key!r} to survive features_for_scope"


def test_features_for_scope_rejects_unknown_scope() -> None:
    with pytest.raises(ValueError):
        ms.features_for_scope(_FEATURE_KEYS, scope="bogus")


def test_features_for_scope_rejects_legacy_scopes() -> None:
    for legacy in (ms.LEGACY_SCOPE_TRACK, ms.LEGACY_SCOPE_GLOBAL):
        with pytest.raises(ValueError):
            ms.features_for_scope(_FEATURE_KEYS, scope=legacy)


def test_group_frame_by_series_dedupes() -> None:
    frame = [
        {"series_id": 1, "week_id": 1},
        {"series_id": 1, "week_id": 2},
        {"series_id": 2, "week_id": 3},
        {"series_id": None, "week_id": 4},
        {"series_id": "1", "week_id": 5},
    ]
    by_series = ms.group_frame_by_series(frame)
    assert set(by_series.keys()) == {1, 2}
    assert len(by_series[1]) == 3
    assert len(by_series[2]) == 1


def test_group_frame_by_track_type_dedupes() -> None:
    frame = [
        {"track_type_id": 3, "week_id": 1},
        {"track_type_id": 3, "week_id": 2},
        {"track_type_id": 5, "week_id": 3},
        {"track_type_id": None, "week_id": 4},
    ]
    grouped = ms.group_frame_by_track_type(frame)
    assert set(grouped.keys()) == {3, 5}
    assert len(grouped[3]) == 2


def test_distinct_week_count_dedupes() -> None:
    frame = [
        {"week_id": 1},
        {"week_id": 1},
        {"week_id": 2},
        {"week_id": 3},
    ]
    assert ms.distinct_week_count(frame) == 3


def test_min_cohort_weeks_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FS_ML_MIN_COHORT_WEEKS", raising=False)
    assert ms.min_cohort_weeks() == 2
    monkeypatch.setenv("FS_ML_MIN_COHORT_WEEKS", "6")
    assert ms.min_cohort_weeks() == 6
    monkeypatch.setenv("FS_ML_MIN_COHORT_WEEKS", "not-an-int")
    assert ms.min_cohort_weeks() == 2


def test_blend_weight_strategy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FS_ML_BLEND_WEIGHTS", raising=False)
    assert ms.blend_weight_strategy() == "inverse_mae"
    monkeypatch.setenv("FS_ML_BLEND_WEIGHTS", "sample_count")
    assert ms.blend_weight_strategy() == "sample_count"
    monkeypatch.setenv("FS_ML_BLEND_WEIGHTS", "equal")
    assert ms.blend_weight_strategy() == "equal"
    monkeypatch.setenv("FS_ML_BLEND_WEIGHTS", "  Inverse_MAE  ")
    assert ms.blend_weight_strategy() == "inverse_mae"
    monkeypatch.setenv("FS_ML_BLEND_WEIGHTS", "nonsense")
    assert ms.blend_weight_strategy() == "inverse_mae"


def test_track_type_ids_for_track_orders_ascending() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(5,), (3,)]
    assert ms.track_type_ids_for_track(hook, 99) == [5, 3]
    args, kwargs = hook.get_records.call_args
    assert "tracks_track_types" in args[0]
    assert kwargs["parameters"] == (99,)


def test_sibling_track_ids_for_type_returns_ints() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(10,), (12,), (20,)]
    siblings = ms.sibling_track_ids_for_type(
        hook, series_id=1, rules_id=7, track_type_id=3
    )
    assert siblings == [10, 12, 20]
    args, kwargs = hook.get_records.call_args
    assert "tracks_track_types" in args[0]
    assert "ml_training_snapshots" in args[0]
    assert kwargs["parameters"] == (1, 7, 3)


def test_cohort_distinct_weeks_zero_when_empty() -> None:
    hook = MagicMock()
    hook.get_records.return_value = []
    assert (
        ms.cohort_distinct_weeks(hook, series_id=1, rules_id=7, track_type_id=3)
        == 0
    )

    hook.get_records.return_value = [(None,)]
    assert (
        ms.cohort_distinct_weeks(hook, series_id=1, rules_id=7, track_type_id=3)
        == 0
    )

    hook.get_records.return_value = [(8,)]
    assert (
        ms.cohort_distinct_weeks(hook, series_id=1, rules_id=7, track_type_id=3)
        == 8
    )


def test_all_scopes_only_contains_track_type() -> None:
    assert ms.ALL_SCOPES == (ms.SCOPE_TRACK_TYPE,)
