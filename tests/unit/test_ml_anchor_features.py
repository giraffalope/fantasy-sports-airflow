"""Unit tests for pre-lock anchor features in snapshot build."""

from __future__ import annotations

from include.nascar.ml.snapshot_build import (
    _anchor_prelock_features,
    _is_anchor_prelock_column,
)


def test_is_anchor_prelock_column() -> None:
    assert _is_anchor_prelock_column("r_start_position") is True
    assert _is_anchor_prelock_column("q_finish_position") is True
    assert _is_anchor_prelock_column("pt_avg_best_speed") is True
    assert _is_anchor_prelock_column("pf_best_speed") is True
    assert _is_anchor_prelock_column("r_finish_position") is False
    assert _is_anchor_prelock_column("cd_pt_avg_finish") is False


def test_anchor_prelock_features_extracts_columns() -> None:
    anchor = {
        "r_start_position": 5,
        "q_finish_position": 3,
        "pt_avg_best_speed": 180.5,
        "r_finish_position": 10,
    }
    features = _anchor_prelock_features(anchor)
    assert features["r_start_position"] == 5.0
    assert features["q_finish_position"] == 3.0
    assert features["pt_avg_best_speed"] == 180.5
    assert "r_finish_position" not in features
