"""Unit tests for the C2 ``qf_*`` qualifying / field-strength feature family."""

from __future__ import annotations

import math
from typing import Any

from include.nascar.ml.field_strength import (
    FIELD_STRENGTH_FEATURE_KEYS,
    compute_field_strength_features,
)


def _anchor(
    *,
    q_best_time: float | None,
    r_start_position: int | None,
    df_ytd_avg_finish: float | None = None,
    df_ytd_top5_rate: float | None = None,
) -> dict[str, Any]:
    return {
        "q_best_time": q_best_time,
        "r_start_position": r_start_position,
        "df_ytd_avg_finish": df_ytd_avg_finish,
        "df_ytd_top5_rate": df_ytd_top5_rate,
    }


def test_field_strength_emits_every_key_per_row() -> None:
    anchors = [
        _anchor(q_best_time=30.0, r_start_position=1),
        _anchor(q_best_time=30.5, r_start_position=2),
    ]
    rows = compute_field_strength_features(anchors)
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == set(FIELD_STRENGTH_FEATURE_KEYS)


def test_field_strength_empty_input_returns_empty_list() -> None:
    assert compute_field_strength_features([]) == []


def test_pole_gap_seconds_uses_fastest_qual_time() -> None:
    anchors = [
        _anchor(q_best_time=30.0, r_start_position=1),
        _anchor(q_best_time=30.5, r_start_position=2),
        _anchor(q_best_time=31.0, r_start_position=3),
    ]
    rows = compute_field_strength_features(anchors)
    assert rows[0]["qf_pole_gap_seconds"] == 0.0
    assert rows[1]["qf_pole_gap_seconds"] == 0.5
    assert rows[2]["qf_pole_gap_seconds"] == 1.0


def test_pole_gap_seconds_none_when_missing_qual_time() -> None:
    anchors = [
        _anchor(q_best_time=None, r_start_position=1),
        _anchor(q_best_time=30.0, r_start_position=2),
    ]
    rows = compute_field_strength_features(anchors)
    assert rows[0]["qf_pole_gap_seconds"] is None
    assert rows[1]["qf_pole_gap_seconds"] == 0.0


def test_qual_and_grid_percentile_ascend_with_value() -> None:
    """Fastest / front-row drivers must land at the lowest percentile."""
    anchors = [
        _anchor(q_best_time=31.0, r_start_position=3),
        _anchor(q_best_time=30.0, r_start_position=1),
        _anchor(q_best_time=30.5, r_start_position=2),
    ]
    rows = compute_field_strength_features(anchors)
    assert rows[1]["qf_qual_percentile"] < rows[2]["qf_qual_percentile"]
    assert rows[2]["qf_qual_percentile"] < rows[0]["qf_qual_percentile"]
    assert rows[1]["qf_grid_percentile"] < rows[2]["qf_grid_percentile"]
    assert rows[2]["qf_grid_percentile"] < rows[0]["qf_grid_percentile"]
    for row in rows:
        assert 0.0 < row["qf_qual_percentile"] <= 1.0
        assert 0.0 < row["qf_grid_percentile"] <= 1.0


def test_qual_percentile_handles_missing_values() -> None:
    anchors = [
        _anchor(q_best_time=None, r_start_position=1),
        _anchor(q_best_time=30.0, r_start_position=2),
        _anchor(q_best_time=30.5, r_start_position=3),
    ]
    rows = compute_field_strength_features(anchors)
    assert rows[0]["qf_qual_percentile"] is None
    assert rows[1]["qf_qual_percentile"] is not None
    assert rows[2]["qf_qual_percentile"] is not None


def test_field_strength_avg_is_leave_one_out() -> None:
    """A driver's own ``df_ytd_*`` must not contribute to their field-strength average."""
    anchors = [
        _anchor(q_best_time=30.0, r_start_position=1, df_ytd_avg_finish=5.0, df_ytd_top5_rate=0.6),
        _anchor(q_best_time=30.5, r_start_position=2, df_ytd_avg_finish=10.0, df_ytd_top5_rate=0.4),
        _anchor(q_best_time=31.0, r_start_position=3, df_ytd_avg_finish=15.0, df_ytd_top5_rate=0.2),
    ]
    rows = compute_field_strength_features(anchors)
    assert rows[0]["qf_field_strength_ytd_avg_finish"] == round((10.0 + 15.0) / 2, 4)
    assert rows[1]["qf_field_strength_ytd_avg_finish"] == round((5.0 + 15.0) / 2, 4)
    assert rows[2]["qf_field_strength_ytd_avg_finish"] == round((5.0 + 10.0) / 2, 4)
    assert rows[0]["qf_field_strength_ytd_top5_rate"] == round((0.4 + 0.2) / 2, 4)


def test_field_strength_dispersion_is_sample_std() -> None:
    """C2 ``qf_field_strength_dispersion`` is the sample std of opponent ``df_ytd_avg_finish``."""
    anchors = [
        _anchor(q_best_time=30.0, r_start_position=1, df_ytd_avg_finish=5.0),
        _anchor(q_best_time=30.5, r_start_position=2, df_ytd_avg_finish=10.0),
        _anchor(q_best_time=31.0, r_start_position=3, df_ytd_avg_finish=20.0),
    ]
    rows = compute_field_strength_features(anchors)
    # Driver 0's opponents: [10.0, 20.0]; mean=15, var=50, std=sqrt(50)
    assert math.isclose(
        rows[0]["qf_field_strength_dispersion"], round(math.sqrt(50.0), 4), abs_tol=1e-4
    )


def test_field_strength_dispersion_none_with_fewer_than_two_opponents() -> None:
    anchors = [
        _anchor(q_best_time=30.0, r_start_position=1, df_ytd_avg_finish=5.0),
        _anchor(q_best_time=30.5, r_start_position=2, df_ytd_avg_finish=10.0),
    ]
    rows = compute_field_strength_features(anchors)
    assert rows[0]["qf_field_strength_dispersion"] is None
    assert rows[1]["qf_field_strength_dispersion"] is None


def test_field_strength_avg_falls_back_to_none_when_all_others_missing() -> None:
    anchors = [
        _anchor(q_best_time=30.0, r_start_position=1, df_ytd_avg_finish=5.0),
        _anchor(q_best_time=30.5, r_start_position=2, df_ytd_avg_finish=None),
    ]
    rows = compute_field_strength_features(anchors)
    assert rows[0]["qf_field_strength_ytd_avg_finish"] is None
