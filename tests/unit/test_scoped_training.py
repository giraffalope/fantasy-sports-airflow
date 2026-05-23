"""Per-scope training tests for :func:`train_models_for_targets`.

We synthesize a tiny multi-series / multi-track frame with per-track
(``*_pt_*``) and per-track-type (``*_tt_<id>_*``) features, then run the
trainer with the scope-appropriate ``feature_allowlist`` from
:func:`include.nascar.ml.model_scope.features_for_scope` and assert that only
the in-scope features survive the Boruta + fit step. We also assert the
series partitioning helper isolates Cup vs Xfinity rows correctly.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from include.nascar.ml.model_scope import (
    SCOPE_TRACK,
    SCOPE_TRACK_TYPE,
    features_for_scope,
    group_frame_by_series,
)
from include.nascar.ml.xgb_train_pipeline import (
    DEFAULT_TARGETS,
    train_models_for_targets,
)


def _synthetic_multi_track_frame(
    seed: int = 11,
    *,
    series_ids: tuple[int, ...] = (1,),
) -> list[dict]:
    rng = np.random.default_rng(seed)
    frame: list[dict] = []
    snapshot_id_counter = 1
    for series_id in series_ids:
        for week_index in range(1, 6):
            week_id = 200 + week_index + 100 * (series_id - 1)
            for track_id in (1, 2):
                track_type_id = 3 if track_id == 1 else 5
                snapshot_id = snapshot_id_counter
                snapshot_id_counter += 1
                for driver_index in range(20):
                    skill = float(rng.normal())
                    speed = float(rng.normal())
                    finish = max(1, min(40, int(round(20 - 6 * skill + rng.normal(scale=0.4)))))
                    fastest_laps = max(0, int(round(6 + 4 * speed + rng.normal(scale=0.4))))
                    laps_led = max(0, int(round(8 + 5 * speed + rng.normal(scale=0.4))))
                    start_position = max(
                        1, min(40, int(round(20 - 4 * skill + rng.normal(scale=1))))
                    )
                    features = {
                        "r_start_position": start_position,
                        "cd_pt_finish_avg": skill,
                        "drv_pt_top10_count": skill * 2.0,
                        f"cd_tt_{track_type_id}_finish_avg": skill * 0.5,
                        f"drv_tt_{track_type_id}_top10_count": skill * 1.5,
                        "manufacturer_id": float(driver_index % 3),
                    }
                    frame.append(
                        {
                            "ml_training_snapshot_id": snapshot_id,
                            "series_id": series_id,
                            "week_id": week_id,
                            "track_id": track_id,
                            "track_type_id": track_type_id,
                            "car_driver_id": 1000 * track_id + driver_index,
                            "driver_id": 2000 + driver_index,
                            "r_start_position": start_position,
                            "features": features,
                            "targets": {
                                "r_finish_position": float(finish),
                                "r_total_fastest_laps": float(fastest_laps),
                                "r_total_laps_led": float(laps_led),
                            },
                            "dfs_points": 0.0,
                            "dfs_points_breakdown": {},
                            "target_keys": list(DEFAULT_TARGETS),
                        }
                    )
    return frame


def test_track_scope_only_trains_on_pt_and_anchor_features(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    frame = _synthetic_multi_track_frame()
    rows = [row for row in frame if row["track_id"] == 1]

    all_keys = sorted({k for row in frame for k in row["features"].keys()})
    allowlist = features_for_scope(all_keys, scope=SCOPE_TRACK)
    assert "cd_pt_finish_avg" in allowlist
    assert "cd_tt_3_finish_avg" not in allowlist

    trained = train_models_for_targets(
        rows,
        targets=DEFAULT_TARGETS,
        holdout_weeks=1,
        artifact_dir=tmp_path,
        feature_allowlist=allowlist,
        boruta_max_iter=8,
    )

    for entry in trained:
        for feature_name in entry.selected_features:
            assert "_tt_" not in feature_name, (
                f"track scope leaked a track-type feature: {feature_name}"
            )


def test_track_type_scope_only_trains_on_matching_type_features(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    frame = _synthetic_multi_track_frame()
    rows = [row for row in frame if row["track_type_id"] == 3]

    all_keys = sorted({k for row in frame for k in row["features"].keys()})
    allowlist = features_for_scope(all_keys, scope=SCOPE_TRACK_TYPE, track_type_id=3)
    assert "cd_tt_3_finish_avg" in allowlist
    assert "cd_tt_5_finish_avg" not in allowlist
    assert "cd_pt_finish_avg" not in allowlist

    trained = train_models_for_targets(
        rows,
        targets=DEFAULT_TARGETS,
        holdout_weeks=1,
        artifact_dir=tmp_path,
        feature_allowlist=allowlist,
        boruta_max_iter=8,
    )

    for entry in trained:
        for feature_name in entry.selected_features:
            assert "_pt_" not in feature_name, (
                f"track_type scope leaked a per-track feature: {feature_name}"
            )
            assert "cd_tt_5_" not in feature_name, (
                f"track_type scope leaked a wrong-type feature: {feature_name}"
            )


def test_series_grouping_isolates_cup_and_xfinity_rows() -> None:
    """``group_frame_by_series`` slices rows so train-task can run a per-series loop.

    A track model trained on the Cup sub-frame must never see Xfinity rows;
    the train-task achieves this by iterating ``group_frame_by_series(frame)``
    first.
    """
    frame = _synthetic_multi_track_frame(series_ids=(1, 2))
    by_series = group_frame_by_series(frame)
    assert set(by_series.keys()) == {1, 2}
    cup_rows = by_series[1]
    xfinity_rows = by_series[2]
    assert all(row["series_id"] == 1 for row in cup_rows)
    assert all(row["series_id"] == 2 for row in xfinity_rows)
    # Sanity check the per-series sub-frames still expose the per-track buckets.
    cup_track_ids = {row["track_id"] for row in cup_rows}
    xfinity_track_ids = {row["track_id"] for row in xfinity_rows}
    assert cup_track_ids == xfinity_track_ids == {1, 2}
