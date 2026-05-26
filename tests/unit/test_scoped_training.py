"""Cohort-scope training tests for :func:`train_models_for_targets`.

We synthesize a tiny multi-series / multi-track frame with per-track
(``*_pt_*``) and per-track-type (``*_tt_<id>_*``) features, then run the
trainer with the cohort feature allowlist from
:func:`include.nascar.ml.model_scope.features_for_scope` and assert that
``*_tt_*`` keys are excluded entirely. We also verify the series partitioning
helper isolates Cup vs Xfinity rows correctly.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from include.nascar.ml.model_scope import (
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


def test_cohort_allowlist_excludes_all_tt_keys() -> None:
    frame = _synthetic_multi_track_frame()
    all_keys = sorted({k for row in frame for k in row["features"].keys()})
    allowlist = features_for_scope(all_keys, scope=SCOPE_TRACK_TYPE)
    assert "cd_pt_finish_avg" in allowlist
    assert "drv_pt_top10_count" in allowlist
    assert "r_start_position" in allowlist
    assert "manufacturer_id" in allowlist
    assert all("_tt_" not in key for key in allowlist)


def test_cohort_allowlist_keeps_other_type_tt_keys() -> None:
    """C3: ``track_type_id=3`` cohort strips own ``_tt_3_*`` but keeps ``_tt_5_*``.

    The synthetic frame mixes type-3 and type-5 track aggregates per row.
    When training the type-3 cohort, the type-3 columns are constant within
    the training frame by construction (they all come from sibling type-3
    tracks) and add only noise — drop them. Type-5 columns are signal about
    how each driver handles a different geometry — keep them.
    """
    frame = _synthetic_multi_track_frame()
    all_keys = sorted({k for row in frame for k in row["features"].keys()})
    allowlist = features_for_scope(
        all_keys, scope=SCOPE_TRACK_TYPE, track_type_id=3
    )
    assert "cd_tt_3_finish_avg" not in allowlist
    assert "drv_tt_3_top10_count" not in allowlist
    assert "cd_tt_5_finish_avg" in allowlist
    assert "drv_tt_5_top10_count" in allowlist
    assert "cd_pt_finish_avg" in allowlist
    assert "r_start_position" in allowlist


def test_cohort_training_does_not_select_tt_features(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Training a cohort (track_type_id=3) on sibling-track rows must not select ``*_tt_*``."""
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    frame = _synthetic_multi_track_frame()
    cohort_rows = [row for row in frame if row["track_type_id"] == 3]
    all_keys = sorted({k for row in frame for k in row["features"].keys()})
    allowlist = features_for_scope(all_keys, scope=SCOPE_TRACK_TYPE)

    trained = train_models_for_targets(
        cohort_rows,
        targets=DEFAULT_TARGETS,
        holdout_weeks=1,
        artifact_dir=tmp_path,
        feature_allowlist=allowlist,
        boruta_max_iter=8,
    )

    for entry in trained:
        for feature_name in entry.selected_features:
            assert "_tt_" not in feature_name, (
                f"cohort scope leaked a track-type feature: {feature_name}"
            )


def test_train_target_mean_populated_per_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """B3: ``TrainedTargetModel.train_target_mean`` carries the y_train mean.

    The mean is what predict-time uses when a secondary target has no
    usable cohort model. It must reflect the per-target training labels,
    not be a single constant across targets, otherwise the bias correction
    would collapse to the primary's mean.
    """
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    frame = _synthetic_multi_track_frame()
    cohort_rows = [row for row in frame if row["track_type_id"] == 3]
    all_keys = sorted({k for row in frame for k in row["features"].keys()})
    allowlist = features_for_scope(all_keys, scope=SCOPE_TRACK_TYPE)

    trained = train_models_for_targets(
        cohort_rows,
        targets=DEFAULT_TARGETS,
        holdout_weeks=1,
        artifact_dir=tmp_path,
        feature_allowlist=allowlist,
        boruta_max_iter=4,
    )

    means: dict[str, float | None] = {entry.target: entry.train_target_mean for entry in trained}
    for target, mean in means.items():
        assert mean is not None, f"train_target_mean missing for {target}"
        assert mean >= 0.0, f"non-negative target {target} got negative mean {mean}"
    # Each target should learn a distinct mean — they're independent label
    # distributions over the same cohort.
    assert len({round(m, 3) for m in means.values()}) > 1, (
        f"train_target_mean collapsed across targets: {means}"
    )


def test_cohort_training_supports_dynamic_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A5: ``train_models_for_targets`` honors a caller-supplied ``targets`` tuple.

    Driving targets from ``target_keys_for_rules(scoring_rules)`` instead of the
    hard-coded ``DEFAULT_TARGETS`` means rules-driven changes (adding a new
    stat, dropping a never-scored stat) propagate without an additional code
    edit. We assert a non-default subset trains successfully.
    """
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    frame = _synthetic_multi_track_frame()
    cohort_rows = [row for row in frame if row["track_type_id"] == 3]
    all_keys = sorted({k for row in frame for k in row["features"].keys()})
    allowlist = features_for_scope(all_keys, scope=SCOPE_TRACK_TYPE)

    custom_targets = ("r_finish_position", "r_total_laps_led")
    trained = train_models_for_targets(
        cohort_rows,
        targets=custom_targets,
        holdout_weeks=1,
        artifact_dir=tmp_path,
        feature_allowlist=allowlist,
        boruta_max_iter=8,
    )

    assert tuple(entry.target for entry in trained) == custom_targets


def test_series_grouping_isolates_cup_and_xfinity_rows() -> None:
    """``group_frame_by_series`` slices rows so train-task can run a per-series loop.

    A cohort model trained on the Cup sub-frame must never see Xfinity rows;
    train-task achieves this by iterating ``group_frame_by_series(frame)``
    first.
    """
    frame = _synthetic_multi_track_frame(series_ids=(1, 2))
    by_series = group_frame_by_series(frame)
    assert set(by_series.keys()) == {1, 2}
    cup_rows = by_series[1]
    xfinity_rows = by_series[2]
    assert all(row["series_id"] == 1 for row in cup_rows)
    assert all(row["series_id"] == 2 for row in xfinity_rows)
    cup_track_ids = {row["track_id"] for row in cup_rows}
    xfinity_track_ids = {row["track_id"] for row in xfinity_rows}
    assert cup_track_ids == xfinity_track_ids == {1, 2}
