"""End-to-end synthetic test of the per-target XGBoost pipeline.

Generates a tiny synthetic frame mimicking ``load_training_frame`` output, runs
``train_models_for_targets`` (Boruta + tuning + XGBRegressor fit), reloads the
saved artifact, and verifies that predicted-target DFS points round-trip through
``compute_dfs_points_from_targets`` to match a hand-computed total.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
from xgboost import XGBRegressor

from include.nascar.ml.scoring import compute_dfs_points_from_targets
from include.nascar.ml.xgb_train_pipeline import (
    DEFAULT_TARGETS,
    PRIMARY_TARGET,
    predicted_dfs_mae_on_holdout,
    train_models_for_targets,
)

_SCORING_RULES: dict = {
    "version": 1,
    "components": [
        {
            "name": "finish",
            "source": "r_finish_position",
            "type": "finish_table",
            "table": {},
            "formula": {"base": 41, "subtract_position": True, "min": 0},
        },
        {
            "name": "place_diff",
            "source": "r_place_diff",
            "type": "linear",
            "multiplier": 0.5,
            "fallback": {
                "from": ["r_start_position", "r_finish_position"],
                "op": "subtract",
            },
        },
        {
            "name": "fastest_laps",
            "source": "r_total_fastest_laps",
            "type": "linear",
            "multiplier": 0.45,
        },
        {
            "name": "laps_led",
            "source": "r_total_laps_led",
            "type": "linear",
            "multiplier": 0.25,
        },
    ],
}


def _synthetic_frame(*, n_weeks: int = 5, n_drivers: int = 30, seed: int = 7) -> list[dict]:
    rng = np.random.default_rng(seed)
    frame: list[dict] = []
    snapshot_id_counter = 1
    for week_index in range(1, n_weeks + 1):
        week_id = 100 + week_index
        snapshot_id = snapshot_id_counter
        snapshot_id_counter += 1
        for driver_index in range(n_drivers):
            skill = float(rng.normal(loc=0.0, scale=1.0))
            speed = float(rng.normal(loc=0.0, scale=1.0))
            noise1 = float(rng.normal(scale=0.4))
            noise2 = float(rng.normal(scale=0.4))
            extra_a = float(rng.normal())
            extra_b = float(rng.normal())

            finish = max(1, min(40, int(round(20 - 6 * skill + noise1))))
            laps_led = max(0, int(round(8 + 5 * speed + noise2)))
            fastest_laps = max(0, int(round(6 + 4 * speed + noise1)))
            start_position = max(1, min(40, int(round(20 - 4 * skill + rng.normal(scale=1.0)))))

            features = {
                "skill_feat": skill,
                "speed_feat": speed,
                "irrelevant_feat_a": extra_a,
                "irrelevant_feat_b": extra_b,
            }
            targets = {
                "r_finish_position": float(finish),
                "r_total_fastest_laps": float(fastest_laps),
                "r_total_laps_led": float(laps_led),
            }
            dfs_total, _ = compute_dfs_points_from_targets(
                start_position,
                targets,
                _SCORING_RULES,
            )

            frame.append(
                {
                    "ml_training_snapshot_id": snapshot_id,
                    "week_id": week_id,
                    "car_driver_id": 1000 + driver_index,
                    "driver_id": 2000 + driver_index,
                    "r_start_position": start_position,
                    "features": features,
                    "targets": targets,
                    "dfs_points": float(dfs_total),
                    "dfs_points_breakdown": {},
                    "target_keys": list(targets.keys()),
                }
            )
    return frame


@pytest.fixture
def synthetic_frame() -> list[dict]:
    return _synthetic_frame()


def test_pipeline_selects_features_and_serializes(
    tmp_path: Path,
    synthetic_frame: list[dict],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    trained = train_models_for_targets(
        synthetic_frame,
        targets=DEFAULT_TARGETS,
        holdout_weeks=1,
        artifact_dir=tmp_path,
        boruta_max_iter=8,
    )

    assert {entry.target for entry in trained} == set(DEFAULT_TARGETS)

    for entry in trained:
        assert entry.selected_features, f"{entry.target} produced empty feature set"
        assert entry.artifact_path is not None
        assert entry.artifact_path.exists()
        assert entry.hyperparams["tree_method"] == "hist"
        assert entry.holdout_predictions is not None

        reloaded = XGBRegressor()
        reloaded.load_model(str(entry.artifact_path))

        feature_index = {name: i for i, name in enumerate(
            sorted({k for row in synthetic_frame for k in row["features"].keys()})
        )}
        cols = [feature_index[name] for name in entry.selected_features]
        sample = np.zeros((1, len(entry.selected_features)), dtype=float)
        first = synthetic_frame[0]["features"]
        for j, name in enumerate(entry.selected_features):
            sample[0, j] = float(first.get(name) or 0.0)
        _ = reloaded.predict(sample)  # smoke check


def test_predicted_dfs_points_reproduce_hand_computed_total(
    tmp_path: Path,
    synthetic_frame: list[dict],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    trained = train_models_for_targets(
        synthetic_frame,
        targets=DEFAULT_TARGETS,
        holdout_weeks=1,
        artifact_dir=tmp_path,
        boruta_max_iter=8,
    )

    pred_by_target = {entry.target: entry.holdout_predictions for entry in trained}
    assert all(p is not None for p in pred_by_target.values())

    holdout_week = max(row["week_id"] for row in synthetic_frame)
    holdout_rows = [row for row in synthetic_frame if row["week_id"] == holdout_week]
    assert len(holdout_rows) == int(next(iter(pred_by_target.values())).shape[0])

    sample_row = holdout_rows[0]
    sample_targets = {target: float(preds[0]) for target, preds in pred_by_target.items()}
    pipeline_total, pipeline_breakdown = compute_dfs_points_from_targets(
        sample_row["r_start_position"],
        sample_targets,
        _SCORING_RULES,
    )

    hand_total = 0.0
    finish = sample_targets["r_finish_position"]
    # ``finish_table`` truncates the predicted finish position via ``int(...)``;
    # the linear ``place_diff`` fallback keeps full float precision.
    finish_int = int(finish)
    hand_total += max(0.0, 41 - finish_int)
    hand_total += (sample_row["r_start_position"] - finish) * 0.5
    hand_total += sample_targets["r_total_fastest_laps"] * 0.45
    hand_total += sample_targets["r_total_laps_led"] * 0.25

    assert float(pipeline_total) == pytest.approx(hand_total, abs=1e-3)
    assert pipeline_breakdown["finish"] == pytest.approx(max(0.0, 41 - finish_int), abs=1e-3)


def test_predicted_dfs_mae_on_holdout_runs(
    tmp_path: Path,
    synthetic_frame: list[dict],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    trained = train_models_for_targets(
        synthetic_frame,
        targets=DEFAULT_TARGETS,
        holdout_weeks=1,
        artifact_dir=tmp_path,
        boruta_max_iter=8,
    )
    mae, n = predicted_dfs_mae_on_holdout(
        synthetic_frame,
        trained,
        _SCORING_RULES,
        holdout_weeks=1,
    )
    assert mae is not None
    assert mae >= 0.0
    assert n == len([r for r in synthetic_frame if r["week_id"] == max(row["week_id"] for row in synthetic_frame)])


def test_primary_target_constant_is_in_default_targets() -> None:
    assert PRIMARY_TARGET in DEFAULT_TARGETS
