"""Pure-Python training pipeline for per-target XGBoost regressors.

This module intentionally has zero Airflow / MySQL imports so the synthetic-data
pipeline tests can exercise the full Boruta + tuning + fit flow without any
external services. The Airflow task in :mod:`include.nascar.ml.train_task`
wraps these helpers with database IO.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from include.nascar.ml.xgb_boruta import filter_columns, select_features
from include.nascar.ml.xgb_tuning import tune_xgb

_LOG = logging.getLogger(__name__)

DEFAULT_TARGETS: tuple[str, ...] = (
    "r_finish_position",
    "r_total_fastest_laps",
    "r_total_laps_led",
)

PRIMARY_TARGET = "r_finish_position"


@dataclass
class TrainedTargetModel:
    target: str
    selected_features: list[str]
    hyperparams: dict[str, Any]
    boruta_metadata: dict[str, Any]
    tuning_metadata: dict[str, Any]
    holdout_mae: float | None
    holdout_predictions: np.ndarray | None
    artifact_path: Path | None = None
    model: Any = field(repr=False, default=None)


def _build_feature_matrix(
    frame: list[dict[str, Any]],
    feature_names: list[str],
) -> np.ndarray:
    rows = np.zeros((len(frame), len(feature_names)), dtype=float)
    for i, row in enumerate(frame):
        feats = row.get("features") or {}
        for j, name in enumerate(feature_names):
            value = feats.get(name)
            if value is None:
                rows[i, j] = 0.0
            else:
                rows[i, j] = float(value)
    return np.nan_to_num(rows, nan=0.0, posinf=0.0, neginf=0.0)


def _build_target_vector(frame: list[dict[str, Any]], target: str) -> np.ndarray:
    y = np.zeros(len(frame), dtype=float)
    for i, row in enumerate(frame):
        value = (row.get("targets") or {}).get(target)
        if value is not None:
            y[i] = float(value)
    return y


def split_holdout_mask(
    frame: list[dict[str, Any]],
    holdout_weeks: int = 1,
) -> tuple[np.ndarray, np.ndarray]:
    """Mark the trailing ``holdout_weeks`` distinct week_ids as the holdout set."""
    if not frame:
        return np.array([], dtype=bool), np.array([], dtype=bool)
    weeks = sorted({int(row["week_id"]) for row in frame})
    if len(weeks) <= holdout_weeks:
        train_mask = np.ones(len(frame), dtype=bool)
        holdout_mask = np.zeros(len(frame), dtype=bool)
        return train_mask, holdout_mask
    hold = set(weeks[-holdout_weeks:])
    train_mask = np.array([int(row["week_id"]) not in hold for row in frame])
    holdout_mask = np.array([int(row["week_id"]) in hold for row in frame])
    return train_mask, holdout_mask


def train_models_for_targets(
    frame: list[dict[str, Any]],
    *,
    targets: tuple[str, ...] = DEFAULT_TARGETS,
    holdout_weeks: int = 1,
    artifact_dir: Path | None = None,
    random_state: int = 42,
    boruta_max_iter: int = 50,
    feature_allowlist: list[str] | None = None,
) -> list[TrainedTargetModel]:
    """
    Train one XGBRegressor per target on ``frame`` and return trained models.

    The caller is responsible for persisting the resulting artifacts/metadata into
    ``ml_feature_sets`` / ``ml_models`` rows; this function only does the math and
    optionally writes XGBoost JSON artifacts to ``artifact_dir`` (one file per
    target named ``<target>.json``).

    ``feature_allowlist`` (when provided) restricts the candidate Boruta input
    columns. Train-task uses it to enforce per-scope feature families (``*_pt_*``
    for track models, ``*_tt_<id>_*`` for track-type models, etc.).
    """
    from sklearn.metrics import mean_absolute_error
    from xgboost import XGBRegressor

    if not frame:
        raise ValueError("train_models_for_targets: empty frame")

    candidate_keys = {key for row in frame for key in (row.get("features") or {}).keys()}
    if feature_allowlist is not None:
        allow = set(feature_allowlist)
        candidate_keys &= allow
    feature_names = sorted(candidate_keys)
    if not feature_names:
        raise ValueError("train_models_for_targets: no feature keys present in frame")

    X = _build_feature_matrix(frame, feature_names)
    train_mask, holdout_mask = split_holdout_mask(frame, holdout_weeks=holdout_weeks)
    has_holdout = bool(holdout_mask.any())

    results: list[TrainedTargetModel] = []

    for target in targets:
        y = _build_target_vector(frame, target)
        y_train = y[train_mask]

        selected, boruta_meta = select_features(
            X[train_mask],
            y_train,
            feature_names,
            random_state=random_state,
            max_iter=boruta_max_iter,
        )

        X_sel = filter_columns(X, feature_names, selected)
        X_train = X_sel[train_mask]

        best_params, tuning_meta = tune_xgb(
            X_train,
            y_train,
            random_state=random_state,
        )

        model = XGBRegressor(**best_params, verbosity=0, n_jobs=-1)
        model.fit(X_train, y_train)

        holdout_mae: float | None = None
        holdout_predictions: np.ndarray | None = None
        if has_holdout:
            X_holdout = X_sel[holdout_mask]
            y_holdout = y[holdout_mask]
            holdout_predictions = model.predict(X_holdout)
            holdout_mae = float(mean_absolute_error(y_holdout, holdout_predictions))

        artifact_path: Path | None = None
        if artifact_dir is not None:
            artifact_dir.mkdir(parents=True, exist_ok=True)
            artifact_path = artifact_dir / f"{target}.json"
            model.save_model(str(artifact_path))

        results.append(
            TrainedTargetModel(
                target=target,
                selected_features=selected,
                hyperparams=best_params,
                boruta_metadata=boruta_meta,
                tuning_metadata=tuning_meta,
                holdout_mae=holdout_mae,
                holdout_predictions=holdout_predictions,
                artifact_path=artifact_path,
                model=model,
            )
        )

        _LOG.info(
            "train_models_for_targets: target=%s n_train=%s n_holdout=%s "
            "selected=%s holdout_mae=%s",
            target,
            int(train_mask.sum()),
            int(holdout_mask.sum()) if has_holdout else 0,
            len(selected),
            holdout_mae,
        )

    return results


def predicted_dfs_mae_on_holdout(
    frame: list[dict[str, Any]],
    trained: list[TrainedTargetModel],
    scoring_rules: dict[str, Any],
    *,
    holdout_weeks: int = 1,
) -> tuple[float | None, int]:
    """Compute MAE between predicted and observed DFS points across the holdout set."""
    from include.nascar.ml.scoring import compute_dfs_points_from_targets

    _train_mask, holdout_mask = split_holdout_mask(frame, holdout_weeks=holdout_weeks)
    holdout_indices = np.flatnonzero(holdout_mask)
    if holdout_indices.size == 0:
        return None, 0

    target_to_preds: dict[str, np.ndarray] = {}
    for entry in trained:
        if entry.holdout_predictions is None:
            return None, 0
        target_to_preds[entry.target] = entry.holdout_predictions

    abs_errors: list[float] = []
    for pos, frame_idx in enumerate(holdout_indices):
        row = frame[frame_idx]
        predicted_targets = {target: float(preds[pos]) for target, preds in target_to_preds.items()}
        predicted_total, _ = compute_dfs_points_from_targets(
            row.get("r_start_position"),
            predicted_targets,
            scoring_rules,
        )
        observed = float(row.get("dfs_points") or 0.0)
        abs_errors.append(abs(float(predicted_total) - observed))

    if not abs_errors:
        return None, 0
    return float(np.mean(abs_errors)), len(abs_errors)
