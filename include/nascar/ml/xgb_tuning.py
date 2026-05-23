"""Per-target hyperparameter tuning for the XGBoost regressors.

Runs a bounded ``RandomizedSearchCV`` with ``TimeSeriesSplit`` so validation
folds always trail the training folds in race-week order. Honours the
``FS_ML_TUNE_*`` environment variables documented in the ML DAG plan so a
one-line override is possible without a code change.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import numpy as np

_LOG = logging.getLogger(__name__)

DEFAULT_PARAMS: dict[str, Any] = {
    "n_estimators": 400,
    "max_depth": 5,
    "learning_rate": 0.05,
    "min_child_weight": 3,
    "subsample": 0.85,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.0,
    "reg_lambda": 5.0,
    "tree_method": "hist",
    "random_state": 42,
}

PARAM_DISTRIBUTIONS: dict[str, list[Any]] = {
    "n_estimators": [200, 400, 800],
    "max_depth": [3, 5, 7],
    "learning_rate": [0.03, 0.05, 0.1],
    "min_child_weight": [1, 3, 5],
    "subsample": [0.7, 0.85, 1.0],
    "colsample_bytree": [0.6, 0.8, 1.0],
    "reg_alpha": [0.0, 0.1, 1.0],
    "reg_lambda": [1.0, 5.0, 10.0],
}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        _LOG.warning("Ignoring non-int %s=%r; using default %s", name, value, default)
        return default


def _tuning_disabled() -> bool:
    return os.environ.get("FS_ML_TUNE_DISABLE", "").strip() in {"1", "true", "True", "yes"}


def tune_xgb(
    X: np.ndarray,
    y: np.ndarray,
    *,
    random_state: int = 42,
    n_iter: int | None = None,
    cv_splits: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Find best XGBRegressor hyperparams for ``X``/``y`` via RandomizedSearchCV.

    Returns ``(best_params, tuning_metadata)``. When ``FS_ML_TUNE_DISABLE`` is set
    we short-circuit to ``DEFAULT_PARAMS`` and record ``tuning_skipped=True`` so
    ``ml_models.hyperparams_json`` stays auditable.
    """
    if _tuning_disabled():
        _LOG.info("tune_xgb: tuning disabled via FS_ML_TUNE_DISABLE; using defaults")
        return dict(DEFAULT_PARAMS), {"tuning_skipped": True, "reason": "FS_ML_TUNE_DISABLE"}

    from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
    from xgboost import XGBRegressor

    iters = n_iter if n_iter is not None else _env_int("FS_ML_TUNE_N_ITER", 25)
    splits = cv_splits if cv_splits is not None else _env_int("FS_ML_TUNE_CV_SPLITS", 4)

    n_rows = X.shape[0]
    # TimeSeriesSplit requires at least ``n_splits + 1`` samples; clamp gracefully.
    max_splits_for_rows = max(2, min(splits, max(2, n_rows - 1)))
    splits = max_splits_for_rows

    base_estimator = XGBRegressor(
        tree_method="hist",
        n_jobs=-1,
        random_state=random_state,
        verbosity=0,
    )

    search = RandomizedSearchCV(
        estimator=base_estimator,
        param_distributions=PARAM_DISTRIBUTIONS,
        n_iter=iters,
        scoring="neg_mean_absolute_error",
        cv=TimeSeriesSplit(n_splits=splits),
        refit=False,
        random_state=random_state,
        n_jobs=1,
        error_score="raise",
    )

    X_filled = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    search.fit(X_filled, y)

    best_params: dict[str, Any] = dict(DEFAULT_PARAMS)
    best_params.update(search.best_params_)
    best_params["random_state"] = random_state
    best_params["tree_method"] = "hist"

    metadata: dict[str, Any] = {
        "tuning_skipped": False,
        "n_iter": int(iters),
        "cv_splits": int(splits),
        "best_score": float(search.best_score_),
        "scoring": "neg_mean_absolute_error",
        "param_distributions": PARAM_DISTRIBUTIONS,
        "best_params": {k: best_params[k] for k in PARAM_DISTRIBUTIONS},
    }
    return best_params, metadata
