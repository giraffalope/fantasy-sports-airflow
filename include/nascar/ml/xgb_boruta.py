"""Boruta feature selection driven by an XGBoost ranker.

Designed to work with the small per-week snapshot sizes we produce: a few hundred
rows per Cup race, a few thousand rows across a season. ``boruta.BorutaPy`` calls
``estimator.fit`` repeatedly so we keep the booster cheap (``tree_method='hist'``,
``n_estimators=300``).
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np

from include.nascar.ml.xgb_tuning import xgb_device, xgb_n_jobs

_LOG = logging.getLogger(__name__)


def select_features(
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    *,
    random_state: int = 42,
    max_iter: int = 50,
) -> tuple[list[str], dict[str, Any]]:
    """
    Run BorutaPy on ``X``/``y`` and return (selected_feature_names, metadata).

    Falls back to returning all features when Boruta selects zero features so the
    downstream XGBRegressor still has something to train on. The metadata dict
    captures support / iteration counts for ``ml_feature_sets.boruta_metadata``.

    Phase 4: the inner XGBoost ``n_jobs`` is resolved via
    :func:`include.nascar.ml.xgb_tuning.xgb_n_jobs` (env var
    ``FS_ML_XGB_N_JOBS``, default ``-1`` outside the configured worker).
    Boruta's shadow-feature trick already doubles the feature matrix in
    memory; capping XGBoost's per-thread histogram buffers is the single
    biggest dial that keeps Boruta from spiking the cohort training peak.

    GPU note: the device is resolved via
    :func:`include.nascar.ml.xgb_tuning.xgb_device` (env var
    ``FS_ML_XGB_DEVICE``, default ``cpu``). Boruta is the worst-case GPU
    target in this pipeline — it calls ``estimator.fit`` up to
    ``max_iter`` times per target on a 2× shadow-augmented matrix, and
    each call pays the host→device transfer cost. On the cohort sizes we
    hit (low single-digit thousands of rows) CPU is usually faster.
    ``FS_ML_XGB_DEVICE=cuda`` is the same toggle as the rest of the
    pipeline so an operator flipping it for ``RandomizedSearchCV``
    automatically gets it here too; benchmark before assuming it's a win.
    """
    from boruta import BorutaPy
    from xgboost import XGBRegressor

    if X.shape[0] == 0:
        raise ValueError("select_features: X has 0 rows")
    if X.shape[1] != len(feature_names):
        raise ValueError(
            f"select_features: X has {X.shape[1]} cols but feature_names has {len(feature_names)}"
        )

    estimator = XGBRegressor(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        tree_method="hist",
        device=xgb_device(),
        n_jobs=xgb_n_jobs(),
        random_state=random_state,
        verbosity=0,
    )

    selector = BorutaPy(
        estimator=estimator,
        n_estimators="auto",
        max_iter=max_iter,
        random_state=random_state,
        verbose=0,
    )

    X_filled = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    selector.fit(X_filled, y)

    support = selector.support_
    weak = getattr(selector, "support_weak_", None)
    selected = [name for name, keep in zip(feature_names, support, strict=True) if keep]
    if not selected and weak is not None:
        selected = [name for name, keep in zip(feature_names, weak, strict=True) if keep]
    if not selected:
        _LOG.warning(
            "select_features: Boruta selected 0 features; falling back to all %s features",
            len(feature_names),
        )
        selected = list(feature_names)

    metadata: dict[str, Any] = {
        "selector": "boruta",
        "n_iter": int(getattr(selector, "n_iter_", 0)),
        "max_iter": int(max_iter),
        "support": [bool(v) for v in support],
        "support_weak": [bool(v) for v in weak] if weak is not None else None,
        "ranking": [int(v) for v in getattr(selector, "ranking_", [])],
        "n_selected_strong": int(sum(bool(v) for v in support)),
        "n_features": int(len(feature_names)),
    }
    return selected, metadata


def filter_columns(
    X: np.ndarray,
    feature_names: list[str],
    selected: list[str],
) -> np.ndarray:
    """Return ``X`` restricted to columns in ``selected`` (NaN-safe)."""
    index = {name: i for i, name in enumerate(feature_names)}
    cols = [index[name] for name in selected if name in index]
    if not cols:
        raise ValueError("filter_columns: no overlap between feature_names and selected")
    sub = X[:, cols]
    return np.nan_to_num(sub, nan=0.0, posinf=0.0, neginf=0.0)
