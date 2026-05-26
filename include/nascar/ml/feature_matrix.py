"""Feature matrix construction with median imputation for ML train/predict."""

from __future__ import annotations

import logging
import os
from typing import Any

import numpy as np

_LOG = logging.getLogger(__name__)


class FeatureValueError(ValueError):
    """Raised by :func:`build_feature_matrix` when residual NaN/inf survives fill.

    Carries the per-feature non-finite counts as ``counts`` so callers can log
    a structured diagnostic instead of guessing which column drifted.
    """

    def __init__(self, message: str, *, counts: dict[str, int]) -> None:
        super().__init__(message)
        self.counts: dict[str, int] = dict(counts)


def _strict_features_env_default() -> bool:
    raw = (os.environ.get("FS_ML_STRICT_FEATURES") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def feature_fill_values(
    frame: list[dict[str, Any]],
    feature_names: list[str],
) -> dict[str, float]:
    """Per-feature median from non-null values in ``frame`` (0.0 if all missing)."""
    fills: dict[str, float] = {}
    for name in feature_names:
        values: list[float] = []
        for row in frame:
            raw = (row.get("features") or {}).get(name)
            if raw is not None:
                values.append(float(raw))
        fills[name] = float(np.median(values)) if values else 0.0
    return fills


def build_feature_matrix(
    frame: list[dict[str, Any]],
    feature_names: list[str],
    *,
    fill_values: dict[str, float] | None = None,
    strict: bool | None = None,
) -> np.ndarray:
    """
    Build a float matrix for ``feature_names``.

    Missing values use ``fill_values[name]`` when provided, otherwise 0.0.

    B2: After fill, any residual NaN / +-inf in the matrix is a data-quality
    signal — either the snapshot serialized a NaN literal or an upstream
    computation produced inf. The previous behavior silently swallowed both
    via :func:`numpy.nan_to_num`, hiding upstream regressions until predict
    started returning garbage.

    Behavior now:

    - **Default / predict path** (``strict=False`` or ``FS_ML_STRICT_FEATURES``
      unset): non-finite cells are logged at WARNING with per-feature counts,
      then zero-filled. This preserves predict-time forward compatibility
      with cohorts that emit a stray NaN from a misconfigured upstream join.
    - **Strict / train path** (``strict=True`` or ``FS_ML_STRICT_FEATURES=1``):
      raises :class:`FeatureValueError` with the per-feature counts attached.
      Training tasks pass ``strict=True`` so a snapshot-build regression
      fails the cohort instead of silently learning around it.

    Memory: the returned matrix is ``float32``. XGBoost ``tree_method='hist'``
    and ``BorutaPy``'s shadow-feature copy both accept float32 natively with
    no measurable quality impact at our cohort sizes (~10^4 rows x ~300
    features), and the halved per-cell footprint propagates through every
    downstream copy (Boruta's 2x shadow matrix, ``X_filled`` inside
    ``tune_xgb``, the CV slices) so the cohort training loop fits inside the
    12 GB worker budget.
    """
    rows = np.zeros((len(frame), len(feature_names)), dtype=np.float32)
    for i, row in enumerate(frame):
        feats = row.get("features") or {}
        for j, name in enumerate(feature_names):
            value = feats.get(name)
            if value is None:
                rows[i, j] = np.float32((fill_values or {}).get(name, 0.0))
            else:
                rows[i, j] = np.float32(value)

    non_finite_mask = ~np.isfinite(rows)
    if not non_finite_mask.any():
        return rows

    use_strict = _strict_features_env_default() if strict is None else bool(strict)
    column_counts: dict[str, int] = {}
    for j, name in enumerate(feature_names):
        c = int(non_finite_mask[:, j].sum())
        if c > 0:
            column_counts[name] = c
    total = int(non_finite_mask.sum())

    if use_strict:
        raise FeatureValueError(
            f"build_feature_matrix: {total} non-finite cell(s) survived "
            f"median fill across {len(column_counts)} feature(s); strict "
            f"mode aborted the matrix. Per-feature counts: {column_counts}",
            counts=column_counts,
        )

    _LOG.warning(
        "build_feature_matrix: zero-filling %s non-finite cell(s) across "
        "%s feature(s); counts=%s. Enable FS_ML_STRICT_FEATURES=1 to fail "
        "train tasks on this instead of silently masking it.",
        total,
        len(column_counts),
        column_counts,
    )
    return np.nan_to_num(rows, nan=0.0, posinf=0.0, neginf=0.0)
