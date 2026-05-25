"""Feature matrix construction with median imputation for ML train/predict."""

from __future__ import annotations

from typing import Any

import numpy as np


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
) -> np.ndarray:
    """
    Build a float matrix for ``feature_names``.

    Missing values use ``fill_values[name]`` when provided, otherwise 0.0.
    """
    rows = np.zeros((len(frame), len(feature_names)), dtype=float)
    for i, row in enumerate(frame):
        feats = row.get("features") or {}
        for j, name in enumerate(feature_names):
            value = feats.get(name)
            if value is None:
                rows[i, j] = float((fill_values or {}).get(name, 0.0))
            else:
                rows[i, j] = float(value)
    return np.nan_to_num(rows, nan=0.0, posinf=0.0, neginf=0.0)
