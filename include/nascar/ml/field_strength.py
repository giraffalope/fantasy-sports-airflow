"""Qualifying / field-strength (``qf_*``) feature family for ML snapshots (C2).

All values are computed **across the anchor list for one snapshot week**, so
they sit naturally in ``features_json`` per row. No SQL — pure NumPy / Python
over the in-memory anchor list once C1's ``df_*`` features are attached.

Conventions:

- ``qf_pole_gap_seconds`` uses ``q_best_time`` (seconds, already normalized
  by :func:`include.nascar.ml.snapshot_build.time_value_to_seconds` upstream).
- ``qf_qual_percentile`` ranks qualifying time ascending (faster = lower
  percentile); ``qf_grid_percentile`` ranks ``r_start_position`` ascending
  (front of grid = lower percentile). Both are normalized to ``[0, 1]`` by
  ``rank / field_size``.
- Field-strength features average ``df_ytd_*`` across all OTHER drivers in
  the same race (leave-one-out), so a single dominant outlier doesn't show
  up in their own field-strength denominator.
- Drivers with missing inputs get ``None`` for the affected key only; the
  feature matrix builder median-fills downstream.
"""

from __future__ import annotations

import math
from typing import Any

FIELD_STRENGTH_FEATURE_KEYS: tuple[str, ...] = (
    "qf_pole_gap_seconds",
    "qf_qual_percentile",
    "qf_grid_percentile",
    "qf_field_strength_ytd_avg_finish",
    "qf_field_strength_ytd_top5_rate",
    "qf_field_strength_dispersion",
)


def _ascending_percentile(
    values: list[tuple[int, float | None]],
) -> dict[int, float | None]:
    """Rank ``(idx, value)`` pairs ascending; missing values stay ``None``.

    Output: ``{idx: percentile in [0,1]}``. Ties share the same rank.
    """
    keyed = [(idx, value) for idx, value in values if value is not None]
    if not keyed:
        return {idx: None for idx, _ in values}

    keyed.sort(key=lambda pair: pair[1])
    field_size = len(keyed)
    rank_by_idx: dict[int, float] = {}
    prior_value: float | None = None
    rank = 0
    last_rank_pct = 0.0
    for position, (idx, value) in enumerate(keyed, start=1):
        if prior_value is None or value > prior_value:
            rank = position
            last_rank_pct = rank / field_size
            prior_value = value
        rank_by_idx[idx] = round(last_rank_pct, 4)

    result: dict[int, float | None] = {idx: None for idx, _ in values}
    result.update(rank_by_idx)
    return result


def _stddev(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return round(math.sqrt(variance), 4)


def _leave_one_out_avg(values: list[float | None], idx: int) -> float | None:
    others = [v for i, v in enumerate(values) if i != idx and v is not None]
    if not others:
        return None
    return round(sum(others) / len(others), 4)


def _leave_one_out_std(values: list[float | None], idx: int) -> float | None:
    others = [v for i, v in enumerate(values) if i != idx and v is not None]
    return _stddev(others)


def compute_field_strength_features(
    anchor_features: list[dict[str, Any]],
) -> list[dict[str, float | None]]:
    """Return one ``qf_*`` feature dict per anchor row, in the same order.

    ``anchor_features`` is the materialized per-driver dict already carrying
    pre-race columns (``q_best_time``, ``r_start_position``) plus the ``df_*``
    family attached by C1. Field-strength averages are leave-one-out across
    the snapshot's drivers.
    """
    n = len(anchor_features)
    if n == 0:
        return []

    q_times = [feat.get("q_best_time") for feat in anchor_features]
    start_positions = [feat.get("r_start_position") for feat in anchor_features]

    qual_pct = _ascending_percentile(list(enumerate(q_times)))
    grid_pct = _ascending_percentile(list(enumerate(start_positions)))

    valid_q_times = [float(t) for t in q_times if t is not None]
    pole_time = min(valid_q_times) if valid_q_times else None

    ytd_finishes = [feat.get("df_ytd_avg_finish") for feat in anchor_features]
    ytd_top5_rates = [feat.get("df_ytd_top5_rate") for feat in anchor_features]

    results: list[dict[str, float | None]] = []
    for idx, feat in enumerate(anchor_features):
        row: dict[str, float | None] = dict.fromkeys(FIELD_STRENGTH_FEATURE_KEYS)
        q_time = feat.get("q_best_time")
        if q_time is not None and pole_time is not None:
            row["qf_pole_gap_seconds"] = round(float(q_time) - float(pole_time), 4)
        row["qf_qual_percentile"] = qual_pct.get(idx)
        row["qf_grid_percentile"] = grid_pct.get(idx)
        row["qf_field_strength_ytd_avg_finish"] = _leave_one_out_avg(ytd_finishes, idx)
        row["qf_field_strength_ytd_top5_rate"] = _leave_one_out_avg(ytd_top5_rates, idx)
        row["qf_field_strength_dispersion"] = _leave_one_out_std(ytd_finishes, idx)
        results.append(row)
    return results
