"""Load multi-output training labels and apply scoring rules to predictions."""

from __future__ import annotations

import json
from typing import Any

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.ml.scoring import (
    compute_dfs_points_from_targets,
    target_keys_for_rules,
)

__all__ = [
    "load_training_frame",
    "predicted_dfs_points_for_rows",
    "target_keys_for_rules",
]


def _parse_json_object(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    raise TypeError(f"Expected JSON object, got {type(raw)!r}")


def load_training_frame(
    ml_training_snapshot_ids: list[int],
    *,
    mysql_conn_id: str = "nascar_conn",
) -> list[dict[str, Any]]:
    """
    Load rows for model training: features, pre-race start, label targets, derived dfs_points.

    Returns a list of dicts with keys: features, r_start_position, targets, dfs_points,
    dfs_points_breakdown, car_driver_id, driver_id, ml_training_snapshot_id, target_keys.
    """
    if not ml_training_snapshot_ids:
        return []

    hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)
    placeholders = ", ".join(["%s"] * len(ml_training_snapshot_ids))
    sql = f"""
        SELECT
            r.ml_training_snapshot_id,
            r.car_driver_id,
            r.driver_id,
            r.r_start_position,
            r.features_json,
            l.targets_json,
            l.dfs_points,
            l.dfs_points_breakdown,
            s.target_keys,
            s.dfs_scoring_rules_id
        FROM nascar.ml_training_rows r
        JOIN nascar.ml_labels l
            ON l.ml_training_snapshot_id = r.ml_training_snapshot_id
           AND l.car_driver_id = r.car_driver_id
           AND l.deleted_at IS NULL
        JOIN nascar.ml_training_snapshots s
            ON s.id = r.ml_training_snapshot_id
           AND s.deleted_at IS NULL
        WHERE r.ml_training_snapshot_id IN ({placeholders})
          AND r.deleted_at IS NULL
    """
    rows = hook.get_records(sql, parameters=tuple(ml_training_snapshot_ids))

    frame: list[dict[str, Any]] = []
    for raw in rows:
        (
            snapshot_id,
            car_driver_id,
            driver_id,
            r_start_position,
            features_json,
            targets_json,
            dfs_points,
            dfs_points_breakdown,
            target_keys_json,
            _rules_id,
        ) = raw
        frame.append(
            {
                "ml_training_snapshot_id": int(snapshot_id),
                "car_driver_id": int(car_driver_id),
                "driver_id": int(driver_id),
                "r_start_position": r_start_position,
                "features": _parse_json_object(features_json),
                "targets": _parse_json_object(targets_json),
                "dfs_points": float(dfs_points),
                "dfs_points_breakdown": _parse_json_object(dfs_points_breakdown),
                "target_keys": json.loads(target_keys_json)
                if isinstance(target_keys_json, str)
                else target_keys_json,
            }
        )
    return frame


def predicted_dfs_points_for_rows(
    rows: list[dict[str, Any]],
    predicted_targets: list[dict[str, float]],
    scoring_rules: dict[str, Any] | str,
) -> list[dict[str, Any]]:
    """
    For each row, apply scoring rules to predicted targets + ``r_start_position``.

    Returns list of dicts with predicted_targets, predicted_dfs_points_breakdown,
    predicted_dfs_points.
    """
    if len(rows) != len(predicted_targets):
        raise ValueError("rows and predicted_targets must have the same length")

    results: list[dict[str, Any]] = []
    for row, targets in zip(rows, predicted_targets, strict=True):
        total, breakdown = compute_dfs_points_from_targets(
            row.get("r_start_position"),
            targets,
            scoring_rules,
        )
        results.append(
            {
                "predicted_targets": targets,
                "predicted_dfs_points_breakdown": breakdown,
                "predicted_dfs_points": float(total),
            }
        )
    return results
