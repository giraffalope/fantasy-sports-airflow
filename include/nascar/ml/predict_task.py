"""Airflow callable: score the latest ``kind='predict'`` snapshot per target.

Loads the latest active ``ml_models`` row per target (joined through
``ml_feature_sets``), runs each booster against the upcoming-week features,
combines the per-target predictions into a ``predicted_targets_json`` blob,
applies the DraftKings scoring rules, and writes one ``ml_predictions`` row
per ``car_driver_id`` keyed by the primary model id.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import numpy as np

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.ml.scoring import compute_dfs_points_from_targets
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID, SNAPSHOT_KIND_PREDICT
from include.nascar.ml.xgb_train_pipeline import DEFAULT_TARGETS, PRIMARY_TARGET

_LOG = logging.getLogger(__name__)


def _parse_json(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


def _active_rules(hook: NascarMySqlHook, dfs_id: int) -> tuple[int, dict[str, Any]] | None:
    rows = hook.get_records(
        """
        SELECT id, rules
        FROM nascar.dfs_scoring_rules
        WHERE dfs_id = %s AND deleted_at IS NULL
        ORDER BY version DESC, id DESC
        LIMIT 1
        """,
        parameters=(dfs_id,),
    )
    if not rows:
        return None
    rules_id, rules_raw = rows[0]
    rules = _parse_json(rules_raw) or {}
    return int(rules_id), rules


def _latest_predict_snapshot(hook: NascarMySqlHook, rules_id: int) -> dict[str, Any] | None:
    rows = hook.get_records(
        """
        SELECT id, series_id, week_id, track_id, feature_keys
        FROM nascar.ml_training_snapshots
        WHERE kind = %s
          AND dfs_scoring_rules_id = %s
          AND deleted_at IS NULL
        ORDER BY week_id DESC, id DESC
        LIMIT 1
        """,
        parameters=(SNAPSHOT_KIND_PREDICT, rules_id),
    )
    if not rows:
        return None
    snapshot_id, series_id, week_id, track_id, feature_keys_raw = rows[0]
    return {
        "id": int(snapshot_id),
        "series_id": int(series_id),
        "week_id": int(week_id),
        "track_id": int(track_id),
        "feature_keys": _parse_json(feature_keys_raw) or [],
    }


def _predict_rows(hook: NascarMySqlHook, snapshot_id: int) -> list[dict[str, Any]]:
    rows = hook.get_records(
        """
        SELECT car_driver_id, r_start_position, features_json
        FROM nascar.ml_training_rows
        WHERE ml_training_snapshot_id = %s
          AND deleted_at IS NULL
        ORDER BY car_driver_id ASC
        """,
        parameters=(snapshot_id,),
    )
    parsed: list[dict[str, Any]] = []
    for car_driver_id, r_start_position, features_json in rows:
        parsed.append(
            {
                "car_driver_id": int(car_driver_id),
                "r_start_position": r_start_position,
                "features": _parse_json(features_json) or {},
            }
        )
    return parsed


def _latest_model_for_target(hook: NascarMySqlHook, target: str) -> dict[str, Any] | None:
    rows = hook.get_records(
        """
        SELECT m.id, m.artifact_uri, m.hyperparams_json, fs.selected_features
        FROM nascar.ml_models m
        JOIN nascar.ml_feature_sets fs
            ON fs.id = m.ml_feature_set_id
           AND fs.deleted_at IS NULL
        WHERE m.deleted_at IS NULL
          AND JSON_CONTAINS(fs.target_keys, JSON_QUOTE(%s))
          AND JSON_LENGTH(fs.target_keys) = 1
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT 1
        """,
        parameters=(target,),
    )
    if not rows:
        return None
    model_id, artifact_uri, hyperparams_json, selected_features_json = rows[0]
    return {
        "id": int(model_id),
        "artifact_uri": artifact_uri,
        "hyperparams": _parse_json(hyperparams_json) or {},
        "selected_features": _parse_json(selected_features_json) or [],
    }


def _build_feature_matrix(
    rows: list[dict[str, Any]],
    selected_features: list[str],
) -> np.ndarray:
    matrix = np.zeros((len(rows), len(selected_features)), dtype=float)
    for i, row in enumerate(rows):
        features = row.get("features") or {}
        for j, name in enumerate(selected_features):
            value = features.get(name)
            if value is None:
                matrix[i, j] = 0.0
            else:
                matrix[i, j] = float(value)
    return np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0)


def _soft_delete_existing_predictions(
    hook: NascarMySqlHook,
    *,
    model_id: int,
    snapshot_id: int,
) -> None:
    hook.run(
        """
        UPDATE nascar.ml_predictions
        SET deleted_at = UTC_TIMESTAMP()
        WHERE ml_model_id = %s
          AND ml_training_snapshot_id = %s
          AND deleted_at IS NULL
        """,
        parameters=(model_id, snapshot_id),
    )


def _insert_prediction(
    hook: NascarMySqlHook,
    *,
    model_id: int,
    snapshot_id: int,
    car_driver_id: int,
    predicted_targets: dict[str, float],
    predicted_breakdown: dict[str, float],
    predicted_total: float,
) -> None:
    hook.run(
        """
        INSERT INTO nascar.ml_predictions (
            ml_model_id,
            ml_training_snapshot_id,
            car_driver_id,
            predicted_targets_json,
            predicted_dfs_points_breakdown,
            predicted_dfs_points
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        parameters=(
            model_id,
            snapshot_id,
            car_driver_id,
            json.dumps(predicted_targets),
            json.dumps(predicted_breakdown),
            round(float(predicted_total), 3),
        ),
    )


def predict_upcoming_week_task(**_context: object) -> dict[str, Any]:
    """Airflow entrypoint for the ML DAG's ``predict_upcoming_week`` task."""
    from xgboost import XGBRegressor

    mysql_conn_id = "nascar_conn"
    dfs_id = DRAFTKINGS_DFS_ID
    hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)

    rules = _active_rules(hook, dfs_id)
    if rules is None:
        return {"skipped": "no_active_rules"}
    rules_id, scoring_rules = rules

    snapshot = _latest_predict_snapshot(hook, rules_id)
    if snapshot is None:
        _LOG.info("predict_upcoming_week_task: no predict snapshot for rules_id=%s", rules_id)
        return {"skipped": "no_predict_snapshot"}

    rows = _predict_rows(hook, snapshot["id"])
    if not rows:
        return {"skipped": "no_predict_rows", "snapshot_id": snapshot["id"]}

    target_predictions: dict[str, np.ndarray] = {}
    model_ids: dict[str, int] = {}
    for target in DEFAULT_TARGETS:
        model_entry = _latest_model_for_target(hook, target)
        if model_entry is None or not model_entry["artifact_uri"]:
            _LOG.warning(
                "predict_upcoming_week_task: no trained model for target=%s; aborting",
                target,
            )
            return {"skipped": f"no_model_for_target:{target}"}
        booster = XGBRegressor()
        booster.load_model(model_entry["artifact_uri"])
        X = _build_feature_matrix(rows, model_entry["selected_features"])
        target_predictions[target] = booster.predict(X)
        model_ids[target] = model_entry["id"]

    primary_model_id = model_ids.get(PRIMARY_TARGET) or next(iter(model_ids.values()))
    _soft_delete_existing_predictions(
        hook,
        model_id=primary_model_id,
        snapshot_id=snapshot["id"],
    )

    inserted = 0
    for i, row in enumerate(rows):
        predicted_targets = {
            target: float(predictions[i])
            for target, predictions in target_predictions.items()
        }
        total, breakdown = compute_dfs_points_from_targets(
            row.get("r_start_position"),
            predicted_targets,
            scoring_rules,
        )
        _insert_prediction(
            hook,
            model_id=primary_model_id,
            snapshot_id=snapshot["id"],
            car_driver_id=row["car_driver_id"],
            predicted_targets=predicted_targets,
            predicted_breakdown=breakdown,
            predicted_total=float(total),
        )
        inserted += 1

    _LOG.info(
        "predict_upcoming_week_task: snapshot_id=%s week_id=%s rows=%s primary_model=%s",
        snapshot["id"],
        snapshot["week_id"],
        inserted,
        primary_model_id,
    )
    return {
        "skipped": None,
        "snapshot_id": snapshot["id"],
        "week_id": snapshot["week_id"],
        "primary_model_id": primary_model_id,
        "model_ids": model_ids,
        "predictions_inserted": inserted,
    }
