"""Airflow callable: score the latest ``kind='predict'`` snapshot per target.

Resolves a per-target ``ml_models`` row via the routing chain
``track -> track_type -> global`` (see :mod:`include.nascar.ml.model_scope`),
runs each booster against the upcoming-week features, combines the per-target
predictions into a ``predicted_targets_json`` blob, applies the DraftKings
scoring rules, and writes one ``ml_predictions`` row per ``car_driver_id``
keyed by the primary model id.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import numpy as np

from airflow_utils.db_cursor import DbCursor
from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.enums import NascarSeries
from include.nascar.ml.model_scope import (
    SCOPE_GLOBAL,
    SCOPE_TRACK,
    SCOPE_TRACK_TYPE,
    SCOPE_TRACK_TYPE_BLEND,
    global_fallback_enabled,
    is_cold_track,
    min_track_weeks,
    track_type_ids_for_track,
)
from include.nascar.ml.scoring import compute_dfs_points_from_targets
from include.nascar.ml.snapshot_config import (
    DRAFTKINGS_DFS_ID,
    SNAPSHOT_KIND_PREDICT,
    SNAPSHOT_KIND_TRAIN,
)
from include.nascar.ml.feature_matrix import build_feature_matrix
from include.nascar.ml.xgb_train_pipeline import DEFAULT_TARGETS, PRIMARY_TARGET

_LOG = logging.getLogger(__name__)

# When a non-primary target model is missing, use these stat defaults at scoring time.
_FALLBACK_TARGET_DEFAULTS: dict[str, float] = {
    "r_total_fastest_laps": 0.0,
    "r_total_laps_led": 0.0,
}


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


def _latest_predict_snapshot(
    hook: NascarMySqlHook,
    rules_id: int,
    *,
    series_id: int = NascarSeries.CUP.series_id,
) -> dict[str, Any] | None:
    rows = hook.get_records(
        """
        SELECT
            s.id,
            s.series_id,
            s.week_id,
            s.track_id,
            tt.track_type_id,
            s.feature_keys
        FROM nascar.ml_training_snapshots s
        JOIN nascar.v_series_current_week cw
            ON cw.series_id = s.series_id
           AND cw.week_id = s.week_id
        LEFT JOIN (
            SELECT track_id, MIN(track_type_id) AS track_type_id
            FROM nascar.tracks_track_types
            WHERE deleted_at IS NULL
            GROUP BY track_id
        ) tt ON tt.track_id = s.track_id
        WHERE s.kind = %s
          AND s.dfs_scoring_rules_id = %s
          AND s.deleted_at IS NULL
          AND s.series_id = %s
        ORDER BY s.id DESC
        LIMIT 1
        """,
        parameters=(SNAPSHOT_KIND_PREDICT, rules_id, series_id),
    )
    if not rows:
        return None
    snapshot_id, series_id, week_id, track_id, track_type_id, feature_keys_raw = rows[0]
    return {
        "id": int(snapshot_id),
        "series_id": int(series_id),
        "week_id": int(week_id),
        "track_id": int(track_id),
        "track_type_id": int(track_type_id) if track_type_id is not None else None,
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


def _scope_lookup(
    hook: NascarMySqlHook,
    *,
    target: str,
    series_id: int,
    scope: str,
    track_id: int | None,
    track_type_id: int | None,
) -> dict[str, Any] | None:
    """Return the most recent active model for ``(series_id, target, scope, key)`` or ``None``.

    The ``m.series_id = %s`` filter is the safety bar that prevents a Cup model
    from being picked up for an Xfinity / Truck predict snapshot at the same
    physical track.
    """
    if scope == SCOPE_TRACK:
        if track_id is None:
            return None
        scope_clause = "AND m.model_scope = %s AND m.track_id = %s"
        scope_params: tuple[Any, ...] = (scope, int(track_id))
    elif scope == SCOPE_TRACK_TYPE:
        if track_type_id is None:
            return None
        scope_clause = "AND m.model_scope = %s AND m.track_type_id = %s"
        scope_params = (scope, int(track_type_id))
    elif scope == SCOPE_GLOBAL:
        scope_clause = "AND m.model_scope = %s"
        scope_params = (scope,)
    else:
        raise ValueError(f"unknown model_scope: {scope!r}")

    rows = hook.get_records(
        f"""
        SELECT m.id, m.artifact_uri, m.hyperparams_json, m.series_id, m.model_scope,
               m.track_id, m.track_type_id, fs.selected_features
        FROM nascar.ml_models m
        JOIN nascar.ml_feature_sets fs
            ON fs.id = m.ml_feature_set_id
           AND fs.deleted_at IS NULL
        WHERE m.deleted_at IS NULL
          AND m.series_id = %s
          AND JSON_CONTAINS(fs.target_keys, JSON_QUOTE(%s))
          AND JSON_LENGTH(fs.target_keys) = 1
          {scope_clause}
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT 1
        """,
        parameters=(int(series_id), target, *scope_params),
    )
    if not rows:
        return None
    (
        model_id,
        artifact_uri,
        hyperparams_json,
        m_series_id,
        model_scope,
        m_track_id,
        m_track_type_id,
        selected_features_json,
    ) = rows[0]
    return {
        "id": int(model_id),
        "artifact_uri": artifact_uri,
        "hyperparams": _parse_json(hyperparams_json) or {},
        "selected_features": _parse_json(selected_features_json) or [],
        "series_id": int(m_series_id),
        "model_scope": model_scope,
        "track_id": int(m_track_id) if m_track_id is not None else None,
        "track_type_id": int(m_track_type_id) if m_track_type_id is not None else None,
    }


def _resolve_model_for_target(
    hook: NascarMySqlHook,
    *,
    target: str,
    series_id: int,
    track_id: int | None,
    track_type_id: int | None,
) -> dict[str, Any] | None:
    """Walk the routing chain ``track -> track_type -> global`` for ``(series_id, target)``.

    Returns ``None`` if no usable model exists; the caller decides how to
    surface the skip. The series filter applies at every step, so Cup / Xfinity
    / Truck never share artifacts.
    """
    entry = _scope_lookup(
        hook,
        target=target,
        series_id=series_id,
        scope=SCOPE_TRACK,
        track_id=track_id,
        track_type_id=track_type_id,
    )
    if entry is not None and entry.get("artifact_uri"):
        return entry
    entry = _scope_lookup(
        hook,
        target=target,
        series_id=series_id,
        scope=SCOPE_TRACK_TYPE,
        track_id=track_id,
        track_type_id=track_type_id,
    )
    if entry is not None and entry.get("artifact_uri"):
        return entry
    if global_fallback_enabled():
        entry = _scope_lookup(
            hook,
            target=target,
            series_id=series_id,
            scope=SCOPE_GLOBAL,
            track_id=None,
            track_type_id=None,
        )
        if entry is not None and entry.get("artifact_uri"):
            return entry
    return None


def _predict_with_model(
    rows: list[dict[str, Any]],
    model_entry: dict[str, Any],
    *,
    booster_cls: type,
) -> np.ndarray:
    booster = booster_cls()
    booster.load_model(model_entry["artifact_uri"])
    features = build_feature_matrix(rows, model_entry["selected_features"])
    return booster.predict(features)


def _predict_track_type_blend(
    hook: NascarMySqlHook,
    *,
    target: str,
    series_id: int,
    track_id: int,
    rows: list[dict[str, Any]],
    booster_cls: type,
) -> tuple[np.ndarray | None, dict[str, Any] | None, int | None]:
    """Average per-driver predictions across every usable track_type model for ``track_id``."""
    type_ids = track_type_ids_for_track(hook, track_id)
    if not type_ids:
        return None, None, None

    per_type_preds: list[np.ndarray] = []
    contributor_ids: list[int] = []
    used_type_ids: list[int] = []
    for type_id in type_ids:
        entry = _scope_lookup(
            hook,
            target=target,
            series_id=series_id,
            scope=SCOPE_TRACK_TYPE,
            track_id=None,
            track_type_id=type_id,
        )
        if entry is None or not entry.get("artifact_uri"):
            continue
        per_type_preds.append(_predict_with_model(rows, entry, booster_cls=booster_cls))
        contributor_ids.append(int(entry["id"]))
        used_type_ids.append(type_id)

    if not per_type_preds:
        return None, None, None

    blended = np.mean(np.stack(per_type_preds, axis=0), axis=0)
    meta = {
        "series_id": series_id,
        "model_scope": SCOPE_TRACK_TYPE_BLEND,
        "track_id": track_id,
        "track_type_id": None,
        "track_type_ids": used_type_ids,
        "n_models": len(used_type_ids),
        "model_ids": contributor_ids,
    }
    return blended, meta, min(contributor_ids)


def _predict_values_for_target(
    hook: NascarMySqlHook,
    *,
    target: str,
    series_id: int,
    track_id: int | None,
    track_type_id: int | None,
    rules_id: int,
    rows: list[dict[str, Any]],
    booster_cls: type,
) -> tuple[np.ndarray | None, dict[str, Any] | None, int | None]:
    """
    Resolve routing and return predictions for one target.

    Cold tracks (fewer than ``FS_ML_MIN_TRACK_WEEKS`` finished weeks at ``track_id``)
    without a track model average all mapped track_type models; warm tracks use
    ``track -> track_type -> global``.
    """
    track_entry = _scope_lookup(
        hook,
        target=target,
        series_id=series_id,
        scope=SCOPE_TRACK,
        track_id=track_id,
        track_type_id=track_type_id,
    )
    if track_entry is not None and track_entry.get("artifact_uri"):
        preds = _predict_with_model(rows, track_entry, booster_cls=booster_cls)
        meta = {
            "series_id": track_entry["series_id"],
            "model_scope": track_entry["model_scope"],
            "track_id": track_entry["track_id"],
            "track_type_id": track_entry["track_type_id"],
        }
        return preds, meta, int(track_entry["id"])

    cold = (
        track_id is not None
        and is_cold_track(
            hook,
            rules_id,
            series_id,
            int(track_id),
            train_kind=SNAPSHOT_KIND_TRAIN,
        )
    )
    if cold and track_id is not None:
        blended, meta, model_id = _predict_track_type_blend(
            hook,
            target=target,
            series_id=series_id,
            track_id=int(track_id),
            rows=rows,
            booster_cls=booster_cls,
        )
        if blended is not None and meta is not None and model_id is not None:
            return blended, meta, model_id

    type_entry = _scope_lookup(
        hook,
        target=target,
        series_id=series_id,
        scope=SCOPE_TRACK_TYPE,
        track_id=track_id,
        track_type_id=track_type_id,
    )
    if type_entry is not None and type_entry.get("artifact_uri"):
        preds = _predict_with_model(rows, type_entry, booster_cls=booster_cls)
        meta = {
            "series_id": type_entry["series_id"],
            "model_scope": type_entry["model_scope"],
            "track_id": type_entry["track_id"],
            "track_type_id": type_entry["track_type_id"],
        }
        return preds, meta, int(type_entry["id"])

    if global_fallback_enabled():
        global_entry = _scope_lookup(
            hook,
            target=target,
            series_id=series_id,
            scope=SCOPE_GLOBAL,
            track_id=None,
            track_type_id=None,
        )
        if global_entry is not None and global_entry.get("artifact_uri"):
            preds = _predict_with_model(rows, global_entry, booster_cls=booster_cls)
            meta = {
                "series_id": global_entry["series_id"],
                "model_scope": global_entry["model_scope"],
                "track_id": global_entry["track_id"],
                "track_type_id": global_entry["track_type_id"],
            }
            return preds, meta, int(global_entry["id"])

    return None, None, None


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
    resolved_scopes: dict[str, dict[str, Any]] = {}
    fallback_targets: list[str] = []
    cold_track = (
        snapshot["track_id"] is not None
        and is_cold_track(
            hook,
            rules_id,
            snapshot["series_id"],
            int(snapshot["track_id"]),
            train_kind=SNAPSHOT_KIND_TRAIN,
        )
    )

    for target in DEFAULT_TARGETS:
        preds, resolved_meta, model_id = _predict_values_for_target(
            hook,
            target=target,
            series_id=snapshot["series_id"],
            track_id=snapshot["track_id"],
            track_type_id=snapshot["track_type_id"],
            rules_id=rules_id,
            rows=rows,
            booster_cls=XGBRegressor,
        )
        if preds is None or model_id is None or resolved_meta is None:
            if target == PRIMARY_TARGET:
                _LOG.warning(
                    "predict_upcoming_week_task: no trained model for primary target=%s; aborting",
                    target,
                )
                return {"skipped": f"no_model_for_target:{target}"}
            default_value = _FALLBACK_TARGET_DEFAULTS.get(target, 0.0)
            target_predictions[target] = np.full(len(rows), default_value, dtype=float)
            fallback_targets.append(target)
            _LOG.warning(
                "predict_upcoming_week_task: no model for target=%s; using default=%s",
                target,
                default_value,
            )
            continue
        target_predictions[target] = preds
        model_ids[target] = model_id
        resolved_scopes[target] = resolved_meta
        if resolved_meta.get("model_scope") == SCOPE_TRACK_TYPE_BLEND:
            _LOG.info(
                "predict_upcoming_week_task: resolved target=%s -> model_id=%s "
                "series_id=%s scope=%s track_id=%s track_type_ids=%s n_models=%s",
                target,
                model_id,
                resolved_meta["series_id"],
                resolved_meta["model_scope"],
                resolved_meta.get("track_id"),
                resolved_meta.get("track_type_ids"),
                resolved_meta.get("n_models"),
            )
        else:
            _LOG.info(
                "predict_upcoming_week_task: resolved target=%s -> model_id=%s "
                "series_id=%s scope=%s track_id=%s track_type_id=%s",
                target,
                model_id,
                resolved_meta["series_id"],
                resolved_meta["model_scope"],
                resolved_meta.get("track_id"),
                resolved_meta.get("track_type_id"),
            )

    if PRIMARY_TARGET not in model_ids:
        return {"skipped": f"no_model_for_target:{PRIMARY_TARGET}"}

    primary_model_id = model_ids[PRIMARY_TARGET]
    insert_params: list[tuple[Any, ...]] = []
    for i, row in enumerate(rows):
        predicted_targets = {
            target: float(predictions[i]) for target, predictions in target_predictions.items()
        }
        total, breakdown = compute_dfs_points_from_targets(
            row.get("r_start_position"),
            predicted_targets,
            scoring_rules,
        )
        insert_params.append(
            (
                primary_model_id,
                snapshot["id"],
                row["car_driver_id"],
                json.dumps(predicted_targets),
                json.dumps(breakdown),
                round(float(total), 3),
            )
        )

    def _write_predictions(_conn: object, cursor: DbCursor) -> None:
        cursor.execute(
            """
            UPDATE nascar.ml_predictions
            SET deleted_at = UTC_TIMESTAMP()
            WHERE ml_model_id = %s
              AND ml_training_snapshot_id = %s
              AND deleted_at IS NULL
            """,
            (primary_model_id, snapshot["id"]),
        )
        if insert_params:
            cursor.executemany(
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
                insert_params,
            )

    hook.run_in_transaction(_write_predictions)
    inserted = len(insert_params)

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
        "series_id": snapshot["series_id"],
        "week_id": snapshot["week_id"],
        "track_id": snapshot["track_id"],
        "track_type_id": snapshot["track_type_id"],
        "cold_track": cold_track,
        "min_track_weeks": min_track_weeks(),
        "primary_model_id": primary_model_id,
        "model_ids": model_ids,
        "resolved_scopes": resolved_scopes,
        "fallback_targets": fallback_targets,
        "predictions_inserted": inserted,
    }
