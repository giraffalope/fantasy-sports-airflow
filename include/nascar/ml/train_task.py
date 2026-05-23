"""Airflow callable: train scoped XGBoost regressors when new finished Cup weeks appear.

For each (scope, target) pair we slice the training frame, run Boruta + tuning
on the appropriate feature allowlist, fit the regressor, and persist a row in
``ml_models`` keyed by ``model_scope`` / ``track_id`` / ``track_type_id``:

- ``track`` scope: one model per ``track_id`` with at least
  ``FS_ML_MIN_TRACK_WEEKS`` finished weeks.
- ``track_type`` scope: one model per ``track_type_id`` with at least
  ``FS_ML_MIN_TYPE_WEEKS`` finished weeks (used when an upcoming race lacks
  enough history at its track for a per-track model).
- ``global`` scope: legacy fallback, retained for safety while per-track models
  warm up. Disable via ``FS_ML_ENABLE_GLOBAL_FALLBACK=0``.

The task short-circuits when ``ml_model_training_snapshots`` already covers the
latest finished ``ml_training_snapshots.week_id`` for every scope/target combo
that has enough history to train, so we don't refit every hour.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.ml.model_scope import (
    ALL_SCOPES,
    SCOPE_GLOBAL,
    SCOPE_TRACK,
    SCOPE_TRACK_TYPE,
    distinct_week_count,
    features_for_scope,
    global_fallback_enabled,
    group_frame_by_series,
    group_frame_by_track,
    group_frame_by_track_type,
    min_track_type_weeks,
    min_track_weeks,
)
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID, SNAPSHOT_KIND_TRAIN
from include.nascar.ml.training_targets import load_training_frame
from include.nascar.ml.xgb_train_pipeline import (
    DEFAULT_TARGETS,
    PRIMARY_TARGET,
    TrainedTargetModel,
    predicted_dfs_mae_on_holdout,
    train_models_for_targets,
)

_LOG = logging.getLogger(__name__)

_DEFAULT_ARTIFACT_ROOT = "/opt/airflow/artifacts/ml"


def _artifact_root() -> Path:
    return Path(os.environ.get("FS_ML_ARTIFACT_ROOT") or _DEFAULT_ARTIFACT_ROOT)


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
    if isinstance(rules_raw, (bytes, bytearray)):
        rules_raw = rules_raw.decode("utf-8")
    rules = json.loads(rules_raw) if isinstance(rules_raw, str) else rules_raw
    return int(rules_id), rules


def _latest_train_week(hook: NascarMySqlHook, rules_id: int) -> int | None:
    rows = hook.get_records(
        """
        SELECT MAX(week_id)
        FROM nascar.ml_training_snapshots
        WHERE kind = %s
          AND dfs_scoring_rules_id = %s
          AND deleted_at IS NULL
        """,
        parameters=(SNAPSHOT_KIND_TRAIN, rules_id),
    )
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])


def _max_covered_train_week(hook: NascarMySqlHook, rules_id: int) -> int | None:
    rows = hook.get_records(
        """
        SELECT MAX(s.week_id)
        FROM nascar.ml_model_training_snapshots mts
        JOIN nascar.ml_training_snapshots s ON s.id = mts.ml_training_snapshot_id
        JOIN nascar.ml_models m ON m.id = mts.ml_model_id AND m.deleted_at IS NULL
        WHERE s.deleted_at IS NULL
          AND s.kind = %s
          AND s.dfs_scoring_rules_id = %s
        """,
        parameters=(SNAPSHOT_KIND_TRAIN, rules_id),
    )
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])


def _train_snapshot_ids(hook: NascarMySqlHook, rules_id: int) -> list[int]:
    rows = hook.get_records(
        """
        SELECT id
        FROM nascar.ml_training_snapshots
        WHERE kind = %s
          AND dfs_scoring_rules_id = %s
          AND deleted_at IS NULL
        ORDER BY week_id ASC, id ASC
        """,
        parameters=(SNAPSHOT_KIND_TRAIN, rules_id),
    )
    return [int(r[0]) for r in rows]


def _insert_feature_set(
    hook: NascarMySqlHook,
    *,
    target: str,
    snapshot_id: int | None,
    selected_features: list[str],
    boruta_metadata: dict[str, Any],
) -> int:
    hook.run(
        """
        INSERT INTO nascar.ml_feature_sets (
            ml_training_snapshot_id,
            target_keys,
            selected_features,
            boruta_metadata
        ) VALUES (%s, %s, %s, %s)
        """,
        parameters=(
            snapshot_id,
            json.dumps([target]),
            json.dumps(selected_features),
            json.dumps(boruta_metadata),
        ),
    )
    rows = hook.get_records("SELECT LAST_INSERT_ID()")
    return int(rows[0][0])


def _insert_model(
    hook: NascarMySqlHook,
    *,
    feature_set_id: int,
    hyperparams: dict[str, Any],
    artifact_uri: str | None,
    series_id: int,
    scope: str,
    track_id: int | None,
    track_type_id: int | None,
) -> int:
    hook.run(
        """
        INSERT INTO nascar.ml_models (
            ml_feature_set_id,
            algorithm,
            series_id,
            model_scope,
            track_id,
            track_type_id,
            hyperparams_json,
            artifact_uri
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        parameters=(
            feature_set_id,
            "xgboost",
            int(series_id),
            scope,
            track_id,
            track_type_id,
            json.dumps(hyperparams),
            artifact_uri,
        ),
    )
    rows = hook.get_records("SELECT LAST_INSERT_ID()")
    return int(rows[0][0])


def _update_model_artifact(hook: NascarMySqlHook, model_id: int, artifact_uri: str) -> None:
    hook.run(
        "UPDATE nascar.ml_models SET artifact_uri = %s WHERE id = %s",
        parameters=(artifact_uri, model_id),
    )


def _insert_model_training_snapshots(
    hook: NascarMySqlHook,
    *,
    model_id: int,
    snapshot_ids: list[int],
) -> None:
    for snapshot_id in snapshot_ids:
        hook.run(
            """
            INSERT INTO nascar.ml_model_training_snapshots (
                ml_model_id,
                ml_training_snapshot_id
            ) VALUES (%s, %s)
            """,
            parameters=(model_id, snapshot_id),
        )


def _insert_evaluation(
    hook: NascarMySqlHook,
    *,
    model_id: int,
    metric: str,
    metric_value: float,
    sample_count: int,
    eval_type: str = "global",
    eval_key: int | None = None,
) -> None:
    hook.run(
        """
        INSERT INTO nascar.ml_model_evaluations (
            ml_model_id,
            eval_type,
            eval_key,
            metric,
            metric_value,
            sample_count
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        parameters=(
            model_id,
            eval_type,
            eval_key,
            metric,
            round(float(metric_value), 6),
            int(sample_count),
        ),
    )


def _persist_trained_model(
    hook: NascarMySqlHook,
    *,
    entry: TrainedTargetModel,
    snapshot_ids: list[int],
    artifact_root: Path,
    series_id: int,
    scope: str,
    track_id: int | None,
    track_type_id: int | None,
) -> int:
    feature_set_id = _insert_feature_set(
        hook,
        target=entry.target,
        snapshot_id=snapshot_ids[-1] if snapshot_ids else None,
        selected_features=entry.selected_features,
        boruta_metadata={
            "boruta": entry.boruta_metadata,
            "tuning": entry.tuning_metadata,
            "scope": {
                "series_id": int(series_id),
                "model_scope": scope,
                "track_id": track_id,
                "track_type_id": track_type_id,
            },
        },
    )
    hyperparams = {"target": entry.target, **entry.hyperparams}
    model_id = _insert_model(
        hook,
        feature_set_id=feature_set_id,
        hyperparams=hyperparams,
        artifact_uri=None,
        series_id=series_id,
        scope=scope,
        track_id=track_id,
        track_type_id=track_type_id,
    )

    if entry.artifact_path is not None:
        final_dir = artifact_root / str(model_id)
        final_dir.mkdir(parents=True, exist_ok=True)
        final_path = final_dir / f"{entry.target}.json"
        entry.artifact_path.replace(final_path)
        entry.artifact_path = final_path
        _update_model_artifact(hook, model_id, str(final_path))

    _insert_model_training_snapshots(hook, model_id=model_id, snapshot_ids=snapshot_ids)

    if entry.holdout_mae is not None and entry.holdout_predictions is not None:
        _insert_evaluation(
            hook,
            model_id=model_id,
            metric="mae",
            metric_value=entry.holdout_mae,
            sample_count=int(entry.holdout_predictions.shape[0]),
        )

    return model_id


def _snapshot_ids_for_rows(rows: list[dict[str, Any]]) -> list[int]:
    """Distinct snapshot ids for the rows, sorted to match training order."""
    seen: dict[int, None] = {}
    for row in rows:
        snapshot_id = row.get("ml_training_snapshot_id")
        if snapshot_id is not None and int(snapshot_id) not in seen:
            seen[int(snapshot_id)] = None
    return list(seen.keys())


def _train_scope(
    *,
    series_id: int,
    scope: str,
    rows: list[dict[str, Any]],
    track_id: int | None,
    track_type_id: int | None,
    feature_keys: list[str],
    holdout_weeks: int,
    artifact_root: Path,
    hook: NascarMySqlHook,
    scoring_rules: dict[str, Any],
) -> dict[str, Any]:
    """Train one (series, scope, group) -> 3 target models and persist them.

    Returns a summary dict with ``trained_models`` (target -> model_id) and the
    DFS-points holdout MAE keyed against the primary model.
    """
    snapshot_ids = _snapshot_ids_for_rows(rows)
    if not snapshot_ids:
        return {"skipped": "no_snapshots", "rows": 0}

    staging_dir = artifact_root / "_staging" / uuid.uuid4().hex

    trained = train_models_for_targets(
        rows,
        targets=DEFAULT_TARGETS,
        holdout_weeks=holdout_weeks,
        artifact_dir=staging_dir,
        feature_allowlist=feature_keys,
    )

    written: dict[str, int] = {}
    primary_model_id: int | None = None
    for entry in trained:
        model_id = _persist_trained_model(
            hook,
            entry=entry,
            snapshot_ids=snapshot_ids,
            artifact_root=artifact_root,
            series_id=series_id,
            scope=scope,
            track_id=track_id,
            track_type_id=track_type_id,
        )
        written[entry.target] = model_id
        if entry.target == PRIMARY_TARGET:
            primary_model_id = model_id

    dfs_mae, dfs_n = predicted_dfs_mae_on_holdout(
        rows,
        trained,
        scoring_rules,
        holdout_weeks=holdout_weeks,
    )
    if dfs_mae is not None and primary_model_id is not None:
        _insert_evaluation(
            hook,
            model_id=primary_model_id,
            metric="dfs_points_mae",
            metric_value=dfs_mae,
            sample_count=dfs_n,
        )

    try:
        staging_dir.rmdir()
    except OSError:
        pass

    _LOG.info(
        "train_scope series_id=%s scope=%s track_id=%s track_type_id=%s n_rows=%s "
        "n_features=%s trained=%s dfs_mae=%s",
        series_id,
        scope,
        track_id,
        track_type_id,
        len(rows),
        len(feature_keys),
        written,
        dfs_mae,
    )
    return {
        "series_id": int(series_id),
        "scope": scope,
        "track_id": track_id,
        "track_type_id": track_type_id,
        "trained_models": written,
        "primary_model_id": primary_model_id,
        "dfs_points_mae": dfs_mae,
        "n_rows": len(rows),
    }


def train_models_if_needed_task(**_context: object) -> dict[str, Any]:
    """Airflow entrypoint for the ML DAG's ``train_models_if_needed`` task."""
    mysql_conn_id = "nascar_conn"
    dfs_id = DRAFTKINGS_DFS_ID
    hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)

    rules = _active_rules(hook, dfs_id)
    if rules is None:
        _LOG.warning("train_models_if_needed_task: no active dfs_scoring_rules for dfs_id=%s", dfs_id)
        return {"skipped": "no_active_rules"}
    rules_id, scoring_rules = rules

    latest_train_week = _latest_train_week(hook, rules_id)
    if latest_train_week is None:
        _LOG.info("train_models_if_needed_task: no train snapshots yet for rules_id=%s", rules_id)
        return {"skipped": "no_training_snapshots"}

    covered_week = _max_covered_train_week(hook, rules_id)
    if covered_week is not None and covered_week >= latest_train_week:
        _LOG.info(
            "train_models_if_needed_task: covered_week=%s >= latest=%s; skipping retrain",
            covered_week,
            latest_train_week,
        )
        return {
            "skipped": "no_new_finished_weeks",
            "latest_train_week": latest_train_week,
            "covered_week": covered_week,
        }

    snapshot_ids = _train_snapshot_ids(hook, rules_id)
    if not snapshot_ids:
        return {"skipped": "no_training_snapshots"}

    frame = load_training_frame(snapshot_ids, mysql_conn_id=mysql_conn_id)
    if not frame:
        _LOG.warning("train_models_if_needed_task: training frame empty for snapshots=%s", snapshot_ids)
        return {"skipped": "empty_frame"}

    artifact_root = _artifact_root()

    holdout_weeks_env = os.environ.get("FS_ML_HOLDOUT_WEEKS")
    try:
        holdout_weeks = max(0, int(holdout_weeks_env)) if holdout_weeks_env else 1
    except ValueError:
        holdout_weeks = 1

    all_feature_keys = sorted({k for row in frame for k in (row.get("features") or {}).keys()})

    scope_results: list[dict[str, Any]] = []

    track_threshold = min_track_weeks()
    type_threshold = min_track_type_weeks()
    enable_global = global_fallback_enabled()

    # Outer series loop ensures Cup / Xfinity / Truck models never share weights.
    for series_id, series_rows in group_frame_by_series(frame).items():
        # Per-track models: one per track with enough finished weeks (within series).
        for track_id, rows in group_frame_by_track(series_rows).items():
            if distinct_week_count(rows) < track_threshold:
                continue
            track_features = features_for_scope(all_feature_keys, scope=SCOPE_TRACK)
            if not track_features:
                continue
            scope_results.append(
                _train_scope(
                    series_id=series_id,
                    scope=SCOPE_TRACK,
                    rows=rows,
                    track_id=track_id,
                    track_type_id=None,
                    feature_keys=track_features,
                    holdout_weeks=holdout_weeks,
                    artifact_root=artifact_root,
                    hook=hook,
                    scoring_rules=scoring_rules,
                )
            )

        # Per-track-type models: similar-track fallback for cold tracks (within series).
        for track_type_id, rows in group_frame_by_track_type(series_rows).items():
            if distinct_week_count(rows) < type_threshold:
                continue
            type_features = features_for_scope(
                all_feature_keys,
                scope=SCOPE_TRACK_TYPE,
                track_type_id=track_type_id,
            )
            if not type_features:
                continue
            scope_results.append(
                _train_scope(
                    series_id=series_id,
                    scope=SCOPE_TRACK_TYPE,
                    rows=rows,
                    track_id=None,
                    track_type_id=track_type_id,
                    feature_keys=type_features,
                    holdout_weeks=holdout_weeks,
                    artifact_root=artifact_root,
                    hook=hook,
                    scoring_rules=scoring_rules,
                )
            )

        # Per-series global fallback (legacy behavior). Disable with FS_ML_ENABLE_GLOBAL_FALLBACK=0.
        if enable_global:
            global_features = features_for_scope(all_feature_keys, scope=SCOPE_GLOBAL)
            scope_results.append(
                _train_scope(
                    series_id=series_id,
                    scope=SCOPE_GLOBAL,
                    rows=series_rows,
                    track_id=None,
                    track_type_id=None,
                    feature_keys=global_features,
                    holdout_weeks=holdout_weeks,
                    artifact_root=artifact_root,
                    hook=hook,
                    scoring_rules=scoring_rules,
                )
            )

    _LOG.info(
        "train_models_if_needed_task: latest_week=%s covered_week=%s scopes_trained=%s",
        latest_train_week,
        covered_week,
        len(scope_results),
    )
    return {
        "skipped": None,
        "latest_train_week": latest_train_week,
        "covered_week": covered_week,
        "scope_results": scope_results,
        "available_scopes": list(ALL_SCOPES),
    }
