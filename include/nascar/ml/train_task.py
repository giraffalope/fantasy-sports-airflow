"""Airflow callable: train scoped XGBoost regressors for predict on the current week.

When new finished weeks appear, this task refreshes only the models
``predict_upcoming_week`` needs for the current week's venue: walk the same
``track`` -> ``track_type`` -> ``global`` chain as predict, skip when the first
usable scope is already trained through ``latest_train_week``, and fit scopes
that are missing or stale. Cold tracks (below ``FS_ML_MIN_TRACK_WEEKS``) are
omitted from the chain; predict falls through to track_type / global.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, NamedTuple

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.cup.current_week import (
    SeriesCurrentWeekTrackContext,
    latest_train_week_for_retrain,
    resolve_current_week_track_context,
    resolve_series_current_week,
)
from include.nascar.enums import NascarSeries
from include.nascar.ml.model_scope import (
    ALL_SCOPES,
    SCOPE_GLOBAL,
    SCOPE_TRACK,
    SCOPE_TRACK_TYPE,
    features_for_scope,
    global_fallback_enabled,
    max_train_weeks,
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


def _latest_train_week_for_retrain_gate(
    hook: NascarMySqlHook,
    rules_id: int,
    *,
    series_id: int = NascarSeries.CUP.series_id,
) -> int | None:
    if resolve_series_current_week(hook, series_id=series_id) is None:
        return None
    return latest_train_week_for_retrain(
        hook,
        rules_id=rules_id,
        series_id=series_id,
        train_kind=SNAPSHOT_KIND_TRAIN,
    )


class ScopeSpec(NamedTuple):
    scope: str
    track_id: int | None
    track_type_id: int | None


def _distinct_series_ids(hook: NascarMySqlHook, rules_id: int) -> list[int]:
    rows = hook.get_records(
        """
        SELECT DISTINCT series_id
        FROM nascar.ml_training_snapshots
        WHERE kind = %s
          AND dfs_scoring_rules_id = %s
          AND deleted_at IS NULL
        ORDER BY series_id ASC
        """,
        parameters=(SNAPSHOT_KIND_TRAIN, rules_id),
    )
    return [int(r[0]) for r in rows]


def _track_meets_threshold(
    hook: NascarMySqlHook,
    rules_id: int,
    series_id: int,
    track_id: int,
    min_weeks: int,
) -> bool:
    rows = hook.get_records(
        """
        SELECT 1
        FROM nascar.ml_training_snapshots s
        WHERE s.kind = %s
          AND s.dfs_scoring_rules_id = %s
          AND s.series_id = %s
          AND s.track_id = %s
          AND s.deleted_at IS NULL
        GROUP BY s.track_id
        HAVING COUNT(DISTINCT s.week_id) >= %s
        LIMIT 1
        """,
        parameters=(SNAPSHOT_KIND_TRAIN, rules_id, series_id, track_id, min_weeks),
    )
    return bool(rows)


def _track_type_meets_threshold(
    hook: NascarMySqlHook,
    rules_id: int,
    series_id: int,
    track_type_id: int,
    min_weeks: int,
) -> bool:
    rows = hook.get_records(
        """
        SELECT 1
        FROM nascar.ml_training_snapshots s
        JOIN (
            SELECT track_id, MIN(track_type_id) AS track_type_id
            FROM nascar.tracks_track_types
            WHERE deleted_at IS NULL
            GROUP BY track_id
        ) tt ON tt.track_id = s.track_id
        WHERE s.kind = %s
          AND s.dfs_scoring_rules_id = %s
          AND s.series_id = %s
          AND s.deleted_at IS NULL
          AND tt.track_type_id = %s
        GROUP BY tt.track_type_id
        HAVING COUNT(DISTINCT s.week_id) >= %s
        LIMIT 1
        """,
        parameters=(SNAPSHOT_KIND_TRAIN, rules_id, series_id, track_type_id, min_weeks),
    )
    return bool(rows)


def _scope_covered_week(
    hook: NascarMySqlHook,
    rules_id: int,
    series_id: int,
    scope: str,
    *,
    track_id: int | None = None,
    track_type_id: int | None = None,
) -> int | None:
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
        SELECT MAX(s.week_id)
        FROM nascar.ml_model_training_snapshots mts
        JOIN nascar.ml_training_snapshots s
            ON s.id = mts.ml_training_snapshot_id AND s.deleted_at IS NULL
        JOIN nascar.ml_models m
            ON m.id = mts.ml_model_id AND m.deleted_at IS NULL
        WHERE s.kind = %s
          AND s.dfs_scoring_rules_id = %s
          AND s.series_id = %s
          {scope_clause}
        """,
        parameters=(SNAPSHOT_KIND_TRAIN, rules_id, series_id, *scope_params),
    )
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])


def _scope_model_exists(
    hook: NascarMySqlHook,
    *,
    series_id: int,
    scope: str,
    track_id: int | None,
    track_type_id: int | None,
    target: str = PRIMARY_TARGET,
) -> bool:
    if scope == SCOPE_TRACK:
        if track_id is None:
            return False
        scope_clause = "AND m.model_scope = %s AND m.track_id = %s"
        scope_params: tuple[Any, ...] = (scope, int(track_id))
    elif scope == SCOPE_TRACK_TYPE:
        if track_type_id is None:
            return False
        scope_clause = "AND m.model_scope = %s AND m.track_type_id = %s"
        scope_params = (scope, int(track_type_id))
    elif scope == SCOPE_GLOBAL:
        scope_clause = "AND m.model_scope = %s"
        scope_params = (scope,)
    else:
        raise ValueError(f"unknown model_scope: {scope!r}")

    rows = hook.get_records(
        f"""
        SELECT 1
        FROM nascar.ml_models m
        JOIN nascar.ml_feature_sets fs
            ON fs.id = m.ml_feature_set_id AND fs.deleted_at IS NULL
        WHERE m.deleted_at IS NULL
          AND m.series_id = %s
          AND m.artifact_uri IS NOT NULL
          AND JSON_CONTAINS(fs.target_keys, JSON_QUOTE(%s))
          AND JSON_LENGTH(fs.target_keys) = 1
          {scope_clause}
        LIMIT 1
        """,
        parameters=(int(series_id), target, *scope_params),
    )
    return bool(rows)


def _predict_routing_chain(
    hook: NascarMySqlHook,
    rules_id: int,
    series_id: int,
    ctx: SeriesCurrentWeekTrackContext,
    *,
    track_threshold: int,
    type_threshold: int,
    enable_global: bool,
) -> list[ScopeSpec]:
    chain: list[ScopeSpec] = []
    if _track_meets_threshold(
        hook, rules_id, series_id, ctx.track_id, track_threshold
    ):
        chain.append(ScopeSpec(SCOPE_TRACK, ctx.track_id, None))
    if ctx.track_type_id is not None and _track_type_meets_threshold(
        hook, rules_id, series_id, ctx.track_type_id, type_threshold
    ):
        chain.append(ScopeSpec(SCOPE_TRACK_TYPE, None, ctx.track_type_id))
    if enable_global:
        chain.append(ScopeSpec(SCOPE_GLOBAL, None, None))
    return chain


def _predict_ready(
    hook: NascarMySqlHook,
    rules_id: int,
    series_id: int,
    chain: list[ScopeSpec],
    latest_train_week: int,
) -> bool:
    if not chain:
        return False
    first = chain[0]
    if not _scope_model_exists(
        hook,
        series_id=series_id,
        scope=first.scope,
        track_id=first.track_id,
        track_type_id=first.track_type_id,
    ):
        return False
    covered = _scope_covered_week(
        hook,
        rules_id,
        series_id,
        first.scope,
        track_id=first.track_id,
        track_type_id=first.track_type_id,
    )
    return covered is not None and covered >= latest_train_week


def _scopes_needing_refresh(
    hook: NascarMySqlHook,
    rules_id: int,
    series_id: int,
    chain: list[ScopeSpec],
    latest_train_week: int,
) -> list[ScopeSpec]:
    """Return the first scope in the predict chain that is missing or stale."""
    for spec in chain:
        exists = _scope_model_exists(
            hook,
            series_id=series_id,
            scope=spec.scope,
            track_id=spec.track_id,
            track_type_id=spec.track_type_id,
        )
        covered = _scope_covered_week(
            hook,
            rules_id,
            series_id,
            spec.scope,
            track_id=spec.track_id,
            track_type_id=spec.track_type_id,
        )
        if not exists or covered is None or covered < latest_train_week:
            return [spec]
    return []


def _feature_keys_from_snapshot_metadata(
    hook: NascarMySqlHook,
    rules_id: int,
) -> list[str]:
    rows = hook.get_records(
        """
        SELECT feature_keys
        FROM nascar.ml_training_snapshots
        WHERE kind = %s
          AND dfs_scoring_rules_id = %s
          AND deleted_at IS NULL
        ORDER BY week_id DESC, id DESC
        LIMIT 1
        """,
        parameters=(SNAPSHOT_KIND_TRAIN, rules_id),
    )
    if not rows or rows[0][0] is None:
        return []
    raw = rows[0][0]
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    keys = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(keys, list):
        return []
    return sorted(str(k) for k in keys)


def _insert_feature_set(
    hook: NascarMySqlHook,
    *,
    target: str,
    snapshot_id: int | None,
    selected_features: list[str],
    boruta_metadata: dict[str, Any],
) -> int:
    return hook.run_insert_returning_id(
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
    return hook.run_insert_returning_id(
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

    if resolve_series_current_week(hook, series_id=NascarSeries.CUP.series_id) is None:
        _LOG.info("train_models_if_needed_task: no current week in v_series_current_week")
        return {"skipped": "no_current_week"}

    latest_train_week = _latest_train_week_for_retrain_gate(hook, rules_id)
    if latest_train_week is None:
        _LOG.info("train_models_if_needed_task: no train snapshots yet for rules_id=%s", rules_id)
        return {"skipped": "no_training_snapshots"}

    series_ids = _distinct_series_ids(hook, rules_id)
    if not series_ids:
        return {"skipped": "no_training_snapshots"}

    all_feature_keys = _feature_keys_from_snapshot_metadata(hook, rules_id)
    if not all_feature_keys:
        _LOG.warning(
            "train_models_if_needed_task: no feature_keys in snapshot metadata for rules_id=%s",
            rules_id,
        )
        return {"skipped": "no_feature_keys"}

    artifact_root = _artifact_root()

    holdout_weeks_env = os.environ.get("FS_ML_HOLDOUT_WEEKS")
    try:
        holdout_weeks = max(0, int(holdout_weeks_env)) if holdout_weeks_env else 1
    except ValueError:
        holdout_weeks = 1

    track_threshold = min_track_weeks()
    type_threshold = min_track_type_weeks()
    enable_global = global_fallback_enabled()
    global_week_cap = max_train_weeks() if enable_global else 0

    scope_results: list[dict[str, Any]] = []
    series_contexts: list[dict[str, Any]] = []
    any_eligible_chain = False
    all_series_predict_ready = True

    for series_id in series_ids:
        ctx = resolve_current_week_track_context(hook, series_id=series_id)
        if ctx is None:
            all_series_predict_ready = False
            continue

        series_latest = latest_train_week_for_retrain(
            hook,
            rules_id=rules_id,
            series_id=series_id,
            train_kind=SNAPSHOT_KIND_TRAIN,
        )
        if series_latest is None:
            all_series_predict_ready = False
            continue

        chain = _predict_routing_chain(
            hook,
            rules_id,
            series_id,
            ctx,
            track_threshold=track_threshold,
            type_threshold=type_threshold,
            enable_global=enable_global,
        )
        if not chain:
            all_series_predict_ready = False
            continue

        any_eligible_chain = True
        chain_labels = [
            {
                "scope": s.scope,
                "track_id": s.track_id,
                "track_type_id": s.track_type_id,
            }
            for s in chain
        ]
        series_contexts.append(
            {
                "series_id": series_id,
                "current_track_id": ctx.track_id,
                "current_track_type_id": ctx.track_type_id,
                "predict_routing_chain": chain_labels,
            }
        )

        if _predict_ready(hook, rules_id, series_id, chain, series_latest):
            _LOG.info(
                "train_models_if_needed_task: series_id=%s predict-ready through week %s",
                series_id,
                series_latest,
            )
            continue

        all_series_predict_ready = False
        scopes_to_train = _scopes_needing_refresh(
            hook, rules_id, series_id, chain, series_latest
        )
        for spec in scopes_to_train:
            if spec.scope == SCOPE_TRACK:
                feature_keys = features_for_scope(all_feature_keys, scope=SCOPE_TRACK)
                load_kw: dict[str, Any] = {
                    "dfs_scoring_rules_id": rules_id,
                    "series_id": series_id,
                    "track_id": spec.track_id,
                    "mysql_conn_id": mysql_conn_id,
                }
            elif spec.scope == SCOPE_TRACK_TYPE:
                feature_keys = features_for_scope(
                    all_feature_keys,
                    scope=SCOPE_TRACK_TYPE,
                    track_type_id=spec.track_type_id,
                )
                load_kw = {
                    "dfs_scoring_rules_id": rules_id,
                    "series_id": series_id,
                    "track_type_id": spec.track_type_id,
                    "mysql_conn_id": mysql_conn_id,
                }
            else:
                feature_keys = features_for_scope(all_feature_keys, scope=SCOPE_GLOBAL)
                load_kw = {
                    "dfs_scoring_rules_id": rules_id,
                    "series_id": series_id,
                    "mysql_conn_id": mysql_conn_id,
                }
                if global_week_cap > 0:
                    load_kw["max_weeks"] = global_week_cap

            if not feature_keys:
                continue
            rows = load_training_frame(**load_kw)
            if not rows:
                continue
            scope_results.append(
                _train_scope(
                    series_id=series_id,
                    scope=spec.scope,
                    rows=rows,
                    track_id=spec.track_id,
                    track_type_id=spec.track_type_id,
                    feature_keys=feature_keys,
                    holdout_weeks=holdout_weeks,
                    artifact_root=artifact_root,
                    hook=hook,
                    scoring_rules=scoring_rules,
                )
            )
            del rows

    if not any_eligible_chain:
        _LOG.info("train_models_if_needed_task: no eligible predict routing chain")
        return {"skipped": "no_eligible_scopes"}

    if all_series_predict_ready and not scope_results:
        _LOG.info(
            "train_models_if_needed_task: predict-ready; latest=%s",
            latest_train_week,
        )
        return {
            "skipped": "predict_ready",
            "latest_train_week": latest_train_week,
        }

    if not scope_results:
        _LOG.info(
            "train_models_if_needed_task: nothing to train after refresh check; latest=%s",
            latest_train_week,
        )
        return {
            "skipped": "no_scopes_to_train",
            "latest_train_week": latest_train_week,
            "series_contexts": series_contexts,
        }

    _LOG.info(
        "train_models_if_needed_task: latest_week=%s scopes_trained=%s",
        latest_train_week,
        len(scope_results),
    )
    return {
        "skipped": None,
        "latest_train_week": latest_train_week,
        "scope_results": scope_results,
        "series_contexts": series_contexts,
        "available_scopes": list(ALL_SCOPES),
    }
