"""Walk-forward projection backtest on finished train snapshots."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import numpy as np

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.ml.model_scope import (
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
from include.nascar.ml.feature_matrix import build_feature_matrix
from include.nascar.ml.scoring import compute_dfs_points_from_targets
from include.nascar.ml.xgb_train_pipeline import (
    DEFAULT_TARGETS,
    holdout_dfs_eval_metrics,
    train_models_for_targets,
)

_LOG = logging.getLogger(__name__)


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


def _parse_feature_keys(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    keys = json.loads(raw) if isinstance(raw, str) else raw
    return sorted(str(k) for k in keys) if isinstance(keys, list) else []


def _holdout_weeks_env() -> int:
    raw = os.environ.get("FS_ML_HOLDOUT_WEEKS", "1")
    try:
        return max(0, int(raw))
    except ValueError:
        return 1


def _backtest_max_weeks_env() -> int:
    raw = os.environ.get("FS_ML_BACKTEST_MAX_WEEKS", "12")
    try:
        return max(1, int(raw))
    except ValueError:
        return 12


def _train_rows_for_eval_week(
    frame: list[dict[str, Any]],
    *,
    eval_week_id: int,
    track_id: int,
    track_type_id: int | None,
    feature_keys: list[str],
    rules_id: int,
    series_id: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    train_rows = [row for row in frame if int(row["week_id"]) < eval_week_id]
    if not train_rows:
        return [], []

    distinct_weeks = {int(row["week_id"]) for row in train_rows}
    scope = SCOPE_GLOBAL
    scoped_keys = features_for_scope(feature_keys, scope=SCOPE_GLOBAL)
    if track_id is not None and len(distinct_weeks) >= min_track_weeks():
        weeks_at_track = {
            int(row["week_id"])
            for row in train_rows
            if row.get("track_id") is not None and int(row["track_id"]) == track_id
        }
        if len(weeks_at_track) >= min_track_weeks():
            scope = SCOPE_TRACK
            scoped_keys = features_for_scope(feature_keys, scope=SCOPE_TRACK)
    if (
        scope != SCOPE_TRACK
        and track_type_id is not None
        and len(distinct_weeks) >= min_track_type_weeks()
    ):
        weeks_at_type = {
            int(row["week_id"])
            for row in train_rows
            if row.get("track_type_id") is not None
            and int(row["track_type_id"]) == track_type_id
        }
        if len(weeks_at_type) >= min_track_type_weeks():
            scope = SCOPE_TRACK_TYPE
            scoped_keys = features_for_scope(
                feature_keys, scope=SCOPE_TRACK_TYPE, track_type_id=track_type_id
            )

    if scope == SCOPE_TRACK:
        train_rows = [
            row
            for row in train_rows
            if row.get("track_id") is not None and int(row["track_id"]) == track_id
        ]
    elif scope == SCOPE_TRACK_TYPE:
        train_rows = [
            row
            for row in train_rows
            if row.get("track_type_id") is not None
            and int(row["track_type_id"]) == track_type_id
        ]

    cap = max_train_weeks() if global_fallback_enabled() and scope == SCOPE_GLOBAL else 0
    if cap > 0:
        keep_weeks = sorted(distinct_weeks)[-cap:]
        train_rows = [row for row in train_rows if int(row["week_id"]) in keep_weeks]

    return train_rows, scoped_keys


def run_walk_forward_backtest(
    hook: NascarMySqlHook | None = None,
    *,
    dfs_id: int = DRAFTKINGS_DFS_ID,
    series_id: int = 1,
    mysql_conn_id: str = "nascar_conn",
) -> dict[str, Any]:
    """
    Leave-one-recent-week-out backtest: train on prior weeks, score the held-out week.

    Uses the same scoped routing heuristic as production train/predict.
    """
    hook = hook or NascarMySqlHook(mysql_conn_id=mysql_conn_id)
    rules = _active_rules(hook, dfs_id)
    if rules is None:
        return {"skipped": "no_active_rules"}

    rules_id, scoring_rules = rules
    frame = load_training_frame(
        dfs_scoring_rules_id=rules_id,
        series_id=series_id,
        mysql_conn_id=mysql_conn_id,
    )
    if not frame:
        return {"skipped": "no_training_frame"}

    key_rows = hook.get_records(
        """
        SELECT feature_keys
        FROM nascar.ml_training_snapshots
        WHERE kind = %s AND dfs_scoring_rules_id = %s AND series_id = %s AND deleted_at IS NULL
        ORDER BY week_id DESC, id DESC
        LIMIT 1
        """,
        parameters=(SNAPSHOT_KIND_TRAIN, rules_id, series_id),
    )
    feature_keys = _parse_feature_keys(key_rows[0][0]) if key_rows else []

    week_ids = sorted({int(row["week_id"]) for row in frame})
    max_weeks = _backtest_max_weeks_env()
    eval_weeks = week_ids[-max_weeks:] if len(week_ids) > max_weeks else week_ids

    holdout_weeks = _holdout_weeks_env()
    per_week: list[dict[str, Any]] = []
    all_pred: list[float] = []
    all_obs: list[float] = []

    for eval_week_id in eval_weeks:
        eval_rows = [row for row in frame if int(row["week_id"]) == eval_week_id]
        if not eval_rows:
            continue
        track_id = int(eval_rows[0]["track_id"])
        track_type_id = (
            int(eval_rows[0]["track_type_id"])
            if eval_rows[0].get("track_type_id") is not None
            else None
        )
        train_rows, scoped_keys = _train_rows_for_eval_week(
            frame,
            eval_week_id=eval_week_id,
            track_id=track_id,
            track_type_id=track_type_id,
            feature_keys=feature_keys,
            rules_id=rules_id,
            series_id=series_id,
        )
        if len(train_rows) < 10 or not scoped_keys:
            per_week.append(
                {"week_id": eval_week_id, "skipped": True, "reason": "insufficient_train_rows"}
            )
            continue

        trained = train_models_for_targets(
            train_rows,
            holdout_weeks=holdout_weeks,
            feature_allowlist=scoped_keys,
        )
        metrics = holdout_dfs_eval_metrics(
            train_rows,
            trained,
            scoring_rules,
            holdout_weeks=holdout_weeks,
        )

        trained_full = train_models_for_targets(
            train_rows,
            holdout_weeks=0,
            feature_allowlist=scoped_keys,
        )
        models_by_target = {
            entry.target: entry for entry in trained_full if entry.model is not None
        }
        if len(models_by_target) < len(DEFAULT_TARGETS):
            per_week.append(
                {
                    "week_id": eval_week_id,
                    "skipped": True,
                    "reason": "incomplete_models",
                    "metrics": metrics,
                }
            )
            continue

        week_pred: list[float] = []
        week_obs: list[float] = []
        for row in eval_rows:
            predicted_targets: dict[str, float] = {}
            for target in DEFAULT_TARGETS:
                entry = models_by_target[target]
                X = build_feature_matrix([row], entry.selected_features)
                predicted_targets[target] = float(entry.model.predict(X)[0])
            total, _ = compute_dfs_points_from_targets(
                row.get("r_start_position"),
                predicted_targets,
                scoring_rules,
            )
            week_pred.append(float(total))
            week_obs.append(float(row.get("dfs_points") or 0.0))

        if week_pred:
            all_pred.extend(week_pred)
            all_obs.extend(week_obs)

        per_week.append(
            {
                "week_id": eval_week_id,
                "track_id": track_id,
                "n_eval_rows": len(eval_rows),
                "n_train_rows": len(train_rows),
                "metrics": metrics,
            }
        )

    summary: dict[str, Any] = {"per_week": per_week, "n_weeks": len(per_week)}
    if all_pred and all_obs:
        pred_arr = np.asarray(all_pred, dtype=float)
        obs_arr = np.asarray(all_obs, dtype=float)
        summary["overall_dfs_points_mae"] = float(np.mean(np.abs(pred_arr - obs_arr)))
        summary["overall_dfs_points_rmse"] = float(
            np.sqrt(np.mean((pred_arr - obs_arr) ** 2))
        )
        if len(obs_arr) >= 2 and np.std(obs_arr) > 0 and np.std(pred_arr) > 0:
            pred_rank = np.argsort(np.argsort(pred_arr))
            obs_rank = np.argsort(np.argsort(obs_arr))
            summary["overall_dfs_points_spearman"] = float(
                np.corrcoef(pred_rank, obs_rank)[0, 1]
            )
        summary["n_predictions"] = len(all_pred)

    _LOG.info("run_walk_forward_backtest: summary=%s", summary)
    return summary
