"""Walk-forward projection backtest on finished train snapshots."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import numpy as np

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.ml.dnf import (
    DNF_TARGET_KEY,
    MODEL_KIND_DNF_CLASSIFIER,
    compose_two_stage_primary,
    compose_two_stage_secondary,
)
from include.nascar.ml.feature_matrix import build_feature_matrix
from include.nascar.ml.model_scope import (
    SCOPE_TRACK_TYPE,
    features_for_scope,
    min_cohort_weeks,
)
from include.nascar.ml.scoring import (
    compute_dfs_points_from_targets,
    target_keys_for_rules,
)
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID, SNAPSHOT_KIND_TRAIN
from include.nascar.ml.training_targets import load_training_frame
from include.nascar.ml.xgb_train_pipeline import (
    DEFAULT_TARGETS,
    PRIMARY_TARGET,
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
    """Module-local alias for :func:`include.nascar.ml.model_scope.holdout_weeks`."""
    from include.nascar.ml.model_scope import holdout_weeks

    return holdout_weeks()


def _dev_weeks_env() -> int:
    """Module-local alias for :func:`include.nascar.ml.model_scope.dev_weeks`."""
    from include.nascar.ml.model_scope import dev_weeks

    return dev_weeks()


def _backtest_max_weeks_env() -> int:
    raw = os.environ.get("FS_ML_BACKTEST_MAX_WEEKS", "12")
    try:
        return max(1, int(raw))
    except ValueError:
        return 12


def _sibling_track_ids_for_type(
    hook: NascarMySqlHook,
    *,
    track_type_id: int,
) -> set[int]:
    rows = hook.get_records(
        """
        SELECT DISTINCT track_id
        FROM nascar.tracks_track_types
        WHERE track_type_id = %s
          AND deleted_at IS NULL
        """,
        parameters=(int(track_type_id),),
    )
    return {int(r[0]) for r in rows}


def _train_rows_for_eval_week(
    frame: list[dict[str, Any]],
    *,
    eval_week_id: int,
    track_type_id: int | None,
    sibling_track_ids: set[int] | None,
    feature_keys: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Build the cohort training slice for a backtest eval week.

    Restricts rows to ``week_id < eval_week_id`` and (when ``track_type_id`` is
    known) to sibling tracks of that type. Returns the per-cohort feature
    allowlist (``*_pt_*`` + anchors + other-type ``_tt_`` aggregates).
    """
    train_rows = [row for row in frame if int(row["week_id"]) < eval_week_id]
    if not train_rows or track_type_id is None or not sibling_track_ids:
        return [], []

    train_rows = [
        row
        for row in train_rows
        if row.get("track_id") is not None and int(row["track_id"]) in sibling_track_ids
    ]
    distinct_weeks = {int(row["week_id"]) for row in train_rows}
    if len(distinct_weeks) < min_cohort_weeks():
        return [], []

    # C3: feed the cohort's ``track_type_id`` into ``features_for_scope`` so
    # only the cohort's own ``_tt_<track_type_id>_*`` keys get stripped;
    # other-type ``_tt_`` aggregates are preserved as cross-type signal.
    scoped_keys = features_for_scope(
        feature_keys,
        scope=SCOPE_TRACK_TYPE,
        track_type_id=int(track_type_id),
    )
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
    dev_weeks = _dev_weeks_env()
    derived = target_keys_for_rules(scoring_rules)
    targets: tuple[str, ...] = tuple(derived) if derived else tuple(DEFAULT_TARGETS)
    if PRIMARY_TARGET not in targets:
        _LOG.warning(
            "run_walk_forward_backtest: PRIMARY_TARGET=%s missing from derived targets=%s; "
            "falling back to DEFAULT_TARGETS",
            PRIMARY_TARGET,
            list(targets),
        )
        targets = tuple(DEFAULT_TARGETS)
    # D3: include the binary DNF head so the backtest exercises the same
    # two-stage pipeline as production. When the loaded frame predates D3
    # (no ``r_is_dnf`` in label rows), ``train_models_for_targets`` warns
    # and skips the head — the backtest collapses to legacy single-stage.
    if DNF_TARGET_KEY not in targets:
        targets = (*targets, DNF_TARGET_KEY)
    per_week: list[dict[str, Any]] = []
    all_pred: list[float] = []
    all_obs: list[float] = []

    sibling_cache: dict[int, set[int]] = {}

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
        if track_type_id is not None and track_type_id not in sibling_cache:
            sibling_cache[track_type_id] = _sibling_track_ids_for_type(
                hook,
                track_type_id=track_type_id,
            )
        sibling_track_ids = (
            sibling_cache.get(track_type_id) if track_type_id is not None else None
        )
        train_rows, scoped_keys = _train_rows_for_eval_week(
            frame,
            eval_week_id=eval_week_id,
            track_type_id=track_type_id,
            sibling_track_ids=sibling_track_ids,
            feature_keys=feature_keys,
        )
        if len(train_rows) < 10 or not scoped_keys:
            per_week.append(
                {"week_id": eval_week_id, "skipped": True, "reason": "insufficient_train_rows"}
            )
            continue

        trained = train_models_for_targets(
            train_rows,
            targets=targets,
            holdout_weeks=holdout_weeks,
            dev_weeks=dev_weeks,
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
            targets=targets,
            holdout_weeks=0,
            dev_weeks=0,
            feature_allowlist=scoped_keys,
        )
        models_by_target = {
            entry.target: entry for entry in trained_full if entry.model is not None
        }
        # D3: it's normal for ``r_is_dnf`` to be absent when the frame
        # predates the two-stage rollout — the trainer logs a warning and
        # skips it. Only count regression heads when checking for
        # incomplete coverage.
        regression_targets = tuple(t for t in targets if t != DNF_TARGET_KEY)
        missing_regression = [
            t for t in regression_targets if t not in models_by_target
        ]
        if missing_regression:
            per_week.append(
                {
                    "week_id": eval_week_id,
                    "skipped": True,
                    "reason": "incomplete_models",
                    "metrics": metrics,
                }
            )
            continue
        dnf_entry = models_by_target.get(DNF_TARGET_KEY)

        week_pred: list[float] = []
        week_obs: list[float] = []
        eval_X_by_target: dict[str, np.ndarray] = {}
        # Predict each head against the entire eval-week field at once so the
        # D3 composer sees per-driver vectors aligned with ``len(eval_rows)``.
        # XGBoost's predict-per-row in a Python loop was correct but the
        # vectorised call also makes the composition arithmetic obvious.
        for target in regression_targets:
            entry = models_by_target[target]
            X = build_feature_matrix(
                eval_rows, entry.selected_features, strict=False
            )
            eval_X_by_target[target] = entry.model.predict(X)
        if dnf_entry is not None:
            X_dnf = build_feature_matrix(
                eval_rows, dnf_entry.selected_features, strict=False
            )
            dnf_proba = np.clip(
                np.asarray(dnf_entry.model.predict_proba(X_dnf))[:, 1],
                0.0,
                1.0,
            )
        else:
            dnf_proba = None

        # D3: compose marginal expectations using the eval-week field size
        # so DNF rows collapse to the back of *that* race's field. The
        # composers are no-ops when ``dnf_proba`` is ``None`` (legacy
        # single-head frames), matching production predict behavior.
        field_size = len(eval_rows)
        composed_predictions: dict[str, np.ndarray] = {}
        for target, clean in eval_X_by_target.items():
            if target == PRIMARY_TARGET:
                composed_predictions[target] = compose_two_stage_primary(
                    clean, dnf_proba, field_size
                )
            else:
                composed_predictions[target] = compose_two_stage_secondary(
                    clean, dnf_proba
                )

        for i, row in enumerate(eval_rows):
            predicted_targets = {
                target: float(preds[i])
                for target, preds in composed_predictions.items()
            }
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
