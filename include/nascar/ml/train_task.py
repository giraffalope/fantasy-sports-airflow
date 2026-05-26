"""Airflow callable: train cohort XGBoost regressors for the current week's track types.

For each ``track_type_id`` mapped to the current week's track in
``tracks_track_types``, this task fits one ``ml_models`` row per
``(series_id, track_type_id, target)`` on the union of train snapshots at
every sibling track sharing that type. Predict-time then blends those cohort
models per ``FS_ML_BLEND_WEIGHTS``.

No ``track`` / ``global`` artifacts are produced; only ``model_scope='track_type'``
rows are written.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import traceback
import uuid
from pathlib import Path
from typing import Any, NamedTuple

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.cup.current_week import (
    latest_train_week_for_retrain,
    resolve_current_week_track_context,
    resolve_series_current_week,
)
from include.nascar.enums import NascarSeries
from include.nascar.ml.model_scope import (
    ALL_SCOPES,
    SCOPE_TRACK_TYPE,
    blend_weight_strategy,
    cohort_distinct_weeks,
    features_for_scope,
    min_cohort_weeks,
    sibling_track_ids_for_type,
    track_type_ids_for_track,
)
from include.nascar.ml.scoring import target_keys_for_rules
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID, SNAPSHOT_KIND_TRAIN
from include.nascar.ml.training_targets import load_training_frame
from include.nascar.ml.xgb_train_pipeline import (
    DEFAULT_TARGETS,
    PRIMARY_TARGET,
    TrainedTargetModel,
    holdout_dfs_eval_metrics,
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


class CohortSpec(NamedTuple):
    series_id: int
    track_type_id: int


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


def _cohort_covered_week(
    hook: NascarMySqlHook,
    rules_id: int,
    *,
    series_id: int,
    track_type_id: int,
    target: str = PRIMARY_TARGET,
) -> int | None:
    rows = hook.get_records(
        """
        SELECT MAX(s.week_id)
        FROM nascar.ml_model_training_snapshots mts
        JOIN nascar.ml_training_snapshots s
            ON s.id = mts.ml_training_snapshot_id AND s.deleted_at IS NULL
        JOIN nascar.ml_models m
            ON m.id = mts.ml_model_id
           AND m.deleted_at IS NULL
        JOIN nascar.ml_feature_sets fs
            ON fs.id = m.ml_feature_set_id AND fs.deleted_at IS NULL
        WHERE s.kind = %s
          AND s.dfs_scoring_rules_id = %s
          AND s.series_id = %s
          AND m.model_scope = %s
          AND m.track_type_id = %s
          AND m.artifact_uri IS NOT NULL
          AND JSON_CONTAINS(fs.target_keys, JSON_QUOTE(%s))
          AND JSON_LENGTH(fs.target_keys) = 1
        """,
        parameters=(
            SNAPSHOT_KIND_TRAIN,
            rules_id,
            series_id,
            SCOPE_TRACK_TYPE,
            track_type_id,
            target,
        ),
    )
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])


def _soft_delete_prior_cohort_models(
    hook: NascarMySqlHook,
    *,
    series_id: int,
    track_type_id: int,
    target: str,
) -> None:
    """Soft-delete any active ``(series_id, track_type, track_type_id, target)`` rows."""
    hook.run(
        """
        UPDATE nascar.ml_models m
        JOIN nascar.ml_feature_sets fs
            ON fs.id = m.ml_feature_set_id
        SET m.deleted_at = UTC_TIMESTAMP()
        WHERE m.deleted_at IS NULL
          AND m.series_id = %s
          AND m.model_scope = %s
          AND m.track_type_id = %s
          AND JSON_CONTAINS(fs.target_keys, JSON_QUOTE(%s))
          AND JSON_LENGTH(fs.target_keys) = 1
        """,
        parameters=(int(series_id), SCOPE_TRACK_TYPE, int(track_type_id), target),
    )


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
    track_type_id: int,
    cohort_n_weeks: int,
    train_target_mean: float | None = None,
    algorithm: str = "xgboost",
    model_kind: str = "regression",
    holdout_residual_quantiles: dict[str, float] | None = None,
) -> int:
    return hook.run_insert_returning_id(
        """
        INSERT INTO nascar.ml_models (
            ml_feature_set_id,
            algorithm,
            model_kind,
            series_id,
            model_scope,
            track_id,
            track_type_id,
            hyperparams_json,
            artifact_uri,
            cohort_n_weeks,
            train_target_mean,
            holdout_residual_quantiles
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        parameters=(
            feature_set_id,
            algorithm,
            model_kind,
            int(series_id),
            SCOPE_TRACK_TYPE,
            None,
            int(track_type_id),
            json.dumps(hyperparams),
            artifact_uri,
            int(cohort_n_weeks),
            (
                None
                if train_target_mean is None
                else round(float(train_target_mean), 6)
            ),
            (
                None
                if holdout_residual_quantiles is None
                else json.dumps(holdout_residual_quantiles)
            ),
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
    eval_type: str = "cohort",
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


def _holdout_weeks_env() -> int:
    """Module-local alias for :func:`include.nascar.ml.model_scope.holdout_weeks`."""
    from include.nascar.ml.model_scope import holdout_weeks

    return holdout_weeks()


def _dev_weeks_env() -> int:
    """Module-local alias for :func:`include.nascar.ml.model_scope.dev_weeks`."""
    from include.nascar.ml.model_scope import dev_weeks

    return dev_weeks()


def _small_cohort(cohort_n_weeks: int) -> bool:
    """Cohorts with fewer than ``2 * FS_ML_TUNE_CV_SPLITS`` distinct weeks tune adaptively."""
    raw = os.environ.get("FS_ML_TUNE_CV_SPLITS")
    try:
        cv_splits = max(2, int(raw)) if raw else 4
    except ValueError:
        cv_splits = 4
    return cohort_n_weeks < (cv_splits * 2)


_BORUTA_MAX_ITER_DEFAULT = 50
_BORUTA_SMALL_COHORT_MAX_ITER_DEFAULT = 10


def _resolve_boruta_max_iter(*, small_cohort: bool) -> int:
    """Phase 4: per-cohort Boruta iteration cap, env-driven.

    Reads ``FS_ML_BORUTA_SMALL_COHORT_MAX_ITER`` (default 10) for small
    cohorts (per :func:`_small_cohort`) and ``FS_ML_BORUTA_MAX_ITER``
    (default 50, but the compose worker lowers it to 30 as part of the
    OOM mitigation) otherwise. Negative or non-int values fall back to
    the legacy hard-coded defaults so a misconfigured env can't push
    Boruta into an infinite loop.
    """
    env_var = (
        "FS_ML_BORUTA_SMALL_COHORT_MAX_ITER" if small_cohort else "FS_ML_BORUTA_MAX_ITER"
    )
    default = (
        _BORUTA_SMALL_COHORT_MAX_ITER_DEFAULT
        if small_cohort
        else _BORUTA_MAX_ITER_DEFAULT
    )
    raw = os.environ.get(env_var)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        _LOG.warning(
            "Ignoring non-int %s=%r; using default %s", env_var, raw, default
        )
        return default
    if value < 1:
        _LOG.warning(
            "Ignoring non-positive %s=%r; using default %s", env_var, raw, default
        )
        return default
    return value


def _resolve_train_targets(scoring_rules: dict[str, Any]) -> tuple[str, ...]:
    """Targets to fit per cohort, derived from the active scoring rules.

    Falls back to ``DEFAULT_TARGETS`` when the rules don't yield any trainable
    keys (e.g. malformed JSON, brand-new ruleset before the components list is
    populated). Logs a warning when ``PRIMARY_TARGET`` is absent from the
    derived list — the primary head is what predict/Monte Carlo couple to, so
    silently dropping it would break downstream consumers.

    D3: appends the binary DNF target ``r_is_dnf`` to the returned tuple so
    the trainer fits the two-stage classifier head alongside the
    regression heads. The DNF target is not a scoring-rule source key,
    so :func:`target_keys_for_rules` never returns it; appending here
    keeps that derivation a single source of truth.
    """
    from include.nascar.ml.dnf import DNF_TARGET_KEY

    derived = target_keys_for_rules(scoring_rules)
    targets = tuple(derived) if derived else tuple(DEFAULT_TARGETS)
    if PRIMARY_TARGET not in targets:
        _LOG.warning(
            "_resolve_train_targets: PRIMARY_TARGET=%s missing from derived targets=%s; "
            "falling back to DEFAULT_TARGETS",
            PRIMARY_TARGET,
            list(targets),
        )
        targets = tuple(DEFAULT_TARGETS)
    if DNF_TARGET_KEY not in targets:
        targets = (*targets, DNF_TARGET_KEY)
    return targets


def _persist_trained_model(
    hook: NascarMySqlHook,
    *,
    entry: TrainedTargetModel,
    snapshot_ids: list[int],
    artifact_root: Path,
    series_id: int,
    track_type_id: int,
    cohort_n_weeks: int,
) -> int:
    from include.nascar.ml.dnf import (
        MODEL_KIND_DNF_CLASSIFIER,
    )

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
                "model_scope": SCOPE_TRACK_TYPE,
                "track_id": None,
                "track_type_id": int(track_type_id),
                "cohort_n_weeks": int(cohort_n_weeks),
            },
        },
    )
    hyperparams = {"target": entry.target, **entry.hyperparams}
    # D3: the binary head writes ``algorithm='xgboost_classifier'`` so the
    # column doubles as a coarse audit of the model family even before
    # ``model_kind`` is consulted by predict-time logic.
    is_classifier = entry.model_kind == MODEL_KIND_DNF_CLASSIFIER
    algorithm = "xgboost_classifier" if is_classifier else "xgboost"
    model_id = _insert_model(
        hook,
        feature_set_id=feature_set_id,
        hyperparams=hyperparams,
        artifact_uri=None,
        series_id=series_id,
        track_type_id=track_type_id,
        cohort_n_weeks=cohort_n_weeks,
        # B3: persist the per-target training-set mean so the predict task
        # can fall back to it when a secondary target has no usable cohort
        # model (instead of the hard 0.0 default that biases DFS scores).
        train_target_mean=entry.train_target_mean,
        algorithm=algorithm,
        model_kind=entry.model_kind,
        # E1: persist the conformal-interval store on the cohort model so
        # downstream Monte Carlo / predict-time σ sizing reads a single
        # source of truth per (series_id, track_type_id, target).
        holdout_residual_quantiles=entry.holdout_residual_quantiles,
    )

    if entry.artifact_path is not None:
        final_dir = artifact_root / str(model_id)
        final_dir.mkdir(parents=True, exist_ok=True)
        final_path = final_dir / f"{entry.target}.json"
        entry.artifact_path.replace(final_path)
        entry.artifact_path = final_path
        _update_model_artifact(hook, model_id, str(final_path))

    _insert_model_training_snapshots(hook, model_id=model_id, snapshot_ids=snapshot_ids)

    if is_classifier:
        # D3 classifier: log_loss is the primary fit-quality metric; auc
        # is added only when the holdout slice contained both classes
        # (otherwise it's undefined). ``sample_count`` is the holdout row
        # count so blend strategies can weight contributors consistently.
        if (
            entry.holdout_log_loss is not None
            and entry.holdout_predictions is not None
        ):
            _insert_evaluation(
                hook,
                model_id=model_id,
                metric="log_loss",
                metric_value=entry.holdout_log_loss,
                sample_count=int(entry.holdout_predictions.shape[0]),
                eval_type="cohort",
                eval_key=int(track_type_id),
            )
        if (
            entry.holdout_auc is not None
            and entry.holdout_predictions is not None
        ):
            _insert_evaluation(
                hook,
                model_id=model_id,
                metric="auc",
                metric_value=entry.holdout_auc,
                sample_count=int(entry.holdout_predictions.shape[0]),
                eval_type="cohort",
                eval_key=int(track_type_id),
            )
    elif entry.holdout_mae is not None and entry.holdout_predictions is not None:
        _insert_evaluation(
            hook,
            model_id=model_id,
            metric="mae",
            metric_value=entry.holdout_mae,
            sample_count=int(entry.holdout_predictions.shape[0]),
            eval_type="cohort",
            eval_key=int(track_type_id),
        )

    return model_id


def _snapshot_ids_for_rows(rows: list[dict[str, Any]]) -> list[int]:
    seen: dict[int, None] = {}
    for row in rows:
        snapshot_id = row.get("ml_training_snapshot_id")
        if snapshot_id is not None and int(snapshot_id) not in seen:
            seen[int(snapshot_id)] = None
    return list(seen.keys())


def _train_cohort(
    *,
    series_id: int,
    track_type_id: int,
    track_id_anchor: int,
    rows: list[dict[str, Any]],
    feature_keys: list[str],
    cohort_n_weeks: int,
    holdout_weeks: int,
    dev_weeks: int,
    artifact_root: Path,
    hook: NascarMySqlHook,
    scoring_rules: dict[str, Any],
    targets: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Train one cohort (series_id, track_type_id) and persist per-target models.

    Writes two evaluation rows per primary target:

    - ``eval_type='cohort'`` keyed by ``track_type_id`` on the cohort holdout.
    - ``eval_type='track'`` keyed by the current week's ``track_id`` on the
      cohort holdout filtered to rows from that track (skipped when fewer
      than 3 holdout rows from the anchor track are available).
    """
    snapshot_ids = _snapshot_ids_for_rows(rows)
    if not snapshot_ids:
        return {
            "skipped": "no_snapshots",
            "series_id": series_id,
            "track_type_id": track_type_id,
            "n_rows": 0,
        }

    boruta_skipped = _small_cohort(cohort_n_weeks)
    boruta_max_iter = _resolve_boruta_max_iter(small_cohort=boruta_skipped)
    staging_dir = artifact_root / "_staging" / uuid.uuid4().hex

    if targets is None:
        targets = _resolve_train_targets(scoring_rules)

    # C3: per-cohort feature selection. Each cohort strips only its own
    # ``_tt_<track_type_id>_`` columns and keeps every other type's
    # ``_tt_`` aggregate as cross-type signal.
    cohort_feature_keys = features_for_scope(
        feature_keys,
        scope=SCOPE_TRACK_TYPE,
        track_type_id=int(track_type_id),
    )

    trained = train_models_for_targets(
        rows,
        targets=targets,
        holdout_weeks=holdout_weeks,
        dev_weeks=dev_weeks,
        artifact_dir=staging_dir,
        feature_allowlist=cohort_feature_keys,
        boruta_max_iter=boruta_max_iter,
    )

    if boruta_skipped:
        for entry in trained:
            entry.boruta_metadata = {
                **(entry.boruta_metadata or {}),
                "boruta_skipped": True,
                "reason": "small_cohort",
                "cohort_n_weeks": int(cohort_n_weeks),
            }

    written: dict[str, int] = {}
    persisted: dict[str, TrainedTargetModel] = {}
    primary_model_id: int | None = None
    # A1 atomic-per-target ordering: interleave the soft-delete with each
    # target's persist quartet (feature_set, model, training_snapshots,
    # eval) so a mid-loop exception leaves earlier targets fully replaced
    # and later targets retaining their prior active models. The previous
    # "delete-all-first, then-persist" pattern stranded entire cohorts on
    # any failure (see the eval_type ENUM bug fix earlier).
    for entry in trained:
        _soft_delete_prior_cohort_models(
            hook,
            series_id=series_id,
            track_type_id=track_type_id,
            target=entry.target,
        )
        model_id = _persist_trained_model(
            hook,
            entry=entry,
            snapshot_ids=snapshot_ids,
            artifact_root=artifact_root,
            series_id=series_id,
            track_type_id=track_type_id,
            cohort_n_weeks=cohort_n_weeks,
        )
        written[entry.target] = model_id
        persisted[entry.target] = entry
        if entry.target == PRIMARY_TARGET:
            primary_model_id = model_id

    dfs_mae, dfs_n = predicted_dfs_mae_on_holdout(
        rows,
        trained,
        scoring_rules,
        holdout_weeks=holdout_weeks,
    )
    eval_metrics = holdout_dfs_eval_metrics(
        rows,
        trained,
        scoring_rules,
        holdout_weeks=holdout_weeks,
    )
    if primary_model_id is not None and eval_metrics.get("n"):
        sample_n = int(eval_metrics["n"])
        for metric in ("dfs_points_mae", "dfs_points_rmse", "dfs_points_spearman"):
            value = eval_metrics.get(metric)
            if value is not None:
                _insert_evaluation(
                    hook,
                    model_id=primary_model_id,
                    metric=metric,
                    metric_value=float(value),
                    sample_count=sample_n,
                    eval_type="cohort",
                    eval_key=int(track_type_id),
                )
    elif dfs_mae is not None and primary_model_id is not None:
        _insert_evaluation(
            hook,
            model_id=primary_model_id,
            metric="dfs_points_mae",
            metric_value=dfs_mae,
            sample_count=dfs_n,
            eval_type="cohort",
            eval_key=int(track_type_id),
        )

    track_eval = _holdout_track_eval(
        rows,
        persisted,
        scoring_rules,
        holdout_weeks=holdout_weeks,
        track_id=track_id_anchor,
        targets=targets,
    )
    if (
        primary_model_id is not None
        and track_eval is not None
        and track_eval.get("n") >= 3
    ):
        for metric_name, value in (
            ("mae", track_eval.get("target_mae")),
            ("dfs_points_mae", track_eval.get("dfs_points_mae")),
        ):
            if value is not None:
                _insert_evaluation(
                    hook,
                    model_id=primary_model_id,
                    metric=metric_name,
                    metric_value=float(value),
                    sample_count=int(track_eval["n"]),
                    eval_type="track",
                    eval_key=int(track_id_anchor),
                )

    try:
        staging_dir.rmdir()
    except OSError:
        pass

    _LOG.info(
        "train_cohort series_id=%s track_type_id=%s anchor_track_id=%s n_rows=%s "
        "n_features=%s cohort_n_weeks=%s trained=%s dfs_mae=%s track_eval=%s",
        series_id,
        track_type_id,
        track_id_anchor,
        len(rows),
        len(feature_keys),
        cohort_n_weeks,
        written,
        dfs_mae,
        track_eval,
    )
    return {
        "series_id": int(series_id),
        "scope": SCOPE_TRACK_TYPE,
        "track_type_id": int(track_type_id),
        "anchor_track_id": int(track_id_anchor),
        "cohort_n_weeks": int(cohort_n_weeks),
        "trained_models": written,
        "primary_model_id": primary_model_id,
        "dfs_points_mae": dfs_mae,
        "holdout_eval": eval_metrics,
        "track_eval": track_eval,
        "boruta_skipped": boruta_skipped,
        "n_rows": len(rows),
    }


def _holdout_track_eval(
    rows: list[dict[str, Any]],
    persisted: dict[str, TrainedTargetModel],
    scoring_rules: dict[str, Any],
    *,
    holdout_weeks: int,
    track_id: int,
    targets: tuple[str, ...] | None = None,
) -> dict[str, Any] | None:
    """Per-track holdout slice: restrict cohort holdout rows to ``track_id`` only.

    Returns ``None`` when no per-target predictions are available or the slice
    is empty. The trainer caller decides whether to persist the slice
    (requires ``n >= 3`` to be informative).
    """
    import numpy as np

    from include.nascar.ml.dnf import DNF_TARGET_KEY
    from include.nascar.ml.scoring import compute_dfs_points_from_targets
    from include.nascar.ml.xgb_train_pipeline import (
        _two_stage_holdout_predictions,
        split_holdout_mask,
    )

    _train_mask, holdout_mask = split_holdout_mask(rows, holdout_weeks=holdout_weeks)
    holdout_indices_all = np.flatnonzero(holdout_mask)
    indices = [
        int(i)
        for i in holdout_indices_all
        if rows[i].get("track_id") is not None and int(rows[i]["track_id"]) == int(track_id)
    ]
    if not indices:
        return None

    target_order = targets if targets else DEFAULT_TARGETS
    target_entries = [
        persisted[target] for target in target_order if target in persisted
    ]
    if not target_entries or any(e.holdout_predictions is None for e in target_entries):
        return None

    # D3: compose two-stage holdout predictions (P_dnf * field_size + (1-P_dnf)*clean
    # for primary; (1-P_dnf)*clean for secondaries). Falls back to identity
    # when no ``dnf_classifier`` is in ``target_entries``, preserving the
    # legacy single-head shape.
    composed = _two_stage_holdout_predictions(rows, target_entries, holdout_indices_all)
    if composed is None:
        return None

    position_in_holdout = {int(row_idx): pos for pos, row_idx in enumerate(holdout_indices_all)}

    abs_errors_target: list[float] = []
    abs_errors_dfs: list[float] = []
    primary_entry = persisted.get(PRIMARY_TARGET)
    for row_idx in indices:
        pos = position_in_holdout.get(row_idx)
        if pos is None:
            continue
        predicted_targets = {
            target: float(preds[pos])
            for target, preds in composed.items()
            if target != DNF_TARGET_KEY
        }
        # D3 defensive: if the cohort somehow trained only the DNF classifier
        # and zero regression heads (e.g. an upstream label-build bug), the
        # composed dict has just ``r_is_dnf`` left and the filtered
        # ``predicted_targets`` is empty. Skip the row rather than feed an
        # empty mapping into the DFS scorer, which would resolve to zero
        # points and inflate the per-track MAE.
        if not predicted_targets:
            continue
        row = rows[row_idx]
        if primary_entry is not None and PRIMARY_TARGET in predicted_targets:
            observed_primary = float((row.get("targets") or {}).get(PRIMARY_TARGET) or 0.0)
            abs_errors_target.append(
                abs(predicted_targets[PRIMARY_TARGET] - observed_primary)
            )
        predicted_total, _ = compute_dfs_points_from_targets(
            row.get("r_start_position"),
            predicted_targets,
            scoring_rules,
        )
        observed_total = float(row.get("dfs_points") or 0.0)
        abs_errors_dfs.append(abs(float(predicted_total) - observed_total))

    n = len(abs_errors_dfs)
    if n == 0:
        return None
    target_mae = (
        float(sum(abs_errors_target) / len(abs_errors_target))
        if abs_errors_target
        else None
    )
    return {
        "n": n,
        "target_mae": target_mae,
        "dfs_points_mae": float(sum(abs_errors_dfs) / n),
    }


def _enumerate_cohorts(
    hook: NascarMySqlHook,
    rules_id: int,
    *,
    series_ids: list[int],
) -> list[tuple[int, "ResolvedCurrentWeek"]]:
    """Resolve ``(series_id, current_week_context)`` for every series with a current week."""
    contexts: list[tuple[int, ResolvedCurrentWeek]] = []
    for series_id in series_ids:
        ctx = resolve_current_week_track_context(hook, series_id=series_id)
        if ctx is None:
            continue
        type_ids = track_type_ids_for_track(hook, ctx.track_id)
        if not type_ids:
            continue
        contexts.append(
            (
                series_id,
                ResolvedCurrentWeek(
                    series_id=int(series_id),
                    track_id=int(ctx.track_id),
                    track_type_ids=type_ids,
                ),
            )
        )
    return contexts


class ResolvedCurrentWeek(NamedTuple):
    series_id: int
    track_id: int
    track_type_ids: list[int]


def _log_rss(stage: str, **extra: Any) -> None:
    """Log the current process's RSS / peak RSS for OOM forensics.

    Reads ``/proc/self/status`` so the same call works in the Airflow
    worker, the spawned cohort child, and unit tests on Linux without a
    new dependency. Silently no-ops when the file isn't readable (e.g.
    macOS dev hosts) — instrumentation should never break the task.
    Values are reported in MiB to keep log lines short; the raw kB
    figure stays in the ``vmrss_kb`` / ``vmpeak_kb`` fields for grep.
    """
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            status_text = fh.read()
    except OSError:
        return

    fields: dict[str, int] = {}
    for line in status_text.splitlines():
        if not line:
            continue
        key, _, rest = line.partition(":")
        key = key.strip()
        if key not in {"VmRSS", "VmPeak", "VmSize", "VmHWM"}:
            continue
        rest = rest.strip()
        # Lines look like ``VmRSS:\t 12345 kB``; everything after the
        # number is the unit which we already assume is kB.
        try:
            value = int(rest.split()[0])
        except (IndexError, ValueError):
            continue
        fields[key] = value

    if not fields:
        return

    rss_kb = fields.get("VmRSS", 0)
    peak_kb = fields.get("VmPeak", 0)
    extras_str = " ".join(f"{k}={v}" for k, v in extra.items()) if extra else ""
    _LOG.info(
        "memory stage=%s pid=%s rss_mib=%.1f peak_mib=%.1f %svmrss_kb=%s vmpeak_kb=%s",
        stage,
        os.getpid(),
        rss_kb / 1024.0,
        peak_kb / 1024.0,
        f"{extras_str} " if extras_str else "",
        rss_kb,
        peak_kb,
    )


def _max_train_weeks_env() -> int | None:
    """Read ``FS_ML_MAX_TRAIN_WEEKS`` (unset / blank = no cap).

    Forwarded to :func:`load_training_frame` as a window subselect filter
    so the per-cohort row count drops proportionally to the cap. Non-int
    or non-positive values disable the cap with a single warning so a
    misconfigured env can't silently throw away training data.
    """
    raw = os.environ.get("FS_ML_MAX_TRAIN_WEEKS")
    if raw is None or raw.strip() == "":
        return None
    try:
        value = int(raw)
    except ValueError:
        _LOG.warning(
            "Ignoring non-int FS_ML_MAX_TRAIN_WEEKS=%r; no cap applied", raw
        )
        return None
    if value < 1:
        _LOG.warning(
            "Ignoring non-positive FS_ML_MAX_TRAIN_WEEKS=%r; no cap applied", raw
        )
        return None
    return value


def _train_cohort_isolation_enabled() -> bool:
    """Read ``FS_ML_TRAIN_COHORT_ISOLATION`` (default on).

    Returns ``False`` only when the env is explicitly set to ``0/false/no/off``,
    matching the truthy parser used elsewhere in the ML config. The default-on
    posture is intentional: the OOM observed on the 12 GB worker only
    reproduces when multiple cohorts share heap arenas inside the same
    Celery child, and the cost of spawning is small compared to a fit.
    """
    raw = os.environ.get("FS_ML_TRAIN_COHORT_ISOLATION", "1")
    return str(raw).strip().lower() not in {"0", "false", "no", "off", ""}


_ISOLATED_COHORT_TIMEOUT_SECONDS = 60 * 60  # 1h ceiling per cohort; well above the worst fit observed.


def _isolated_cohort_entry(
    args: dict[str, Any],
    queue: "multiprocessing.queues.Queue[dict[str, Any]]",
) -> None:
    """Child-process entry: load the cohort frame and train inside one spawn.

    Module-level (required for ``multiprocessing.get_context('spawn')`` to
    pickle the target) and intentionally narrow: builds its own DB hook,
    runs :func:`load_training_frame` + :func:`_train_cohort`, then puts the
    result (or a serialized exception) on the IPC queue and exits. The
    spawned child inherits no Python objects from the parent — every
    cohort's working set (rows, Boruta shadow matrices, fitted boosters,
    feature dicts) is returned to the OS on ``exit(0)``.

    Args are passed as a plain ``dict`` to dodge ``kwargs`` pickling
    edge cases with non-positional ``NamedTuple`` defaults. Errors are
    re-raised on the parent side by :func:`_run_cohort_isolated`.
    """
    try:
        from include.nascar.ml.training_targets import (
            load_training_frame as _load_training_frame,
        )

        _log_rss(
            "isolated_cohort_child_start",
            series_id=args.get("series_id"),
            track_type_id=args.get("track_type_id"),
        )

        mysql_conn_id = args["mysql_conn_id"]
        child_hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)

        load_kwargs = dict(
            dfs_scoring_rules_id=args["rules_id"],
            series_id=args["series_id"],
            track_type_id=args["track_type_id"],
            mysql_conn_id=mysql_conn_id,
        )
        max_train_weeks = args.get("max_train_weeks")
        if max_train_weeks is not None:
            load_kwargs["max_train_weeks"] = max_train_weeks

        rows = _load_training_frame(**load_kwargs)
        _log_rss(
            "isolated_cohort_after_frame_load",
            series_id=args.get("series_id"),
            track_type_id=args.get("track_type_id"),
            n_rows=len(rows),
        )
        if not rows:
            queue.put({"_empty": True})
            return

        result = _train_cohort(
            series_id=args["series_id"],
            track_type_id=args["track_type_id"],
            track_id_anchor=args["track_id_anchor"],
            rows=rows,
            feature_keys=args["feature_keys"],
            cohort_n_weeks=args["cohort_n_weeks"],
            holdout_weeks=args["holdout_weeks"],
            dev_weeks=args["dev_weeks"],
            artifact_root=Path(args["artifact_root"]),
            hook=child_hook,
            scoring_rules=args["scoring_rules"],
            targets=tuple(args["targets"]) if args.get("targets") else None,
        )
        _log_rss(
            "isolated_cohort_after_train",
            series_id=args.get("series_id"),
            track_type_id=args.get("track_type_id"),
        )
        queue.put({"_ok": True, "result": result})
    except BaseException as exc:
        queue.put(
            {
                "_error": True,
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
    finally:
        _log_rss(
            "isolated_cohort_child_exit",
            series_id=args.get("series_id"),
            track_type_id=args.get("track_type_id"),
        )


def _run_cohort_isolated(args: dict[str, Any]) -> dict[str, Any] | None:
    """Parent-side spawner: run one cohort in a child process and unpack the result.

    Returns the cohort result dict, ``None`` when the child reported an
    empty frame (so the caller can record ``empty_cohort_frame`` the same
    way the in-process branch does), or raises ``RuntimeError`` wrapping
    the child's traceback on failure.

    Uses ``multiprocessing.get_context("spawn")`` (not ``fork``) so the
    child does not inherit the parent's heap state — that's the whole
    point of process isolation here. ``spawn`` also makes the path
    behave the same on macOS dev hosts and Linux workers.
    """
    ctx = multiprocessing.get_context("spawn")
    queue: "multiprocessing.queues.Queue[dict[str, Any]]" = ctx.Queue()
    proc = ctx.Process(target=_isolated_cohort_entry, args=(args, queue))
    proc.start()
    proc.join(timeout=_ISOLATED_COHORT_TIMEOUT_SECONDS)

    if proc.is_alive():
        proc.terminate()
        proc.join(timeout=30)
        if proc.is_alive():
            proc.kill()
            proc.join()
        raise RuntimeError(
            "Isolated cohort training exceeded "
            f"{_ISOLATED_COHORT_TIMEOUT_SECONDS}s and was killed; "
            f"series_id={args.get('series_id')} track_type_id={args.get('track_type_id')}"
        )

    if queue.empty():
        raise RuntimeError(
            "Isolated cohort training exited without posting a result; "
            f"exitcode={proc.exitcode} series_id={args.get('series_id')} "
            f"track_type_id={args.get('track_type_id')}"
        )

    payload = queue.get()
    if payload.get("_error"):
        raise RuntimeError(
            "Isolated cohort training raised "
            f"{payload.get('type')}: {payload.get('message')}\n"
            f"{payload.get('traceback')}"
        )
    if payload.get("_empty"):
        return None
    return payload.get("result")


def train_models_if_needed_task(**_context: object) -> dict[str, Any]:
    """Airflow entrypoint for the ML DAG's ``train_models_if_needed`` task."""
    _log_rss("train_task_start")
    mysql_conn_id = "nascar_conn"
    dfs_id = DRAFTKINGS_DFS_ID
    hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)

    rules = _active_rules(hook, dfs_id)
    if rules is None:
        _LOG.warning(
            "train_models_if_needed_task: no active dfs_scoring_rules for dfs_id=%s", dfs_id
        )
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

    # C3: defer ``features_for_scope`` to each cohort so it can pass its own
    # ``track_type_id`` and keep other-type ``_tt_`` aggregates as signal.
    # We still run a track_type_id-less pass here only to catch the
    # "metadata has zero usable features" edge case early.
    if not features_for_scope(all_feature_keys, scope=SCOPE_TRACK_TYPE):
        _LOG.warning(
            "train_models_if_needed_task: no per-track features in snapshot metadata"
        )
        return {"skipped": "no_feature_keys"}

    cohort_min = min_cohort_weeks()
    holdout_weeks = _holdout_weeks_env()
    dev_weeks = _dev_weeks_env()
    artifact_root = _artifact_root()
    targets = _resolve_train_targets(scoring_rules)
    isolation_enabled = _train_cohort_isolation_enabled()
    max_train_weeks = _max_train_weeks_env()

    cohort_results: list[dict[str, Any]] = []
    series_contexts: list[dict[str, Any]] = []
    any_eligible = False
    cold_cohorts: list[dict[str, Any]] = []
    all_predict_ready = True

    enumerated = _enumerate_cohorts(hook, rules_id, series_ids=series_ids)
    if not enumerated:
        return {"skipped": "no_eligible_cohorts"}

    _log_rss(
        "train_task_after_initial_scope_queries",
        n_series=len(enumerated),
        isolation_enabled=int(isolation_enabled),
        max_train_weeks=max_train_weeks if max_train_weeks is not None else -1,
    )

    for series_id, current in enumerated:
        series_latest = latest_train_week_for_retrain(
            hook,
            rules_id=rules_id,
            series_id=series_id,
            train_kind=SNAPSHOT_KIND_TRAIN,
        )
        if series_latest is None:
            all_predict_ready = False
            continue

        any_eligible = True
        per_series_pairs: list[dict[str, Any]] = []

        for track_type_id in current.track_type_ids:
            n_weeks = cohort_distinct_weeks(
                hook,
                series_id=series_id,
                rules_id=rules_id,
                track_type_id=track_type_id,
            )
            if n_weeks < cohort_min:
                cold_cohorts.append(
                    {
                        "series_id": int(series_id),
                        "track_type_id": int(track_type_id),
                        "eligible_weeks": int(n_weeks),
                        "required_weeks": int(cohort_min),
                    }
                )
                per_series_pairs.append(
                    {
                        "track_type_id": int(track_type_id),
                        "cohort_n_weeks": int(n_weeks),
                        "skipped": "insufficient_cohort_weeks",
                    }
                )
                continue

            covered = _cohort_covered_week(
                hook,
                rules_id,
                series_id=series_id,
                track_type_id=track_type_id,
            )
            if covered is not None and covered >= series_latest:
                per_series_pairs.append(
                    {
                        "track_type_id": int(track_type_id),
                        "cohort_n_weeks": int(n_weeks),
                        "covered_week": int(covered),
                        "skipped": "cohort_already_fresh",
                    }
                )
                continue

            all_predict_ready = False

            _log_rss(
                "train_task_before_cohort",
                series_id=int(series_id),
                track_type_id=int(track_type_id),
                cohort_n_weeks=int(n_weeks),
                isolation_enabled=int(isolation_enabled),
            )

            siblings = sibling_track_ids_for_type(
                hook,
                series_id=series_id,
                rules_id=rules_id,
                track_type_id=track_type_id,
            )

            cohort_kwargs = {
                "rules_id": int(rules_id),
                "series_id": int(series_id),
                "track_type_id": int(track_type_id),
                "track_id_anchor": int(current.track_id),
                "feature_keys": list(all_feature_keys),
                "cohort_n_weeks": int(n_weeks),
                "holdout_weeks": int(holdout_weeks),
                "dev_weeks": int(dev_weeks),
                "artifact_root": str(artifact_root),
                "scoring_rules": scoring_rules,
                "targets": list(targets),
                "mysql_conn_id": mysql_conn_id,
                "max_train_weeks": max_train_weeks,
            }

            # Two code paths share a single ``_train_cohort`` body. The
            # isolated path spawns a fresh subprocess so the heavy working
            # set (Boruta shadow matrix, XGBoost models, feature dicts)
            # is reclaimed by the OS at child exit. The in-process path
            # remains the default for tests and any deployment that opts
            # out via FS_ML_TRAIN_COHORT_ISOLATION=0.
            if isolation_enabled:
                result = _run_cohort_isolated(cohort_kwargs)
                if result is None:
                    per_series_pairs.append(
                        {
                            "track_type_id": int(track_type_id),
                            "cohort_n_weeks": int(n_weeks),
                            "skipped": "empty_cohort_frame",
                        }
                    )
                    continue
            else:
                load_kwargs = dict(
                    dfs_scoring_rules_id=rules_id,
                    series_id=series_id,
                    track_type_id=track_type_id,
                    mysql_conn_id=mysql_conn_id,
                )
                if max_train_weeks is not None:
                    load_kwargs["max_train_weeks"] = max_train_weeks
                rows = load_training_frame(**load_kwargs)
                if not rows:
                    per_series_pairs.append(
                        {
                            "track_type_id": int(track_type_id),
                            "cohort_n_weeks": int(n_weeks),
                            "skipped": "empty_cohort_frame",
                        }
                    )
                    continue
                result = _train_cohort(
                    series_id=series_id,
                    track_type_id=track_type_id,
                    track_id_anchor=current.track_id,
                    rows=rows,
                    feature_keys=all_feature_keys,
                    cohort_n_weeks=n_weeks,
                    holdout_weeks=holdout_weeks,
                    dev_weeks=dev_weeks,
                    artifact_root=artifact_root,
                    hook=hook,
                    scoring_rules=scoring_rules,
                    targets=targets,
                )
                del rows

            result["sibling_track_ids"] = siblings
            cohort_results.append(result)
            per_series_pairs.append(result)

            _log_rss(
                "train_task_after_cohort",
                series_id=int(series_id),
                track_type_id=int(track_type_id),
                cohort_n_weeks=int(n_weeks),
                isolation_enabled=int(isolation_enabled),
            )

        series_contexts.append(
            {
                "series_id": int(series_id),
                "current_track_id": int(current.track_id),
                "current_track_type_ids": list(current.track_type_ids),
                "cohorts": per_series_pairs,
            }
        )

    if not any_eligible:
        return {"skipped": "no_eligible_cohorts"}

    if not cohort_results and all_predict_ready and not cold_cohorts:
        _LOG.info(
            "train_models_if_needed_task: predict-ready; latest=%s",
            latest_train_week,
        )
        return {
            "skipped": "predict_ready",
            "latest_train_week": latest_train_week,
            "series_contexts": series_contexts,
            "blend_weight_strategy": blend_weight_strategy(),
        }

    if not cohort_results and cold_cohorts:
        _LOG.warning(
            "train_models_if_needed_task: every eligible cohort below FS_ML_MIN_COHORT_WEEKS; "
            "cold=%s",
            cold_cohorts,
        )
        return {
            "skipped": "insufficient_cohort_weeks_all_cohorts",
            "latest_train_week": latest_train_week,
            "cold_cohorts": cold_cohorts,
            "series_contexts": series_contexts,
            "blend_weight_strategy": blend_weight_strategy(),
        }

    _LOG.info(
        "train_models_if_needed_task: latest_week=%s cohorts_trained=%s cold_cohorts=%s",
        latest_train_week,
        len(cohort_results),
        len(cold_cohorts),
    )
    _log_rss(
        "train_task_end",
        cohorts_trained=len(cohort_results),
        cold_cohorts=len(cold_cohorts),
    )
    return {
        "skipped": None,
        "latest_train_week": latest_train_week,
        "cohort_results": cohort_results,
        "cold_cohorts": cold_cohorts,
        "series_contexts": series_contexts,
        "available_scopes": list(ALL_SCOPES),
        "blend_weight_strategy": blend_weight_strategy(),
    }
