"""Airflow callable: score the latest ``kind='predict'`` snapshot per target.

For each target, the current week's ``track_id`` is mapped to its
``track_type_id`` set via ``tracks_track_types``. We load the active
``model_scope='track_type'`` artifact for each type, predict per driver, and
blend the per-cohort predictions via ``FS_ML_BLEND_WEIGHTS``
(``inverse_mae`` (default), ``sample_count``, or ``equal``).

The single ``ml_predictions`` row per driver is keyed by the lowest cohort
``ml_models.id`` that contributed to the primary-target blend so downstream
joins (`Monte Carlo`, lineup generator) keep a single foreign key.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import numpy as np

from airflow_utils.db_cursor import DbCursor
from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.enums import NascarSeries
from include.nascar.ml.dnf import (
    DNF_TARGET_KEY,
    MODEL_KIND_DNF_CLASSIFIER,
    MODEL_KIND_REGRESSION,
    MODEL_KIND_REGRESSION_CLEAN,
    compose_two_stage_primary,
    compose_two_stage_secondary,
)
from include.nascar.ml.feature_matrix import build_feature_matrix
from include.nascar.ml.model_scope import (
    DEFAULT_BLEND_STRATEGY,
    SCOPE_TRACK_TYPE,
    blend_weight_strategy,
    track_type_ids_for_track,
)
from include.nascar.ml.scoring import compute_dfs_points_from_targets
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID, SNAPSHOT_KIND_PREDICT
from include.nascar.ml.xgb_train_pipeline import DEFAULT_TARGETS, PRIMARY_TARGET

_LOG = logging.getLogger(__name__)

_FALLBACK_TARGET_DEFAULTS: dict[str, float] = {
    "r_total_fastest_laps": 0.0,
    "r_total_laps_led": 0.0,
}
"""Floor defaults used when neither a cohort model nor a cohort training mean
is available for a secondary target. The B3 preferred fallback chain is
``cohort_mean -> floor_default -> 0.0``; this dict only owns the floor."""

PRIMARY_TARGET_RAW_KEY = "r_finish_position_raw"
"""D1: per-row key carrying the raw blended float behind the integer rank.

We persist both the blended float (for audit / debugging) and the reconciled
integer ``1..field_size`` rank on ``ml_predictions.predicted_targets_json``
so downstream consumers (DFS scoring, Monte Carlo, lineup generator) see
physically valid permutations while we keep the float for drift tracking.
"""


def reconcile_primary_to_integer_ranks(
    blended_primary: np.ndarray,
    *,
    descending: bool = False,
) -> tuple[np.ndarray, np.ndarray]:
    """Convert blended primary predictions to integer ranks ``1..field_size``.

    Returns ``(raw_floats, integer_ranks)``. ``raw_floats`` is a defensive
    copy of the input. ``integer_ranks`` is an int array where the driver
    with the lowest predicted finish (regression default: lower = better)
    gets rank ``1``. When ``descending=True`` (the D2 ``rank:pairwise``
    case where higher scores mean predicted top finishers), the argsort
    direction is flipped so rank ``1`` still goes to the predicted winner.

    The result is always a permutation of ``1..n`` — D1's invariant. Ties
    in the float input get broken by the stable input order, which is
    deterministic across runs because the predict task orders rows by
    ``car_driver_id`` upstream.
    """
    raw_floats = np.asarray(blended_primary, dtype=float).copy()
    n = raw_floats.size
    if n == 0:
        return raw_floats, np.zeros(0, dtype=int)
    if descending:
        order = np.argsort(-raw_floats, kind="stable")
    else:
        order = np.argsort(raw_floats, kind="stable")
    ranks = np.empty(n, dtype=int)
    ranks[order] = np.arange(1, n + 1, dtype=int)
    return raw_floats, ranks


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
            s.feature_keys
        FROM nascar.ml_training_snapshots s
        JOIN nascar.v_series_current_week cw
            ON cw.series_id = s.series_id
           AND cw.week_id = s.week_id
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


def _scope_lookup(
    hook: NascarMySqlHook,
    *,
    target: str,
    series_id: int,
    track_type_id: int,
) -> dict[str, Any] | None:
    """Return the most recent active cohort model for ``(series_id, target, track_type_id)``.

    The ``m.series_id = %s`` filter is the safety bar preventing a Cup cohort
    model from being picked up for an Xfinity / Truck predict snapshot at the
    same physical track. Only ``model_scope='track_type'`` rows are
    considered.

    D3: returns ``model_kind`` in the result dict so the predict task can
    distinguish ``regression_clean`` (two-stage) from legacy ``regression``
    and ``dnf_classifier`` heads.
    """
    rows = hook.get_records(
        """
        SELECT m.id, m.artifact_uri, m.hyperparams_json, m.series_id, m.model_scope,
               m.track_id, m.track_type_id, m.cohort_n_weeks, fs.selected_features,
               m.model_kind, m.holdout_residual_quantiles, fs.boruta_metadata
        FROM nascar.ml_models m
        JOIN nascar.ml_feature_sets fs
            ON fs.id = m.ml_feature_set_id
           AND fs.deleted_at IS NULL
        WHERE m.deleted_at IS NULL
          AND m.series_id = %s
          AND m.model_scope = %s
          AND m.track_type_id = %s
          AND JSON_CONTAINS(fs.target_keys, JSON_QUOTE(%s))
          AND JSON_LENGTH(fs.target_keys) = 1
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT 1
        """,
        parameters=(int(series_id), SCOPE_TRACK_TYPE, int(track_type_id), target),
    )
    if not rows:
        return None
    row = rows[0]
    # D3 + E1 + E4: ``model_kind`` (D3), ``holdout_residual_quantiles``
    # (E1), and ``boruta_metadata`` (E4 ``train_feature_stats``) are
    # appended to the projection; legacy mock fixtures that emit the
    # shorter tuple shape fall through to the defaults so older unit
    # tests don't need rewrites.
    residual_quantiles_raw: Any = None
    boruta_metadata_raw: Any = None
    if len(row) >= 12:
        (
            model_id,
            artifact_uri,
            hyperparams_json,
            m_series_id,
            model_scope,
            m_track_id,
            m_track_type_id,
            m_cohort_n_weeks,
            selected_features_json,
            model_kind,
            residual_quantiles_raw,
            boruta_metadata_raw,
        ) = row[:12]
    elif len(row) >= 10:
        (
            model_id,
            artifact_uri,
            hyperparams_json,
            m_series_id,
            model_scope,
            m_track_id,
            m_track_type_id,
            m_cohort_n_weeks,
            selected_features_json,
            model_kind,
        ) = row[:10]
    else:
        (
            model_id,
            artifact_uri,
            hyperparams_json,
            m_series_id,
            model_scope,
            m_track_id,
            m_track_type_id,
            m_cohort_n_weeks,
            selected_features_json,
        ) = row[:9]
        model_kind = MODEL_KIND_REGRESSION
    return {
        "id": int(model_id),
        "artifact_uri": artifact_uri,
        "hyperparams": _parse_json(hyperparams_json) or {},
        "selected_features": _parse_json(selected_features_json) or [],
        "series_id": int(m_series_id),
        "model_scope": model_scope,
        "track_id": int(m_track_id) if m_track_id is not None else None,
        "track_type_id": int(m_track_type_id) if m_track_type_id is not None else None,
        "cohort_n_weeks": int(m_cohort_n_weeks) if m_cohort_n_weeks is not None else 0,
        "model_kind": model_kind or MODEL_KIND_REGRESSION,
        "holdout_residual_quantiles": _parse_json(residual_quantiles_raw),
        "boruta_metadata": _parse_json(boruta_metadata_raw) or {},
    }


def _cohort_train_target_mean(
    hook: NascarMySqlHook,
    *,
    target: str,
    series_id: int,
    track_type_ids: list[int] | None = None,
) -> float | None:
    """B3: predict-time fallback for a secondary target with no cohort model.

    Averages :col:`ml_models.train_target_mean` across active cohort
    (``model_scope='track_type'``) rows for ``(series_id, target)``,
    preferring the snapshot's ``track_type_ids`` when supplied. Falls
    back to the unfiltered ``series_id`` average when the type-scoped
    query is empty, and returns ``None`` when no cohort model has
    populated ``train_target_mean`` yet (legacy rows / cold start).
    """
    if track_type_ids:
        placeholders = ", ".join(["%s"] * len(track_type_ids))
        rows = hook.get_records(
            f"""
            SELECT AVG(m.train_target_mean)
            FROM nascar.ml_models m
            JOIN nascar.ml_feature_sets fs
                ON fs.id = m.ml_feature_set_id
               AND fs.deleted_at IS NULL
            WHERE m.deleted_at IS NULL
              AND m.series_id = %s
              AND m.model_scope = %s
              AND m.train_target_mean IS NOT NULL
              AND m.track_type_id IN ({placeholders})
              AND JSON_CONTAINS(fs.target_keys, JSON_QUOTE(%s))
              AND JSON_LENGTH(fs.target_keys) = 1
            """,
            parameters=(
                int(series_id),
                SCOPE_TRACK_TYPE,
                *[int(t) for t in track_type_ids],
                target,
            ),
        )
        if rows and rows[0][0] is not None:
            try:
                return float(rows[0][0])
            except (TypeError, ValueError):
                pass

    rows = hook.get_records(
        """
        SELECT AVG(m.train_target_mean)
        FROM nascar.ml_models m
        JOIN nascar.ml_feature_sets fs
            ON fs.id = m.ml_feature_set_id
           AND fs.deleted_at IS NULL
        WHERE m.deleted_at IS NULL
          AND m.series_id = %s
          AND m.model_scope = %s
          AND m.train_target_mean IS NOT NULL
          AND JSON_CONTAINS(fs.target_keys, JSON_QUOTE(%s))
          AND JSON_LENGTH(fs.target_keys) = 1
        """,
        parameters=(int(series_id), SCOPE_TRACK_TYPE, target),
    )
    if not rows or rows[0][0] is None:
        return None
    try:
        return float(rows[0][0])
    except (TypeError, ValueError):
        return None


def _latest_holdout_mae(
    hook: NascarMySqlHook,
    *,
    model_id: int,
    target: str,
) -> float | None:
    """Read the latest ``ml_model_evaluations.metric='mae'`` row for the cohort model.

    Prefers ``eval_type='cohort'`` since ``mae`` rows are written keyed to the
    cohort slice. Returns ``None`` when no MAE is available (cold start).
    Skips ``dfs_points_*`` metrics — they are total-DFS MAE / RMSE / Spearman,
    not the per-target holdout MAE we want for blending.
    """
    rows = hook.get_records(
        """
        SELECT metric_value
        FROM nascar.ml_model_evaluations
        WHERE ml_model_id = %s
          AND metric = 'mae'
          AND deleted_at IS NULL
        ORDER BY (eval_type = 'cohort') DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        parameters=(int(model_id),),
    )
    del target  # parameter kept for symmetry; target is encoded by model_id
    if not rows or rows[0][0] is None:
        return None
    try:
        return float(rows[0][0])
    except (TypeError, ValueError):
        return None


def _latest_dfs_points_spearman(
    hook: NascarMySqlHook,
    *,
    model_id: int,
) -> float | None:
    """E5: read the latest ``dfs_points_spearman`` evaluation for ``model_id``.

    The Spearman correlation is written per cohort by ``_train_cohort`` in
    :mod:`include.nascar.ml.train_task` after the trainer runs
    :func:`include.nascar.ml.xgb_train_pipeline.holdout_dfs_eval_metrics`
    on the cohort holdout. Returns ``None`` when the metric is missing or
    non-numeric (e.g. cold-start cohort whose holdout was too small for
    a stable rank correlation). Predict's blend fallback chain swaps to
    equal weights when any contributor returns ``None`` from this helper.
    """
    rows = hook.get_records(
        """
        SELECT metric_value
        FROM nascar.ml_model_evaluations
        WHERE ml_model_id = %s
          AND metric = 'dfs_points_spearman'
          AND deleted_at IS NULL
        ORDER BY (eval_type = 'cohort') DESC, created_at DESC, id DESC
        LIMIT 1
        """,
        parameters=(int(model_id),),
    )
    if not rows or rows[0][0] is None:
        return None
    try:
        return float(rows[0][0])
    except (TypeError, ValueError):
        return None


def _predict_with_model(
    rows: list[dict[str, Any]],
    model_entry: dict[str, Any],
    *,
    booster_cls: type,
) -> np.ndarray:
    booster = booster_cls()
    booster.load_model(model_entry["artifact_uri"])
    # B2: predict time is always non-strict — a stray non-finite at score
    # time gets zero-filled and warned about, never raised. Training is the
    # right place to fail upstream-data-quality regressions.
    features = build_feature_matrix(
        rows, model_entry["selected_features"], strict=False
    )
    return booster.predict(features)


_DRIFT_Z_THRESHOLD = 3.0
"""E4: warn when ``|predict_mean - train_mean| > 3 * train_std`` for a feature.

Three standard deviations is the classic 99.7% Gaussian rule; nothing about
NASCAR's feature distributions is exactly Normal but it's a conservative
"this almost certainly drifted" bar that keeps the warning log usable
during a normal race week without flooding it."""


def _drift_warnings_for_cohort(
    rows: list[dict[str, Any]],
    *,
    model_entry: dict[str, Any],
) -> list[dict[str, Any]]:
    """E4: per-cohort drift report comparing predict snapshot vs train stats.

    Reads ``model_entry["boruta_metadata"]["train_feature_stats"]`` (written by
    the trainer at fit time) and recomputes the same per-feature mean over
    the predict snapshot. Any feature whose Z-score (using the *train*
    ``std`` as the denominator) exceeds :data:`_DRIFT_Z_THRESHOLD` is
    emitted as a warning dict so the predict task can attach it to the
    per-row ``blend_metadata_json.drift_warnings`` field.

    Returns an empty list when the cohort lacks ``train_feature_stats``
    (legacy cohort trained before E4 landed) or when no selected feature
    drifts. Predict still proceeds in either case; this surface is
    observability only.
    """
    boruta = model_entry.get("boruta_metadata") or {}
    train_stats = boruta.get("train_feature_stats") or {}
    selected_features = model_entry.get("selected_features") or []
    if not train_stats or not selected_features:
        return []

    # Reuse ``build_feature_matrix`` so the predict-time column ordering
    # mirrors the trainer exactly (including median imputation of any
    # missing predict-time features).
    matrix = build_feature_matrix(rows, selected_features, strict=False)
    if matrix.size == 0:
        return []

    cohort_track_type_id = model_entry.get("track_type_id")
    warnings: list[dict[str, Any]] = []
    for j, feature in enumerate(selected_features):
        stats = train_stats.get(feature)
        if not stats:
            continue
        train_mean = float(stats.get("mean", 0.0) or 0.0)
        train_std = float(stats.get("std", 0.0) or 0.0)
        if train_std <= 0.0:
            continue
        predict_mean = float(np.nanmean(matrix[:, j]))
        z = (predict_mean - train_mean) / train_std
        if abs(z) > _DRIFT_Z_THRESHOLD:
            warnings.append(
                {
                    "feature": feature,
                    "train_mean": round(train_mean, 6),
                    "predict_mean": round(predict_mean, 6),
                    "train_std": round(train_std, 6),
                    "z": round(float(z), 4),
                    "cohort_track_type_id": (
                        int(cohort_track_type_id)
                        if cohort_track_type_id is not None
                        else None
                    ),
                    "ml_model_id": int(model_entry["id"]),
                }
            )
    if warnings:
        _LOG.warning(
            "predict_upcoming_week_task: drift detected for ml_model_id=%s "
            "track_type_id=%s features=%s",
            model_entry["id"],
            cohort_track_type_id,
            [w["feature"] for w in warnings],
        )
    return warnings


def _weight_average_intervals(
    cohort_entries: list[dict[str, Any]],
    weights: list[float],
) -> dict[str, float] | None:
    """E1 helper: weight-average per-cohort ``holdout_residual_quantiles``.

    Returns ``None`` when no cohort carries a usable quantile dict
    (cold start / legacy rows). Each quantile is averaged independently
    by the blend weights; missing entries skip the contributor for that
    quantile so a partial cohort doesn't tank the whole interval.
    """
    if not cohort_entries:
        return None
    keys = ("q05", "q10", "q50", "q90", "q95")
    averaged: dict[str, float] = {}
    for key in keys:
        weighted: list[tuple[float, float]] = []
        for entry, weight in zip(cohort_entries, weights, strict=True):
            quantiles = entry.get("holdout_residual_quantiles") or {}
            value = quantiles.get(key)
            if value is None:
                continue
            try:
                weighted.append((float(value), float(weight)))
            except (TypeError, ValueError):
                continue
        if not weighted:
            continue
        total_weight = sum(w for _, w in weighted)
        if total_weight <= 0:
            continue
        averaged[key] = sum(v * w for v, w in weighted) / total_weight
    return averaged or None


def _predict_proba_with_model(
    rows: list[dict[str, Any]],
    model_entry: dict[str, Any],
    *,
    booster_cls: type,
) -> np.ndarray:
    """Classifier sibling of :func:`_predict_with_model` — returns ``P(class=1)``.

    D3: ``r_is_dnf`` is a binary head whose predict-time output is the
    probability of a DNF. Threading this through a separate function (vs
    overloading ``_predict_with_model``) keeps each call site type-clear
    about what the returned array means; the regression call sites still
    receive raw regression scores, and the classifier call sites
    explicitly receive probabilities.
    """
    booster = booster_cls()
    booster.load_model(model_entry["artifact_uri"])
    features = build_feature_matrix(
        rows, model_entry["selected_features"], strict=False
    )
    proba = booster.predict_proba(features)
    return np.asarray(proba)[:, 1]


def _blend_weights(
    contributors: list[tuple[int, int, np.ndarray, float | None, int, float | None]],
    *,
    strategy: str,
) -> tuple[np.ndarray, str]:
    """Return the per-contributor weights (sum to 1) and the effective strategy.

    ``contributors`` is a list of
    ``(model_id, track_type_id, preds, mae, cohort_n_weeks, spearman)``.
    The ``spearman`` slot is only populated when ``strategy == 'spearman'``;
    other strategies ignore the value. Falls back to equal weights when:

    - ``inverse_mae`` is requested but any contributor lacks a non-null MAE.
    - ``spearman`` is requested but any contributor lacks a usable
      ``dfs_points_spearman`` (E5). Spearman <= 0 means the cohort ranks
      drivers worse than chance on DFS points, so even when present we
      treat the cohort as untrusted and fall back to equal weights.
    """
    n = len(contributors)
    if n == 0:
        return np.zeros(0, dtype=float), strategy
    if strategy == "equal" or n == 1:
        return np.full(n, 1.0 / n), "equal" if strategy != "equal" and n > 1 else strategy
    if strategy == "sample_count":
        raw = np.array([max(1, c[4]) for c in contributors], dtype=float)
        return raw / raw.sum(), strategy
    if strategy == "spearman":
        spearmans = [c[5] for c in contributors]
        if any(s is None for s in spearmans):
            _LOG.warning(
                "predict_upcoming_week_task: spearman blend missing dfs_points_spearman "
                "for some contributors; falling back to equal weights"
            )
            return np.full(n, 1.0 / n), "equal_fallback"
        positive = [max(0.0, float(s)) for s in spearmans]
        total = sum(positive)
        if total <= 0:
            _LOG.warning(
                "predict_upcoming_week_task: spearman blend had no positive correlations; "
                "falling back to equal weights"
            )
            return np.full(n, 1.0 / n), "equal_fallback"
        return np.array(positive, dtype=float) / total, strategy
    # Default branch: inverse_mae.
    maes = [c[3] for c in contributors]
    if any(m is None or m <= 0 for m in maes):
        _LOG.warning(
            "predict_upcoming_week_task: inverse_mae blend missing MAE for some "
            "contributors; falling back to equal weights"
        )
        return np.full(n, 1.0 / n), "equal_fallback"
    inv = np.array([1.0 / float(m) for m in maes], dtype=float)
    return inv / inv.sum(), strategy


def _predict_values_for_target(
    hook: NascarMySqlHook,
    *,
    target: str,
    series_id: int,
    track_id: int,
    rows: list[dict[str, Any]],
    booster_cls: type,
    type_ids: list[int],
    strategy: str,
    task: str = "regression",
) -> tuple[np.ndarray | None, dict[str, Any] | None, int | None]:
    """Blend cohort models for ``track_id`` -> per-driver prediction vector.

    D3: ``task='classifier'`` routes per-cohort predictions through
    :func:`_predict_proba_with_model` so the contributors are
    ``P(DNF)`` arrays rather than regression scores. The blend strategy
    is honoured identically (probability average is just ``np.average``
    over the contributor stack).
    """
    if not type_ids:
        return None, None, None

    if task == "classifier":
        per_cohort_predict = _predict_proba_with_model
    else:
        per_cohort_predict = _predict_with_model

    contributors: list[
        tuple[int, int, np.ndarray, float | None, int, float | None]
    ] = []
    skipped_types: list[dict[str, Any]] = []
    model_kinds: list[str] = []
    cohort_artifacts: list[dict[str, Any]] = []
    for type_id in type_ids:
        entry = _scope_lookup(
            hook,
            target=target,
            series_id=series_id,
            track_type_id=type_id,
        )
        if entry is None or not entry.get("artifact_uri"):
            skipped_types.append({"track_type_id": type_id, "reason": "no_model"})
            continue
        preds = per_cohort_predict(rows, entry, booster_cls=booster_cls)
        mae = _latest_holdout_mae(hook, model_id=entry["id"], target=target)
        # E5: only hit ``ml_model_evaluations`` for ``dfs_points_spearman``
        # when the requested strategy actually consumes it. Other strategies
        # (default ``inverse_mae``, ``equal``, ``sample_count``) get a
        # ``None`` placeholder so the tuple shape stays uniform.
        spearman = (
            _latest_dfs_points_spearman(hook, model_id=entry["id"])
            if strategy == "spearman"
            else None
        )
        contributors.append(
            (
                entry["id"],
                type_id,
                preds,
                mae,
                entry.get("cohort_n_weeks") or 0,
                spearman,
            )
        )
        model_kinds.append(entry.get("model_kind", MODEL_KIND_REGRESSION))
        cohort_artifacts.append(entry)

    if not contributors:
        return None, None, None

    weights, effective_strategy = _blend_weights(contributors, strategy=strategy)
    blended = np.average(np.stack([c[2] for c in contributors]), axis=0, weights=weights)
    meta = {
        "series_id": int(series_id),
        "model_scope": SCOPE_TRACK_TYPE,
        "track_id": int(track_id),
        "track_type_ids": [int(c[1]) for c in contributors],
        "model_ids": [int(c[0]) for c in contributors],
        "model_kinds": model_kinds,
        "weights": [round(float(w), 6) for w in weights],
        "maes": [None if c[3] is None else round(float(c[3]), 6) for c in contributors],
        "cohort_n_weeks": [int(c[4]) for c in contributors],
        # E5: emit per-contributor Spearman in metadata so the same lineage
        # blob can be audited downstream. Stays ``None`` outside the
        # spearman strategy to avoid surprising readers that don't expect
        # the values.
        "spearmans": [
            None if c[5] is None else round(float(c[5]), 6) for c in contributors
        ],
        "blend_strategy_requested": strategy,
        "blend_strategy_used": effective_strategy,
        "skipped_track_type_ids": skipped_types,
        # E3 lineage: carry the full cohort artifact dicts in-memory only
        # (not persisted on ``ml_predictions.blend_metadata_json``); the
        # caller uses these to look up per-model
        # ``holdout_residual_quantiles`` and ``train_feature_stats`` for
        # the drift guard and conformal-interval reporting.
        "_cohort_entries": cohort_artifacts,
    }
    return blended, meta, min(c[0] for c in contributors)


def predict_upcoming_week_task(**_context: object) -> dict[str, Any]:
    """Airflow entrypoint for the ML DAG's ``predict_upcoming_week`` task."""
    from xgboost import XGBClassifier, XGBRegressor

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

    type_ids = track_type_ids_for_track(hook, int(snapshot["track_id"]))
    if not type_ids:
        _LOG.warning(
            "predict_upcoming_week_task: no track_type_id mappings for track_id=%s",
            snapshot["track_id"],
        )
        return {
            "skipped": "no_track_type_mapping",
            "snapshot_id": snapshot["id"],
            "track_id": snapshot["track_id"],
        }

    strategy = blend_weight_strategy()
    target_predictions: dict[str, np.ndarray] = {}
    model_ids: dict[str, int] = {}
    resolved_scopes: dict[str, dict[str, Any]] = {}
    fallback_targets: list[str] = []
    field_size = len(rows)

    # D3: look up the per-cohort DNF classifiers FIRST so the composer
    # can fold ``P_dnf`` into every regression head. The classifiers are
    # blended across cohorts via probability average (or whatever
    # ``FS_ML_BLEND_WEIGHTS`` resolves to); cohorts without a
    # ``dnf_classifier`` row are simply skipped. When the blended
    # ``P_dnf`` is ``None`` (no cohort has a DNF classifier yet),
    # composition is a no-op and the pipeline collapses to the legacy
    # single-head behavior for full backward compatibility.
    dnf_blended, dnf_meta, dnf_model_id = _predict_values_for_target(
        hook,
        target=DNF_TARGET_KEY,
        series_id=snapshot["series_id"],
        track_id=int(snapshot["track_id"]),
        rows=rows,
        booster_cls=XGBClassifier,
        type_ids=type_ids,
        strategy=strategy,
        task="classifier",
    )
    if dnf_blended is not None and dnf_meta is not None:
        # Clip probabilities to [0, 1] in case any classifier underflows
        # at the edges; the compose formulas assume a valid probability.
        dnf_blended = np.clip(dnf_blended, 0.0, 1.0)
        resolved_scopes[DNF_TARGET_KEY] = dnf_meta
        model_ids[DNF_TARGET_KEY] = dnf_model_id
        _LOG.info(
            "predict_upcoming_week_task: dnf_classifier blend "
            "model_ids=%s weights=%s p_dnf_mean=%.4f",
            dnf_meta["model_ids"],
            dnf_meta["weights"],
            float(np.mean(dnf_blended)),
        )

    for target in DEFAULT_TARGETS:
        preds, resolved_meta, model_id = _predict_values_for_target(
            hook,
            target=target,
            series_id=snapshot["series_id"],
            track_id=int(snapshot["track_id"]),
            rows=rows,
            booster_cls=XGBRegressor,
            type_ids=type_ids,
            strategy=strategy,
        )
        if preds is None or model_id is None or resolved_meta is None:
            if target == PRIMARY_TARGET:
                _LOG.warning(
                    "predict_upcoming_week_task: no blend models for primary target=%s; aborting",
                    target,
                )
                return {
                    "skipped": "no_blend_models",
                    "snapshot_id": snapshot["id"],
                    "track_id": snapshot["track_id"],
                    "track_type_ids": type_ids,
                    "target": target,
                }
            # B3: prefer the cohort training mean over a hard floor default.
            # Falling back to 0.0 underpredicts DFS points (the secondaries
            # are non-negative counts); the cohort mean is the
            # bias-corrected baseline learned at fit time.
            cohort_mean = _cohort_train_target_mean(
                hook,
                target=target,
                series_id=snapshot["series_id"],
                track_type_ids=type_ids,
            )
            floor = _FALLBACK_TARGET_DEFAULTS.get(target, 0.0)
            default_value = cohort_mean if cohort_mean is not None else floor
            fallback_source = (
                "cohort_train_target_mean" if cohort_mean is not None else "floor_default"
            )
            target_predictions[target] = np.full(len(rows), default_value, dtype=float)
            fallback_targets.append(target)
            _LOG.warning(
                "predict_upcoming_week_task: no blend models for target=%s; "
                "using fallback=%s (source=%s, floor=%s)",
                target,
                default_value,
                fallback_source,
                floor,
            )
            continue
        target_predictions[target] = preds
        model_ids[target] = model_id
        resolved_scopes[target] = resolved_meta
        _LOG.info(
            "predict_upcoming_week_task: blend target=%s series_id=%s track_id=%s "
            "track_type_ids=%s model_ids=%s model_kinds=%s weights=%s strategy=%s",
            target,
            resolved_meta["series_id"],
            resolved_meta["track_id"],
            resolved_meta["track_type_ids"],
            resolved_meta["model_ids"],
            resolved_meta.get("model_kinds"),
            resolved_meta["weights"],
            resolved_meta["blend_strategy_used"],
        )

    if PRIMARY_TARGET not in model_ids:
        return {
            "skipped": "no_blend_models",
            "snapshot_id": snapshot["id"],
            "track_id": snapshot["track_id"],
            "track_type_ids": type_ids,
            "target": PRIMARY_TARGET,
        }

    # D3 two-stage compose: when ``dnf_blended`` is present, every
    # regression head represents ``E[y | clean]`` (because the trainer
    # tagged it ``regression_clean`` and fit only on the non-DNF
    # subset). Compose into the marginal expectation:
    #   E[finish] = P_dnf * field_size + (1 - P_dnf) * E[finish | clean]
    #   E[secondary] = (1 - P_dnf) * E[secondary | clean]
    # When ``dnf_blended`` is ``None`` (no classifier exists for this
    # cohort yet), each compose helper is a no-op pass-through so the
    # predict task remains backward-compatible with legacy single-head
    # ``regression`` models still in the registry.
    two_stage_targets: list[str] = []
    if dnf_blended is not None:
        for target in list(target_predictions.keys()):
            clean = target_predictions[target]
            primary_kinds = (resolved_scopes.get(target) or {}).get("model_kinds", []) or []
            composed_from_clean = any(
                k == MODEL_KIND_REGRESSION_CLEAN for k in primary_kinds
            )
            if target == PRIMARY_TARGET:
                target_predictions[target] = compose_two_stage_primary(
                    clean, dnf_blended, field_size
                )
            else:
                target_predictions[target] = compose_two_stage_secondary(
                    clean, dnf_blended
                )
            if composed_from_clean:
                two_stage_targets.append(target)

    # D1: post-hoc rank reconciliation. Replace the blended float for the
    # primary target with an integer rank ``1..field_size`` so DFS scoring
    # / Monte Carlo see a physically valid permutation, and stash the raw
    # float under ``r_finish_position_raw`` for audit + drift tracking.
    primary_raw_floats, primary_integer_ranks = reconcile_primary_to_integer_ranks(
        target_predictions[PRIMARY_TARGET]
    )
    target_predictions[PRIMARY_TARGET] = primary_integer_ranks.astype(float)

    # E4: gather drift warnings from each cohort that contributed to the
    # primary head. The primary blend is the right anchor here because
    # every regression head ultimately feeds DFS scoring through it, and
    # we don't want N copies of the same warning per target.
    primary_resolved = resolved_scopes.get(PRIMARY_TARGET) or {}
    drift_warnings: list[dict[str, Any]] = []
    for entry in primary_resolved.get("_cohort_entries", []) or []:
        try:
            drift_warnings.extend(_drift_warnings_for_cohort(rows, model_entry=entry))
        except Exception:  # noqa: BLE001 — drift surface must never tank predict
            _LOG.exception(
                "predict_upcoming_week_task: drift guard raised for ml_model_id=%s; "
                "continuing without warnings for this cohort",
                entry.get("id"),
            )

    # E1: weight-averaged conformal interval per target, keyed by the
    # primary blend's weights when present. Secondary targets reuse the
    # same set of cohorts (one model per (series, track_type, target))
    # so the interval shape lines up with Monte Carlo's σ source.
    conformal_intervals: dict[str, dict[str, float]] = {}
    for target_name, target_meta in resolved_scopes.items():
        cohort_entries = (target_meta or {}).get("_cohort_entries") or []
        target_weights = (target_meta or {}).get("weights") or []
        if not cohort_entries or not target_weights:
            continue
        averaged = _weight_average_intervals(cohort_entries, target_weights)
        if averaged is not None:
            conformal_intervals[target_name] = {
                k: round(float(v), 6) for k, v in averaged.items()
            }

    # E3 lineage: build a single ``blend_metadata`` dict per row. Per-row
    # variation only kicks in via ``drift_warnings`` once a per-driver
    # drift surface is added (out of scope for E4); for now every row in
    # this snapshot shares the same lineage blob, which is what makes
    # the column auditable as a "this snapshot's blend setup" record.
    def _strip_internal(meta: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in meta.items() if not k.startswith("_")}

    blend_metadata_base: dict[str, Any] = {
        "blend_strategy_requested": strategy,
        "blend_strategy_default": DEFAULT_BLEND_STRATEGY,
        "primary_model_id": (
            int(model_ids[PRIMARY_TARGET]) if PRIMARY_TARGET in model_ids else None
        ),
        "track_type_ids": list(type_ids),
        "fallback_targets": list(fallback_targets),
        "two_stage_targets": list(two_stage_targets),
        "dnf_classifier_present": dnf_blended is not None,
        "resolved_scopes": {
            target_name: _strip_internal(meta)
            for target_name, meta in resolved_scopes.items()
        },
        "conformal_intervals": conformal_intervals,
        "drift_warnings": drift_warnings,
    }

    primary_model_id = model_ids[PRIMARY_TARGET]
    insert_params: list[tuple[Any, ...]] = []
    for i, row in enumerate(rows):
        predicted_targets = {
            target: float(predictions[i]) for target, predictions in target_predictions.items()
        }
        predicted_targets[PRIMARY_TARGET_RAW_KEY] = float(primary_raw_floats[i])
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
                json.dumps(blend_metadata_base),
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
                    predicted_dfs_points,
                    blend_metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                insert_params,
            )

    hook.run_in_transaction(_write_predictions)
    inserted = len(insert_params)

    _LOG.info(
        "predict_upcoming_week_task: snapshot_id=%s week_id=%s rows=%s primary_model=%s "
        "blend_strategy=%s track_type_ids=%s",
        snapshot["id"],
        snapshot["week_id"],
        inserted,
        primary_model_id,
        strategy,
        type_ids,
    )
    return {
        "skipped": None,
        "snapshot_id": snapshot["id"],
        "series_id": snapshot["series_id"],
        "week_id": snapshot["week_id"],
        "track_id": snapshot["track_id"],
        "track_type_ids": type_ids,
        "primary_model_id": primary_model_id,
        "model_ids": model_ids,
        "resolved_scopes": {
            target_name: _strip_internal(meta)
            for target_name, meta in resolved_scopes.items()
        },
        "fallback_targets": fallback_targets,
        "two_stage_targets": two_stage_targets,
        "dnf_classifier_present": dnf_blended is not None,
        "blend_strategy_requested": strategy,
        "blend_strategy_default": DEFAULT_BLEND_STRATEGY,
        "predictions_inserted": inserted,
        "drift_warnings": drift_warnings,
        "conformal_intervals": conformal_intervals,
    }
