"""Pure-Python training pipeline for per-target XGBoost regressors.

This module intentionally has zero Airflow / MySQL imports so the synthetic-data
pipeline tests can exercise the full Boruta + tuning + fit flow without any
external services. The Airflow task in :mod:`include.nascar.ml.train_task`
wraps these helpers with database IO.
"""

from __future__ import annotations

import gc
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from include.nascar.ml.dnf import (
    DNF_TARGET_KEY,
    MODEL_KIND_DNF_CLASSIFIER,
    MODEL_KIND_REGRESSION,
    MODEL_KIND_REGRESSION_CLEAN,
)
from include.nascar.ml.feature_matrix import build_feature_matrix, feature_fill_values
from include.nascar.ml.scoring import compute_dfs_points_from_targets
from include.nascar.ml.xgb_boruta import filter_columns, select_features
from include.nascar.ml.xgb_tuning import (
    TASK_CLASSIFIER,
    TASK_RANKER,
    TASK_REGRESSION,
    tune_xgb,
)

_LOG = logging.getLogger(__name__)

DEFAULT_TARGETS: tuple[str, ...] = (
    "r_finish_position",
    "r_total_fastest_laps",
    "r_total_laps_led",
)

PRIMARY_TARGET = "r_finish_position"


@dataclass
class TrainedTargetModel:
    target: str
    selected_features: list[str]
    hyperparams: dict[str, Any]
    boruta_metadata: dict[str, Any]
    tuning_metadata: dict[str, Any]
    holdout_mae: float | None
    holdout_predictions: np.ndarray | None
    # B3: mean of ``y_train`` for this target; predict-time fallback when a
    # cohort lacks a usable model for a secondary target. ``None`` when no
    # training labels were present (caller should treat as 0.0).
    train_target_mean: float | None = None
    # D3: model_kind discriminator (``regression`` for legacy single-head
    # fits, ``regression_clean`` when paired with a DNF classifier sibling,
    # ``dnf_classifier`` for the binary ``r_is_dnf`` head).
    model_kind: str = MODEL_KIND_REGRESSION
    # D3: holdout metrics for the binary classifier head; ``None`` for
    # regression heads which advertise ``holdout_mae`` instead.
    holdout_log_loss: float | None = None
    holdout_auc: float | None = None
    # E1: holdout residual quantiles for split-conformal intervals.
    # ``None`` when the cohort has no usable holdout slice (cold start /
    # tiny cohort). Keys are the quantile labels ``q05`` / ``q10`` /
    # ``q50`` / ``q90`` / ``q95``; values are the absolute residuals on
    # the holdout slice. Regression heads compute against the model's
    # raw target labels; the classifier head measures
    # ``|proba - label|`` so a single column can carry both families.
    holdout_residual_quantiles: dict[str, float] | None = None
    artifact_path: Path | None = None
    model: Any = field(repr=False, default=None)


_RESIDUAL_QUANTILES: tuple[tuple[str, float], ...] = (
    ("q05", 0.05),
    ("q10", 0.10),
    ("q50", 0.50),
    ("q90", 0.90),
    ("q95", 0.95),
)


def _compute_residual_quantiles(
    y_true: np.ndarray, y_pred: np.ndarray
) -> dict[str, float] | None:
    """E1: ``|y_true - y_pred|`` quantiles for the cohort-level conformal store.

    Returns ``None`` when fewer than two paired observations exist —
    a single residual yields a degenerate (q05 == q95) interval that
    inflates downstream σ. The classifier head feeds in
    ``|proba - label|`` so probability calibration drift also fills the
    same column.
    """
    if y_true.size < 2 or y_pred.size < 2 or y_true.size != y_pred.size:
        return None
    residuals = np.abs(y_true - y_pred)
    return {
        label: float(np.quantile(residuals, q))
        for label, q in _RESIDUAL_QUANTILES
    }


def _build_target_vector(frame: list[dict[str, Any]], target: str) -> np.ndarray:
    y = np.zeros(len(frame), dtype=float)
    for i, row in enumerate(frame):
        value = (row.get("targets") or {}).get(target)
        if value is not None:
            y[i] = float(value)
    return y


def split_train_dev_holdout_masks(
    frame: list[dict[str, Any]],
    *,
    dev_weeks: int = 0,
    holdout_weeks: int = 1,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Three-way temporal split: ``(train_mask, dev_mask, holdout_mask)``.

    Weeks are sorted ascending; the trailing ``holdout_weeks`` distinct
    ``week_id`` values are the **holdout** slice (untouched until final
    eval), the ``dev_weeks`` weeks immediately preceding holdout are the
    **dev** slice (hyperparameter selection / early stopping), and the
    remainder is **train**.

    Small-cohort fallbacks:

    - ``len(weeks) <= holdout_weeks``: train = everything; dev and holdout
      both empty. (Bootstrapping case; eval helpers return ``n=0``.)
    - ``holdout_weeks < len(weeks) <= holdout_weeks + dev_weeks``: train =
      everything not in holdout; dev empty; holdout = trailing weeks.
      Equivalent to the legacy two-way split.
    - Otherwise: full three-way split.

    The three returned masks are always pairwise disjoint and cover all
    rows in ``frame``.
    """
    n = len(frame)
    if n == 0:
        empty = np.array([], dtype=bool)
        return empty, empty.copy(), empty.copy()

    weeks = sorted({int(row["week_id"]) for row in frame})
    if len(weeks) <= holdout_weeks:
        return (
            np.ones(n, dtype=bool),
            np.zeros(n, dtype=bool),
            np.zeros(n, dtype=bool),
        )

    hold_set: set[int] = set(weeks[-holdout_weeks:]) if holdout_weeks > 0 else set()
    remaining = [w for w in weeks if w not in hold_set]

    if dev_weeks <= 0 or len(remaining) <= dev_weeks:
        # Not enough weeks to carve out a dev slice — collapse to the legacy
        # two-way (train+holdout) split. Empty dev mask preserves the
        # disjoint-union contract.
        train_mask = np.array([int(r["week_id"]) not in hold_set for r in frame])
        holdout_mask = np.array([int(r["week_id"]) in hold_set for r in frame])
        return train_mask, np.zeros(n, dtype=bool), holdout_mask

    dev_set: set[int] = set(remaining[-dev_weeks:])
    train_mask = np.array(
        [
            int(r["week_id"]) not in hold_set and int(r["week_id"]) not in dev_set
            for r in frame
        ]
    )
    dev_mask = np.array([int(r["week_id"]) in dev_set for r in frame])
    holdout_mask = np.array([int(r["week_id"]) in hold_set for r in frame])
    return train_mask, dev_mask, holdout_mask


def split_holdout_mask(
    frame: list[dict[str, Any]],
    holdout_weeks: int = 1,
) -> tuple[np.ndarray, np.ndarray]:
    """Mark the trailing ``holdout_weeks`` distinct week_ids as the holdout set.

    Backwards-compatible two-way wrapper around
    :func:`split_train_dev_holdout_masks` that folds any (empty) dev slice
    into the train mask. Use the three-way function directly when you need
    a dev slice for hyperparameter selection / early stopping.
    """
    train, dev, holdout = split_train_dev_holdout_masks(
        frame,
        dev_weeks=0,
        holdout_weeks=holdout_weeks,
    )
    return train | dev, holdout


def _is_dnf_label_vector(frame: list[dict[str, Any]]) -> np.ndarray | None:
    """Return the ``r_is_dnf`` label vector (0/1) or ``None`` if absent.

    D3 derives DNF at snapshot build into ``ml_labels.targets_json``; the
    trainer reads it from ``row['targets']``. If no row carries the key,
    the cohort frame predates the two-stage rollout and falls back to the
    legacy single-head training path.
    """
    has_key = any(DNF_TARGET_KEY in (row.get("targets") or {}) for row in frame)
    if not has_key:
        return None
    return _build_target_vector(frame, DNF_TARGET_KEY)


def _rank_groups_and_relevance(
    frame: list[dict[str, Any]],
    row_mask: np.ndarray,
    finish_y: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Build ``(ordered_indices, group_sizes, relevance_labels)`` for ``rank:pairwise``.

    D2: XGBRanker needs the rows grouped contiguously by ``week_id`` with
    a parallel ``group`` array of per-week field sizes. The relevance
    label is ``field_size_week - r_finish_position`` so a higher
    relevance means a better predicted finish (XGBoost ranks descending
    by score).

    ``ordered_indices`` is the row-order the caller must apply to ``X``
    and ``y`` before handing them to ``tune_xgb`` — ranker fits are
    order-sensitive within each group.
    """
    selected_indices = np.flatnonzero(row_mask)
    if selected_indices.size == 0:
        return (
            np.zeros(0, dtype=np.int64),
            np.zeros(0, dtype=np.int64),
            np.zeros(0, dtype=float),
        )
    week_ids = np.array(
        [int(frame[i]["week_id"]) for i in selected_indices], dtype=np.int64
    )

    # Stable sort by ``week_id`` so each group is contiguous; rows within a
    # week keep their original order, which keeps the joint
    # ``X[ordered_indices]`` / ``y[ordered_indices]`` aligned for fit.
    order = np.argsort(week_ids, kind="stable")
    ordered_indices = selected_indices[order]
    sorted_week_ids = week_ids[order]

    _unique_weeks, group_sizes = np.unique(sorted_week_ids, return_counts=True)
    group_sizes = group_sizes.astype(np.int64)

    relevance = np.zeros_like(ordered_indices, dtype=float)
    offset = 0
    for size in group_sizes:
        slice_idx = ordered_indices[offset : offset + size]
        finishes = finish_y[slice_idx]
        # NASCAR convention: lower ``r_finish_position`` = better finish.
        # ``rank:pairwise`` ranks DESCENDING on the relevance label, so the
        # transformation flips the sign and shifts by the field size so the
        # winner gets the highest relevance.
        relevance[offset : offset + size] = float(size) - finishes
        offset += int(size)
    return ordered_indices, group_sizes, relevance


def _primary_rank_objective_active() -> bool:
    """D2: deferred lookup of the env flag so unit tests can monkeypatch it."""
    from include.nascar.ml.model_scope import primary_rank_objective_enabled

    return primary_rank_objective_enabled()


def train_models_for_targets(
    frame: list[dict[str, Any]],
    *,
    targets: tuple[str, ...] = DEFAULT_TARGETS,
    holdout_weeks: int = 1,
    dev_weeks: int = 0,
    artifact_dir: Path | None = None,
    random_state: int = 42,
    boruta_max_iter: int = 50,
    feature_allowlist: list[str] | None = None,
) -> list[TrainedTargetModel]:
    """
    Train one XGBoost head per target on ``frame`` and return trained models.

    The caller is responsible for persisting the resulting artifacts/metadata into
    ``ml_feature_sets`` / ``ml_models`` rows; this function only does the math and
    optionally writes XGBoost JSON artifacts to ``artifact_dir`` (one file per
    target named ``<target>.json``).

    ``feature_allowlist`` (when provided) restricts the candidate Boruta input
    columns. Train-task uses it to enforce per-scope feature families (``*_pt_*``
    for track models, ``*_tt_<id>_*`` for track-type models, etc.).

    When ``dev_weeks > 0`` the frame is split three ways (train/dev/holdout)
    and the dev slice is forwarded to ``tune_xgb`` for XGBoost early stopping
    (A4). When the cohort lacks enough weeks for a dev split, the wrapper
    silently collapses back to the legacy two-way split.

    D3 two-stage DNF:

    - If any ``targets`` entry is ``r_is_dnf`` **and** the frame carries
      that label, that target trains an :class:`XGBClassifier` head
      (``binary:logistic``) on the full train slice. Its
      ``TrainedTargetModel.model_kind`` is ``dnf_classifier``; the
      holdout metrics are ``log_loss`` and ``auc`` (``holdout_mae`` is
      left ``None``).
    - The other (regression) targets train on the **non-DNF subset**
      of the train / dev / holdout masks so they learn
      ``E[y | clean]``. Each one's ``model_kind`` is
      ``regression_clean``; predict-time uses the DNF classifier to
      compose the marginal expectation.
    - When no ``r_is_dnf`` label is present the trainer collapses to the
      legacy single-head behavior: every regression head is trained on
      every row and tagged ``model_kind='regression'``. This keeps the
      pipeline backward-compatible with cohort frames built before D3
      landed.
    """
    from sklearn.metrics import mean_absolute_error

    if not frame:
        raise ValueError("train_models_for_targets: empty frame")

    candidate_keys = {key for row in frame for key in (row.get("features") or {}).keys()}
    if feature_allowlist is not None:
        allow = set(feature_allowlist)
        candidate_keys &= allow
    feature_names = sorted(candidate_keys)
    if not feature_names:
        raise ValueError("train_models_for_targets: no feature keys present in frame")

    fill_values = feature_fill_values(frame, feature_names)
    # B2: train tasks defer to the ``FS_ML_STRICT_FEATURES`` env var. When
    # set (=1) a residual NaN / inf raises :class:`FeatureValueError` instead
    # of silently learning around an upstream snapshot-build regression.
    # Default (env unset / 0) preserves the legacy zero-fill behavior while
    # logging the per-feature counts.
    X = build_feature_matrix(frame, feature_names, fill_values=fill_values)
    train_mask, dev_mask, holdout_mask = split_train_dev_holdout_masks(
        frame,
        dev_weeks=dev_weeks,
        holdout_weeks=holdout_weeks,
    )
    has_holdout = bool(holdout_mask.any())
    has_dev = bool(dev_mask.any())

    # D3: detect whether the frame supports the two-stage design. ``dnf_y``
    # is the row-wise label vector (0/1); ``clean_mask`` selects rows the
    # regression heads should fit on (clean races only).
    dnf_y = _is_dnf_label_vector(frame)
    two_stage = dnf_y is not None
    clean_mask = (
        (dnf_y < 0.5)
        if two_stage
        else np.ones(len(frame), dtype=bool)
    )
    regression_kind = (
        MODEL_KIND_REGRESSION_CLEAN if two_stage else MODEL_KIND_REGRESSION
    )
    # D2: cache the env flag once so monkeypatch in tests + production share
    # a single resolution. The pairwise ranker is mutually exclusive with
    # the two-stage DNF compose (ranker outputs scores, not finishes) so we
    # log and disable the flag when both would otherwise stack.
    primary_uses_ranker = _primary_rank_objective_active()
    if primary_uses_ranker and two_stage:
        _LOG.warning(
            "train_models_for_targets: FS_ML_PRIMARY_RANK_OBJECTIVE and two-stage "
            "DNF (frame has r_is_dnf labels) can't both run; rank objective "
            "expects raw finish-position labels, not clean-only conditionals. "
            "Falling back to reg:absoluteerror for the primary head."
        )
        primary_uses_ranker = False

    results: list[TrainedTargetModel] = []

    for target in targets:
        is_dnf_target = target == DNF_TARGET_KEY
        is_primary_ranker = (
            primary_uses_ranker and target == PRIMARY_TARGET and not two_stage
        )
        if is_dnf_target and not two_stage:
            _LOG.warning(
                "train_models_for_targets: skipping target=%s — no ``r_is_dnf`` "
                "label found in frame (likely a cohort built before D3 landed)",
                target,
            )
            continue

        if is_dnf_target:
            row_mask = np.ones(len(frame), dtype=bool)
            task = TASK_CLASSIFIER
            model_kind = MODEL_KIND_DNF_CLASSIFIER
        elif is_primary_ranker:
            row_mask = np.ones(len(frame), dtype=bool)
            task = TASK_RANKER
            model_kind = MODEL_KIND_REGRESSION
        else:
            row_mask = clean_mask
            task = TASK_REGRESSION
            model_kind = regression_kind

        y = _build_target_vector(frame, target)
        target_train_mask = train_mask & row_mask
        target_dev_mask = dev_mask & row_mask
        target_holdout_mask = holdout_mask & row_mask
        y_train = y[target_train_mask]

        # B3: train_target_mean = mean of the training labels (post-mask) for
        # this target; persisted on ``ml_models.train_target_mean`` and used
        # at predict time when a secondary target has no usable cohort
        # model. ``None`` when the train slice is empty (caller treats as 0).
        train_target_mean: float | None = (
            float(np.mean(y_train)) if y_train.size > 0 else None
        )

        selected, boruta_meta = select_features(
            X[target_train_mask],
            y_train,
            feature_names,
            random_state=random_state,
            max_iter=boruta_max_iter,
        )

        X_sel = filter_columns(X, feature_names, selected)
        X_train = X_sel[target_train_mask]

        # E4: train-time per-feature summary stats persisted alongside
        # ``boruta_metadata`` so the predict task can Z-score the predict
        # snapshot's per-feature distribution against this baseline.
        # ``mean`` and ``std`` come from numpy; ``p05`` / ``p95`` are
        # robust tail markers. ``std`` is floor-clamped to a tiny epsilon
        # so the predict-time Z-score never blows up on a constant
        # column.
        boruta_meta = dict(boruta_meta or {})
        if X_train.size > 0:
            train_feature_stats: dict[str, dict[str, float]] = {}
            mean_vec = np.nanmean(X_train, axis=0)
            std_vec = np.nanstd(X_train, axis=0)
            p05_vec = np.nanpercentile(X_train, 5, axis=0)
            p95_vec = np.nanpercentile(X_train, 95, axis=0)
            for j, name in enumerate(selected):
                train_feature_stats[name] = {
                    "mean": float(mean_vec[j]),
                    "std": float(std_vec[j]),
                    "p05": float(p05_vec[j]),
                    "p95": float(p95_vec[j]),
                }
            boruta_meta["train_feature_stats"] = train_feature_stats

        # A3: ``tune_xgb`` returns a refit ``best_estimator_`` directly,
        # so we skip the manual rebuild + double-fit.
        # A4: when a dev slice exists, hand it to ``tune_xgb`` which fits
        # the final model with ``early_stopping_rounds`` watched on dev.
        target_has_dev = has_dev and bool(target_dev_mask.any())
        if target_has_dev:
            X_dev = X_sel[target_dev_mask]
            y_dev = y[target_dev_mask]
        else:
            X_dev = None
            y_dev = None

        if is_primary_ranker:
            # D2: re-order X/y by week_id and build per-week qid groups
            # before handing them to the ranker. The relevance label
            # replaces the raw finish position so XGBoost ranks the
            # winner highest.
            train_ordered_idx, train_groups, train_relevance = (
                _rank_groups_and_relevance(frame, target_train_mask, y)
            )
            X_train = X_sel[train_ordered_idx]
            y_train = train_relevance
            if target_has_dev:
                dev_ordered_idx, dev_groups, dev_relevance = (
                    _rank_groups_and_relevance(frame, target_dev_mask, y)
                )
                X_dev = X_sel[dev_ordered_idx]
                y_dev = dev_relevance
            else:
                dev_groups = None
            model, best_params, tuning_meta = tune_xgb(
                X_train,
                y_train,
                random_state=random_state,
                X_dev=X_dev,
                y_dev=y_dev,
                task=task,
                group=train_groups,
                group_dev=dev_groups,
            )
        else:
            model, best_params, tuning_meta = tune_xgb(
                X_train,
                y_train,
                random_state=random_state,
                X_dev=X_dev,
                y_dev=y_dev,
                task=task,
            )

        holdout_mae: float | None = None
        holdout_log_loss: float | None = None
        holdout_auc: float | None = None
        holdout_predictions: np.ndarray | None = None
        residual_quantiles: dict[str, float] | None = None
        if has_holdout:
            # D3: regression heads predict on the **full** holdout (even
            # DNF rows) so downstream DFS-points evaluators can compose
            # ``E[finish] = P_dnf * field_size + (1 - P_dnf) * E[finish|clean]``
            # over a single positionally aligned vector. The training
            # mask is still clean-only — the model only learned the
            # clean-race conditional — but ``predict()`` happily
            # extrapolates and the composer's ``(1 - P_dnf)`` weights it
            # down on rows the classifier flags as likely DNFs.
            X_holdout = X_sel[holdout_mask]
            y_holdout = y[holdout_mask]
            if is_dnf_target:
                # D3 classifier: store class probabilities so predict / Monte
                # Carlo see a calibrated ``P(DNF)``; log_loss / auc populate
                # the corresponding ``ml_model_evaluations`` rows.
                from sklearn.metrics import log_loss, roc_auc_score

                proba = model.predict_proba(X_holdout)[:, 1]
                holdout_predictions = proba
                holdout_log_loss = float(log_loss(y_holdout, proba, labels=[0, 1]))
                if len(np.unique(y_holdout)) > 1:
                    holdout_auc = float(roc_auc_score(y_holdout, proba))
                # E1: classifier residual = |proba - label|. Calibration
                # drift surfaces in the same column the regression heads
                # use so cross-target tooling stays uniform.
                residual_quantiles = _compute_residual_quantiles(y_holdout, proba)
            elif is_primary_ranker:
                # D2 eval continuity: the ranker outputs relevance scores,
                # not finish positions. Map them back to integer ranks per
                # week (highest score = rank 1), then compute MAE against
                # the raw ``r_finish_position`` labels so the published
                # ``ml_model_evaluations.metric='mae'`` row stays
                # comparable with the regression path's MAE for blending.
                raw_scores = model.predict(X_holdout)
                holdout_indices = np.flatnonzero(holdout_mask)
                week_ids_h = np.array(
                    [int(frame[i]["week_id"]) for i in holdout_indices],
                    dtype=np.int64,
                )
                predicted_finishes = np.empty(raw_scores.shape, dtype=float)
                for week in np.unique(week_ids_h):
                    in_week = week_ids_h == week
                    week_scores = raw_scores[in_week]
                    # argsort DESC: highest score -> rank 1.
                    order = np.argsort(-week_scores, kind="stable")
                    week_ranks = np.empty_like(order)
                    week_ranks[order] = np.arange(1, len(order) + 1)
                    predicted_finishes[in_week] = week_ranks
                holdout_predictions = predicted_finishes
                holdout_mae = float(
                    mean_absolute_error(y_holdout, predicted_finishes)
                )
                # E1: residuals measured against the reconciled integer
                # ranks so the conformal interval matches the score-space
                # the predict task ultimately persists.
                residual_quantiles = _compute_residual_quantiles(
                    y_holdout, predicted_finishes
                )
            else:
                holdout_predictions = model.predict(X_holdout)
                # D3: regression MAE on the clean-race subset of the holdout
                # is the metric that matches the model's training
                # distribution. Composing with the classifier's ``P_dnf``
                # would conflate two heads' errors into one number; the
                # composed DFS-points MAE lives on
                # :func:`holdout_dfs_eval_metrics` and ``_holdout_track_eval``
                # which already iterate per-row.
                if two_stage:
                    clean_holdout_mask = clean_mask[holdout_mask]
                    if clean_holdout_mask.any():
                        holdout_mae = float(
                            mean_absolute_error(
                                y_holdout[clean_holdout_mask],
                                holdout_predictions[clean_holdout_mask],
                            )
                        )
                    # E1: conformal residuals on the clean subset only;
                    # the DNF branch carries its own quantiles via the
                    # classifier head so this column always represents
                    # ``E[y | clean]`` calibration.
                    residual_quantiles = _compute_residual_quantiles(
                        y_holdout[clean_holdout_mask],
                        holdout_predictions[clean_holdout_mask],
                    )
                else:
                    holdout_mae = float(
                        mean_absolute_error(y_holdout, holdout_predictions)
                    )
                    residual_quantiles = _compute_residual_quantiles(
                        y_holdout, holdout_predictions
                    )

        artifact_path: Path | None = None
        if artifact_dir is not None:
            artifact_dir.mkdir(parents=True, exist_ok=True)
            artifact_path = artifact_dir / f"{target}.json"
            model.save_model(str(artifact_path))

        results.append(
            TrainedTargetModel(
                target=target,
                selected_features=selected,
                hyperparams=best_params,
                boruta_metadata=boruta_meta,
                tuning_metadata=tuning_meta,
                holdout_mae=holdout_mae,
                holdout_predictions=holdout_predictions,
                train_target_mean=train_target_mean,
                model_kind=model_kind,
                holdout_log_loss=holdout_log_loss,
                holdout_auc=holdout_auc,
                holdout_residual_quantiles=residual_quantiles,
                artifact_path=artifact_path,
                model=model,
            )
        )

        _LOG.info(
            "train_models_for_targets: target=%s task=%s model_kind=%s "
            "n_train=%s n_dev=%s n_holdout=%s selected=%s holdout_mae=%s "
            "holdout_log_loss=%s holdout_auc=%s best_iteration=%s",
            target,
            task,
            model_kind,
            int(target_train_mask.sum()),
            int(target_dev_mask.sum()) if target_has_dev else 0,
            int(target_holdout_mask.sum()) if has_holdout else 0,
            len(selected),
            holdout_mae,
            holdout_log_loss,
            holdout_auc,
            tuning_meta.get("best_iteration"),
        )

        # Phase 3: per-target memory hygiene. ``X_sel`` and the per-mask
        # slices (``X_train`` / ``X_dev`` / ``X_holdout``) dominate the
        # loop's resident memory; releasing them between iterations keeps
        # the peak at ~single-target matrix size instead of accumulating
        # ``N_targets`` copies on the heap. The returned ``model`` and
        # ``holdout_predictions`` arrays stay live via the
        # :class:`TrainedTargetModel` reference, so the public outputs
        # are unaffected.
        del X_sel, X_train, y_train, X_dev, y_dev
        del target_train_mask, target_dev_mask, target_holdout_mask, y, row_mask
        if has_holdout:
            del X_holdout, y_holdout
        gc.collect()

    return results


def _two_stage_holdout_predictions(
    frame: list[dict[str, Any]],
    trained: list[TrainedTargetModel],
    holdout_indices: np.ndarray,
) -> dict[str, np.ndarray] | None:
    """Compose ``E[y]`` per target on the holdout slice for DFS-points eval.

    D3: when a ``dnf_classifier`` head is present, regression heads carry
    ``E[y | clean]`` on the full holdout and the classifier carries
    ``P_dnf``. We fold them into a single ``target -> E[y]`` map so the
    DFS-points evaluators see a marginal expectation and don't have to
    know about the two-stage shape. ``r_finish_position`` collapses to
    the field size on a DNF (NASCAR places DNFs at the back of the
    field); secondary counts collapse to zero.

    Returns ``None`` when any head is missing predictions (caller's cue
    to skip DFS eval entirely).
    """
    if holdout_indices.size == 0:
        return None

    dnf_entry: TrainedTargetModel | None = None
    regression_entries: list[TrainedTargetModel] = []
    for entry in trained:
        if entry.holdout_predictions is None:
            return None
        if entry.target == DNF_TARGET_KEY:
            dnf_entry = entry
        else:
            regression_entries.append(entry)

    # Holdout weeks may span multiple races; the NASCAR convention puts DNFs
    # at the back of *that race's* field. We compute per-row field size as
    # the number of holdout rows sharing the same ``week_id`` so the
    # composed primary collapses to a sensible per-week rank.
    week_ids = np.array(
        [int(frame[i].get("week_id") or 0) for i in holdout_indices]
    )
    week_field_size: dict[int, int] = {}
    for wid in week_ids:
        week_field_size[int(wid)] = week_field_size.get(int(wid), 0) + 1
    field_sizes = np.array([week_field_size[int(w)] for w in week_ids], dtype=float)

    composed: dict[str, np.ndarray] = {}
    if dnf_entry is not None:
        dnf_proba = np.asarray(dnf_entry.holdout_predictions, dtype=float)
        composed[DNF_TARGET_KEY] = dnf_proba
        for entry in regression_entries:
            clean = np.asarray(entry.holdout_predictions, dtype=float)
            if entry.target == PRIMARY_TARGET:
                composed[entry.target] = dnf_proba * field_sizes + (1.0 - dnf_proba) * clean
            else:
                composed[entry.target] = (1.0 - dnf_proba) * clean
    else:
        for entry in regression_entries:
            composed[entry.target] = np.asarray(entry.holdout_predictions, dtype=float)
    return composed


def predicted_dfs_mae_on_holdout(
    frame: list[dict[str, Any]],
    trained: list[TrainedTargetModel],
    scoring_rules: dict[str, Any],
    *,
    holdout_weeks: int = 1,
) -> tuple[float | None, int]:
    """Compute MAE between predicted and observed DFS points across the holdout set."""
    from include.nascar.ml.scoring import compute_dfs_points_from_targets

    _train_mask, holdout_mask = split_holdout_mask(frame, holdout_weeks=holdout_weeks)
    holdout_indices = np.flatnonzero(holdout_mask)
    if holdout_indices.size == 0:
        return None, 0

    target_to_preds = _two_stage_holdout_predictions(frame, trained, holdout_indices)
    if target_to_preds is None:
        return None, 0

    abs_errors: list[float] = []
    for pos, frame_idx in enumerate(holdout_indices):
        row = frame[frame_idx]
        predicted_targets = {
            target: float(preds[pos])
            for target, preds in target_to_preds.items()
            if target != DNF_TARGET_KEY
        }
        predicted_total, _ = compute_dfs_points_from_targets(
            row.get("r_start_position"),
            predicted_targets,
            scoring_rules,
        )
        observed = float(row.get("dfs_points") or 0.0)
        abs_errors.append(abs(float(predicted_total) - observed))

    if not abs_errors:
        return None, 0
    return float(np.mean(abs_errors)), len(abs_errors)


def holdout_dfs_eval_metrics(
    frame: list[dict[str, Any]],
    trained: list[TrainedTargetModel],
    scoring_rules: dict[str, Any],
    *,
    holdout_weeks: int = 1,
) -> dict[str, float | int | None]:
    """
    Holdout metrics on total DraftKings points: MAE, RMSE, Spearman rank correlation.
    """
    _train_mask, holdout_mask = split_holdout_mask(frame, holdout_weeks=holdout_weeks)
    holdout_indices = np.flatnonzero(holdout_mask)
    if holdout_indices.size == 0:
        return {"dfs_points_mae": None, "dfs_points_rmse": None, "dfs_points_spearman": None, "n": 0}

    target_to_preds = _two_stage_holdout_predictions(frame, trained, holdout_indices)
    if target_to_preds is None:
        return {
            "dfs_points_mae": None,
            "dfs_points_rmse": None,
            "dfs_points_spearman": None,
            "n": 0,
        }

    predicted: list[float] = []
    observed: list[float] = []
    for pos, frame_idx in enumerate(holdout_indices):
        row = frame[frame_idx]
        predicted_targets = {
            target: float(preds[pos])
            for target, preds in target_to_preds.items()
            if target != DNF_TARGET_KEY
        }
        predicted_total, _ = compute_dfs_points_from_targets(
            row.get("r_start_position"),
            predicted_targets,
            scoring_rules,
        )
        predicted.append(float(predicted_total))
        observed.append(float(row.get("dfs_points") or 0.0))

    pred_arr = np.asarray(predicted, dtype=float)
    obs_arr = np.asarray(observed, dtype=float)
    mae = float(np.mean(np.abs(pred_arr - obs_arr)))
    rmse = float(np.sqrt(np.mean((pred_arr - obs_arr) ** 2)))
    if len(obs_arr) < 2 or np.std(obs_arr) == 0 or np.std(pred_arr) == 0:
        spearman: float | None = None
    else:
        pred_rank = np.argsort(np.argsort(pred_arr))
        obs_rank = np.argsort(np.argsort(obs_arr))
        spearman = float(np.corrcoef(pred_rank, obs_rank)[0, 1])
    return {
        "dfs_points_mae": mae,
        "dfs_points_rmse": rmse,
        "dfs_points_spearman": spearman,
        "n": int(len(obs_arr)),
    }
