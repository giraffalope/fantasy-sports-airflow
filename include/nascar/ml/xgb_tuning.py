"""Per-target hyperparameter tuning for the XGBoost regressors.

Runs a bounded ``RandomizedSearchCV`` with ``TimeSeriesSplit`` so validation
folds always trail the training folds in race-week order. Honours the
``FS_ML_TUNE_*`` environment variables documented in the ML DAG plan so a
one-line override is possible without a code change.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import numpy as np

_LOG = logging.getLogger(__name__)

DEFAULT_PARAMS: dict[str, Any] = {
    # B1: align the regression training loss with the CV scoring metric
    # (``neg_mean_absolute_error``) and with the published holdout ``mae`` on
    # ``ml_model_evaluations``. ``reg:absoluteerror`` minimises the same L1
    # loss the search ranks on, eliminating the squared-error <-> MAE drift
    # that biased the prior models toward outliers.
    "objective": "reg:absoluteerror",
    "n_estimators": 400,
    "max_depth": 5,
    "learning_rate": 0.05,
    "min_child_weight": 3,
    "subsample": 0.85,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.0,
    "reg_lambda": 5.0,
    "tree_method": "hist",
    "random_state": 42,
}

CLASSIFIER_DEFAULT_PARAMS: dict[str, Any] = {
    # D3: ``r_is_dnf`` is a binary classification head. Logistic loss is the
    # native fit; ``RandomizedSearchCV`` scores on ``neg_log_loss`` so the
    # search picks hyperparams that minimise the same loss the final model
    # optimises. ``binary:logistic`` produces calibrated-ish probabilities
    # via the sigmoid, which is what the predict-time two-stage composer
    # consumes (``E[finish] = P_dnf * field_size + (1 - P_dnf) * ...``).
    "objective": "binary:logistic",
    "n_estimators": 400,
    "max_depth": 5,
    "learning_rate": 0.05,
    "min_child_weight": 3,
    "subsample": 0.85,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.0,
    "reg_lambda": 5.0,
    "tree_method": "hist",
    "random_state": 42,
    "eval_metric": "logloss",
}

RANKER_DEFAULT_PARAMS: dict[str, Any] = {
    # D2: ``rank:pairwise`` optimizes the relative ordering of finishes
    # within each ``week_id`` group. The relevance label is
    # ``field_size_week - r_finish_position`` so a higher score means a
    # better predicted finish. Eval continuity on ``ml_model_evaluations``
    # is preserved by post-hoc rank reconciliation (D1) mapping scores
    # back to integer finish positions before the published MAE.
    "objective": "rank:pairwise",
    "n_estimators": 400,
    "max_depth": 5,
    "learning_rate": 0.05,
    "min_child_weight": 3,
    "subsample": 0.85,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.0,
    "reg_lambda": 5.0,
    "tree_method": "hist",
    "random_state": 42,
    "eval_metric": "ndcg",
}

PARAM_DISTRIBUTIONS: dict[str, list[Any]] = {
    "n_estimators": [200, 400, 800],
    "max_depth": [3, 5, 7],
    "learning_rate": [0.03, 0.05, 0.1],
    "min_child_weight": [1, 3, 5],
    "subsample": [0.7, 0.85, 1.0],
    "colsample_bytree": [0.6, 0.8, 1.0],
    "reg_alpha": [0.0, 0.1, 1.0],
    "reg_lambda": [1.0, 5.0, 10.0],
}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        _LOG.warning("Ignoring non-int %s=%r; using default %s", name, value, default)
        return default


def _tuning_disabled() -> bool:
    return os.environ.get("FS_ML_TUNE_DISABLE", "").strip() in {"1", "true", "True", "yes"}


def xgb_n_jobs() -> int:
    """Per-fit XGBoost ``n_jobs`` (Phase 4 memory mitigation).

    XGBoost ``tree_method='hist'`` allocates per-thread histogram buffers.
    On a 4-core worker with ``OMP_NUM_THREADS=2`` already pinned, leaving
    ``n_jobs=-1`` here would have XGBoost ignore the OpenMP cap and grab
    all 4 cores for itself. ``FS_ML_XGB_N_JOBS=2`` (the compose default)
    re-pins it to match. Set to ``1`` as the next lever when a cohort
    still spikes under the standard mitigation stack.

    Defaults to ``-1`` (legacy "use every core") when the env var is
    unset, so callers outside the configured worker keep their old
    behavior.
    """
    raw = os.environ.get("FS_ML_XGB_N_JOBS")
    if raw is None or raw.strip() == "":
        return -1
    try:
        value = int(raw)
    except ValueError:
        _LOG.warning(
            "Ignoring non-int FS_ML_XGB_N_JOBS=%r; using legacy n_jobs=-1", raw
        )
        return -1
    if value == 0:
        _LOG.warning("FS_ML_XGB_N_JOBS=0 is invalid; using legacy n_jobs=-1")
        return -1
    return value


DEFAULT_EARLY_STOPPING_ROUNDS = 20


def _env_early_stopping_rounds() -> int:
    """Patience rounds passed to ``XGBRegressor(early_stopping_rounds=...)``.

    ``FS_ML_EARLY_STOPPING_ROUNDS=0`` disables early stopping (training runs the
    full ``n_estimators``); any positive value enables it.
    """
    return max(0, _env_int("FS_ML_EARLY_STOPPING_ROUNDS", DEFAULT_EARLY_STOPPING_ROUNDS))


TASK_REGRESSION = "regression"
TASK_CLASSIFIER = "classifier"
TASK_RANKER = "ranker"


def _task_config(task: str) -> tuple[type, dict[str, Any], str]:
    """Return ``(estimator_cls, default_params, cv_scoring)`` for ``task``.

    D3: a single ``tune_xgb`` entrypoint serves both the regression heads
    and the binary DNF classifier; the only differences are the XGBoost
    estimator class, the objective baked into the search's base
    estimator, and the CV scoring metric. Keeping the configuration in
    one helper avoids three near-identical tuning functions drifting out
    of sync (e.g. someone bumping ``n_estimators`` only on the
    regression path).

    D2: the pairwise ranker uses ``XGBRanker`` with ``rank:pairwise``;
    the CV scorer falls back to ``None`` because scikit-learn's standard
    scorers don't accept the ``qid`` group argument the ranker needs.
    The search relies on the estimator's default ``ndcg`` score path,
    which is what we want for a ranker anyway.
    """
    from xgboost import XGBClassifier, XGBRanker, XGBRegressor

    if task == TASK_REGRESSION:
        return XGBRegressor, DEFAULT_PARAMS, "neg_mean_absolute_error"
    if task == TASK_CLASSIFIER:
        return XGBClassifier, CLASSIFIER_DEFAULT_PARAMS, "neg_log_loss"
    if task == TASK_RANKER:
        return XGBRanker, RANKER_DEFAULT_PARAMS, ""
    raise ValueError(
        f"tune_xgb: unsupported task={task!r}; "
        f"expected one of {TASK_REGRESSION!r}, {TASK_CLASSIFIER!r}, {TASK_RANKER!r}"
    )


def tune_xgb(
    X: np.ndarray,
    y: np.ndarray,
    *,
    random_state: int = 42,
    n_iter: int | None = None,
    cv_splits: int | None = None,
    X_dev: np.ndarray | None = None,
    y_dev: np.ndarray | None = None,
    early_stopping_rounds: int | None = None,
    task: str = TASK_REGRESSION,
    group: np.ndarray | None = None,
    group_dev: np.ndarray | None = None,
) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    """
    Find best XGBoost hyperparams for ``X``/``y`` via ``RandomizedSearchCV``.

    Returns ``(best_estimator, best_params, tuning_metadata)``:

    - ``best_estimator`` is a fitted ``XGBRegressor`` (``task='regression'``)
      or ``XGBClassifier`` (``task='classifier'``). When ``X_dev``/``y_dev``
      are supplied the final fit uses ``eval_set=[(X_dev, y_dev)]`` plus
      ``early_stopping_rounds`` (A4), so ``best_iteration`` is recorded on
      both the estimator and ``tuning_metadata``. When no dev slice is
      provided (small-cohort fallback or external callers without a dev
      split) the search refits internally with ``refit=True``.
    - ``best_params`` is exactly what the search selected, with
      ``random_state`` / ``tree_method`` re-asserted for audit logging.
      A3 dropped the manual ``DEFAULT_PARAMS`` merge — ``best_estimator`` is
      the source of truth.
    - ``tuning_metadata`` is persisted on ``ml_models.tuning_metadata_json``.

    ``task='classifier'`` (D3) routes the search through
    ``XGBClassifier(objective='binary:logistic')`` and switches the CV
    scorer to ``neg_log_loss``. All other code paths (early stopping,
    tune-disable, dev-fallback) are shared with the regression task.
    """
    estimator_cls, defaults, cv_scoring = _task_config(task)

    X_filled = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    has_dev = X_dev is not None and y_dev is not None and len(X_dev) > 0
    if has_dev:
        X_dev_filled = np.nan_to_num(X_dev, nan=0.0, posinf=0.0, neginf=0.0)
        if early_stopping_rounds is None:
            early_stopping_rounds = _env_early_stopping_rounds()
        if early_stopping_rounds <= 0:
            has_dev = False  # explicit disable wins over a non-empty dev slice

    # D2: the pairwise ranker bypasses ``RandomizedSearchCV`` because
    # scikit-learn doesn't know how to thread the ``group`` array through
    # CV folds. We fit a single ``XGBRanker`` with default params + qid;
    # the env var is OFF by default so this code path runs only when an
    # operator explicitly opts in for a champion/challenger evaluation
    # cycle. Tuning continuity for the ranker is future work.
    if task == TASK_RANKER:
        if group is None:
            raise ValueError(
                "tune_xgb: task='ranker' requires a non-None ``group`` (qid sizes)"
            )
        params = dict(defaults)
        params["random_state"] = random_state
        kwargs = dict(params, verbosity=0, n_jobs=xgb_n_jobs())
        if has_dev and early_stopping_rounds and early_stopping_rounds > 0:
            kwargs["early_stopping_rounds"] = int(early_stopping_rounds)
        estimator = estimator_cls(**kwargs)
        fit_kwargs: dict[str, Any] = {"group": group}
        if has_dev and group_dev is not None:
            fit_kwargs["eval_set"] = [(X_dev_filled, y_dev)]
            fit_kwargs["eval_group"] = [group_dev]
            fit_kwargs["verbose"] = False
        estimator.fit(X_filled, y, **fit_kwargs)
        meta: dict[str, Any] = {
            "tuning_skipped": True,
            "reason": "ranker_uses_default_params",
            "task": task,
            "n_groups": int(len(group)),
        }
        if has_dev and group_dev is not None:
            best_iter = getattr(estimator, "best_iteration", None)
            meta["dev_n_rows"] = int(len(X_dev_filled))
            if best_iter is not None:
                meta["best_iteration"] = int(best_iter)
        return estimator, params, meta

    if _tuning_disabled():
        _LOG.info(
            "tune_xgb: tuning disabled via FS_ML_TUNE_DISABLE; using %s defaults",
            task,
        )
        params = dict(defaults)
        params["random_state"] = random_state
        kwargs: dict[str, Any] = dict(params, verbosity=0, n_jobs=xgb_n_jobs())
        if has_dev:
            kwargs["early_stopping_rounds"] = int(early_stopping_rounds)
        estimator = estimator_cls(**kwargs)
        if has_dev:
            estimator.fit(X_filled, y, eval_set=[(X_dev_filled, y_dev)], verbose=False)
        else:
            estimator.fit(X_filled, y)
        meta = {
            "tuning_skipped": True,
            "reason": "FS_ML_TUNE_DISABLE",
            "task": task,
        }
        if has_dev:
            best_iter = getattr(estimator, "best_iteration", None)
            meta["early_stopping_rounds"] = int(early_stopping_rounds)
            meta["dev_n_rows"] = int(len(X_dev_filled))
            if best_iter is not None:
                meta["best_iteration"] = int(best_iter)
        return estimator, params, meta

    from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit

    iters = n_iter if n_iter is not None else _env_int("FS_ML_TUNE_N_ITER", 25)
    splits = cv_splits if cv_splits is not None else _env_int("FS_ML_TUNE_CV_SPLITS", 4)

    n_rows = X.shape[0]
    # TimeSeriesSplit requires at least ``n_splits + 1`` samples; clamp gracefully.
    max_splits_for_rows = max(2, min(splits, max(2, n_rows - 1)))
    splits = max_splits_for_rows

    base_kwargs: dict[str, Any] = dict(
        objective=defaults["objective"],
        tree_method="hist",
        n_jobs=xgb_n_jobs(),
        random_state=random_state,
        verbosity=0,
    )
    # D3: XGBClassifier needs the eval_metric pinned upfront so the search's
    # base estimator agrees with the holdout-time AUC / log_loss audit.
    if "eval_metric" in defaults:
        base_kwargs["eval_metric"] = defaults["eval_metric"]
    base_estimator = estimator_cls(**base_kwargs)

    # ``refit`` is False when we plan to do the final early-stopping fit
    # ourselves against the dev slice; otherwise we let scikit-learn refit
    # on the union (A3 behavior).
    search = RandomizedSearchCV(
        estimator=base_estimator,
        param_distributions=PARAM_DISTRIBUTIONS,
        n_iter=iters,
        scoring=cv_scoring,
        cv=TimeSeriesSplit(n_splits=splits),
        refit=not has_dev,
        random_state=random_state,
        n_jobs=1,
        error_score="raise",
    )

    search.fit(X_filled, y)

    best_params: dict[str, Any] = dict(search.best_params_)
    best_params["random_state"] = random_state
    best_params["tree_method"] = "hist"
    # B1: surface the chosen objective on ``best_params`` so the early-stopping
    # final fit + ``ml_models.hyperparams_json`` audits both reflect it.
    best_params["objective"] = defaults["objective"]
    if "eval_metric" in defaults:
        best_params["eval_metric"] = defaults["eval_metric"]

    metadata: dict[str, Any] = {
        "tuning_skipped": False,
        "n_iter": int(iters),
        "cv_splits": int(splits),
        "best_score": float(search.best_score_),
        "scoring": cv_scoring,
        "param_distributions": PARAM_DISTRIBUTIONS,
        "best_params": dict(search.best_params_),
        "task": task,
    }

    if has_dev:
        # A4: final fit with XGBoost early stopping watched on the dev slice.
        final_kwargs: dict[str, Any] = dict(
            best_params,
            verbosity=0,
            n_jobs=xgb_n_jobs(),
            early_stopping_rounds=int(early_stopping_rounds),
        )
        final = estimator_cls(**final_kwargs)
        final.fit(X_filled, y, eval_set=[(X_dev_filled, y_dev)], verbose=False)
        best_iter = getattr(final, "best_iteration", None)
        metadata["early_stopping_rounds"] = int(early_stopping_rounds)
        metadata["dev_n_rows"] = int(len(X_dev_filled))
        if best_iter is not None:
            metadata["best_iteration"] = int(best_iter)
        return final, best_params, metadata

    return search.best_estimator_, best_params, metadata
