"""Unit tests for ``include.nascar.ml.xgb_tuning``."""

from __future__ import annotations

import numpy as np
import pytest

from include.nascar.ml.xgb_tuning import (
    DEFAULT_EARLY_STOPPING_ROUNDS,
    DEFAULT_PARAMS,
    PARAM_DISTRIBUTIONS,
    tune_xgb,
)


@pytest.fixture
def tiny_dataset() -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(0)
    X = rng.normal(size=(40, 5))
    y = X[:, 0] * 1.5 - X[:, 1] * 0.5 + rng.normal(scale=0.1, size=40)
    return X, y


@pytest.fixture
def tiny_dataset_with_dev() -> (
    tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]
):
    rng = np.random.default_rng(0)
    X = rng.normal(size=(80, 5))
    y = X[:, 0] * 1.5 - X[:, 1] * 0.5 + rng.normal(scale=0.1, size=80)
    return X[:60], y[:60], X[60:], y[60:]


def test_tune_xgb_returns_refit_estimator(tiny_dataset: tuple[np.ndarray, np.ndarray]) -> None:
    """A3: ``tune_xgb`` returns a fitted estimator via ``refit=True``."""
    X, y = tiny_dataset
    estimator, best_params, metadata = tune_xgb(X, y, n_iter=2, cv_splits=2)

    # Estimator is fitted and usable directly (no double-fit needed).
    preds = estimator.predict(X)
    assert preds.shape == (X.shape[0],)
    # A3: best_params keeps only tuned keys + audit knobs, no DEFAULT_PARAMS merge.
    for key in PARAM_DISTRIBUTIONS:
        assert key in best_params, f"missing tuned key {key} in best_params"
    assert best_params["tree_method"] == "hist"
    assert best_params["random_state"] == 42
    assert metadata["tuning_skipped"] is False
    assert metadata["n_iter"] == 2
    assert metadata["cv_splits"] == 2
    assert metadata["scoring"] == "neg_mean_absolute_error"
    assert set(metadata["best_params"]).issubset(set(PARAM_DISTRIBUTIONS))


def test_tune_xgb_best_params_does_not_merge_defaults(
    tiny_dataset: tuple[np.ndarray, np.ndarray],
) -> None:
    """A3 + B1: ``best_params`` carries only tuned keys plus audit-stable knobs.

    The audit-stable knobs are ``random_state`` (reproducibility),
    ``tree_method`` (training hardware contract) and ``objective`` (loss
    function — pinned by B1 to ``reg:absoluteerror`` so it must always be
    visible). No other ``DEFAULT_PARAMS`` value should leak in.
    """
    X, y = tiny_dataset
    _, best_params, _ = tune_xgb(X, y, n_iter=2, cv_splits=2)

    allowed_keys = set(PARAM_DISTRIBUTIONS) | {
        "random_state",
        "tree_method",
        "objective",
    }
    extra_keys = set(best_params) - allowed_keys
    assert not extra_keys, (
        f"best_params leaked non-tuned DEFAULT_PARAMS entries: {extra_keys}"
    )
    assert best_params["objective"] == "reg:absoluteerror"


def test_tune_xgb_disable_env_short_circuits(
    tiny_dataset: tuple[np.ndarray, np.ndarray],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    X, y = tiny_dataset
    estimator, best_params, metadata = tune_xgb(X, y)

    # D3: tune_xgb now records ``task`` on the disabled-path metadata so the
    # registry can audit regression vs classifier fits even when the search
    # is short-circuited.
    assert metadata == {
        "tuning_skipped": True,
        "reason": "FS_ML_TUNE_DISABLE",
        "task": "regression",
    }
    expected_params = dict(DEFAULT_PARAMS)
    expected_params["random_state"] = 42
    assert best_params == expected_params
    # B1: the disabled fallback path still uses the MAE objective.
    assert best_params["objective"] == "reg:absoluteerror"
    # A3: the disabled path still returns a fitted estimator.
    preds = estimator.predict(X)
    assert preds.shape == (X.shape[0],)


def test_default_params_objective_is_absoluteerror() -> None:
    """B1: the default training loss must match the published MAE metric."""
    assert DEFAULT_PARAMS["objective"] == "reg:absoluteerror"


def test_tune_xgb_dev_set_triggers_early_stopping(
    tiny_dataset_with_dev: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
) -> None:
    """A4: passing ``X_dev``/``y_dev`` records ``best_iteration`` + rounds."""
    X, y, X_dev, y_dev = tiny_dataset_with_dev
    estimator, best_params, metadata = tune_xgb(
        X,
        y,
        n_iter=2,
        cv_splits=2,
        X_dev=X_dev,
        y_dev=y_dev,
        early_stopping_rounds=5,
    )

    assert metadata["early_stopping_rounds"] == 5
    assert metadata["dev_n_rows"] == len(X_dev)
    # The fitted booster should expose a non-negative best_iteration.
    assert "best_iteration" in metadata
    assert metadata["best_iteration"] >= 0
    # Estimator must be the early-stopped final fit, not the search refit.
    assert getattr(estimator, "best_iteration", None) == metadata["best_iteration"]


def test_tune_xgb_disabled_with_dev_set_still_early_stops(
    tiny_dataset_with_dev: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A4: tuning-disabled path still honors early stopping when a dev slice exists."""
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    X, y, X_dev, y_dev = tiny_dataset_with_dev
    estimator, _, metadata = tune_xgb(
        X,
        y,
        X_dev=X_dev,
        y_dev=y_dev,
        early_stopping_rounds=3,
    )

    assert metadata["tuning_skipped"] is True
    assert metadata["early_stopping_rounds"] == 3
    assert "best_iteration" in metadata
    assert metadata["best_iteration"] >= 0
    assert getattr(estimator, "best_iteration", None) is not None


def test_tune_xgb_env_default_early_stopping_rounds(
    tiny_dataset_with_dev: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
) -> None:
    """A4: ``early_stopping_rounds=None`` falls back to ``FS_ML_EARLY_STOPPING_ROUNDS`` (default 20)."""
    X, y, X_dev, y_dev = tiny_dataset_with_dev
    _, _, metadata = tune_xgb(
        X,
        y,
        n_iter=2,
        cv_splits=2,
        X_dev=X_dev,
        y_dev=y_dev,
    )

    assert metadata["early_stopping_rounds"] == DEFAULT_EARLY_STOPPING_ROUNDS


def test_tune_xgb_env_disables_early_stopping(
    tiny_dataset_with_dev: tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A4: ``FS_ML_EARLY_STOPPING_ROUNDS=0`` disables early stopping even with a dev slice."""
    monkeypatch.setenv("FS_ML_EARLY_STOPPING_ROUNDS", "0")
    X, y, X_dev, y_dev = tiny_dataset_with_dev
    _, _, metadata = tune_xgb(
        X,
        y,
        n_iter=2,
        cv_splits=2,
        X_dev=X_dev,
        y_dev=y_dev,
    )

    assert "early_stopping_rounds" not in metadata
    assert "best_iteration" not in metadata
