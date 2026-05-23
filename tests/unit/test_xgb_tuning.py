"""Unit tests for ``include.nascar.ml.xgb_tuning``."""

from __future__ import annotations

import numpy as np
import pytest

from include.nascar.ml.xgb_tuning import (
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


def test_tune_xgb_returns_complete_param_dict(tiny_dataset: tuple[np.ndarray, np.ndarray]) -> None:
    X, y = tiny_dataset
    best_params, metadata = tune_xgb(X, y, n_iter=2, cv_splits=2)

    for key in PARAM_DISTRIBUTIONS:
        assert key in best_params, f"missing tuned key {key} in best_params"

    assert best_params["tree_method"] == "hist"
    assert best_params["random_state"] == 42
    assert metadata["tuning_skipped"] is False
    assert metadata["n_iter"] == 2
    assert metadata["cv_splits"] == 2
    assert metadata["scoring"] == "neg_mean_absolute_error"
    assert set(metadata["best_params"]).issubset(set(PARAM_DISTRIBUTIONS))


def test_tune_xgb_disable_env_short_circuits(
    tiny_dataset: tuple[np.ndarray, np.ndarray],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    X, y = tiny_dataset
    best_params, metadata = tune_xgb(X, y)

    assert metadata == {"tuning_skipped": True, "reason": "FS_ML_TUNE_DISABLE"}
    assert best_params == DEFAULT_PARAMS
