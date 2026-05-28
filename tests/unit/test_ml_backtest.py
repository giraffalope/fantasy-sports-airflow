"""Unit tests for walk-forward backtest helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from include.nascar.ml import backtest as backtest_module
from include.nascar.ml.backtest import (
    _BACKTEST_LOAD_WEEKS_DEFAULT,
    _backtest_load_weeks_env,
    run_walk_forward_backtest,
)
from include.nascar.ml.backtest_task import run_ml_backtest_task


def test_backtest_task_calls_walk_forward() -> None:
    with patch(
        "include.nascar.ml.backtest_task.run_walk_forward_backtest",
        return_value={"n_weeks": 0},
    ) as mock_run:
        result = run_ml_backtest_task()

    mock_run.assert_called_once()
    assert result == {"n_weeks": 0}


def test_backtest_load_weeks_env_default(monkeypatch) -> None:
    monkeypatch.delenv("FS_ML_BACKTEST_LOAD_WEEKS", raising=False)
    assert _backtest_load_weeks_env() == _BACKTEST_LOAD_WEEKS_DEFAULT
    assert _BACKTEST_LOAD_WEEKS_DEFAULT == 64


def test_backtest_load_weeks_env_respects_override(monkeypatch) -> None:
    monkeypatch.setenv("FS_ML_BACKTEST_LOAD_WEEKS", "24")
    assert _backtest_load_weeks_env() == 24


def test_backtest_load_weeks_env_blank_is_default(monkeypatch) -> None:
    monkeypatch.setenv("FS_ML_BACKTEST_LOAD_WEEKS", "   ")
    assert _backtest_load_weeks_env() == _BACKTEST_LOAD_WEEKS_DEFAULT


def test_backtest_load_weeks_env_rejects_garbage(monkeypatch, caplog) -> None:
    monkeypatch.setenv("FS_ML_BACKTEST_LOAD_WEEKS", "abc")
    with caplog.at_level("WARNING", logger=backtest_module.__name__):
        assert _backtest_load_weeks_env() == _BACKTEST_LOAD_WEEKS_DEFAULT
    assert any(
        "FS_ML_BACKTEST_LOAD_WEEKS" in record.getMessage()
        for record in caplog.records
    )


def test_backtest_load_weeks_env_rejects_zero(monkeypatch) -> None:
    monkeypatch.setenv("FS_ML_BACKTEST_LOAD_WEEKS", "0")
    assert _backtest_load_weeks_env() == _BACKTEST_LOAD_WEEKS_DEFAULT


def test_backtest_load_weeks_env_rejects_negative(monkeypatch) -> None:
    monkeypatch.setenv("FS_ML_BACKTEST_LOAD_WEEKS", "-5")
    assert _backtest_load_weeks_env() == _BACKTEST_LOAD_WEEKS_DEFAULT


def test_backtest_passes_max_train_weeks_to_loader(monkeypatch) -> None:
    """Regression guard for the 27 May host OOM.

    The unbounded ``load_training_frame`` call in ``run_walk_forward_backtest``
    materialized the full Cup history (~24k rows of ~238 KB features_json
    each) and SIGKILL'd the worker at 11.36 GB RSS. This test pins the
    contract that the loader is always called with ``max_train_weeks`` set
    to a positive int (sourced from ``FS_ML_BACKTEST_LOAD_WEEKS``), so any
    future refactor that drops the argument fails CI rather than crashing
    the host.
    """
    monkeypatch.setenv("FS_ML_BACKTEST_LOAD_WEEKS", "32")

    hook = MagicMock()
    with patch(
        "include.nascar.ml.backtest._active_rules",
        return_value=(1, {"some": "rules"}),
    ), patch(
        "include.nascar.ml.backtest.load_training_frame",
        return_value=[],
    ) as mock_loader:
        result = run_walk_forward_backtest(hook=hook, series_id=1)

    assert result == {"skipped": "no_training_frame"}
    mock_loader.assert_called_once()
    kwargs = mock_loader.call_args.kwargs
    assert kwargs.get("max_train_weeks") == 32
    assert kwargs.get("dfs_scoring_rules_id") == 1
    assert kwargs.get("series_id") == 1


def test_backtest_loader_uses_default_when_env_unset(monkeypatch) -> None:
    monkeypatch.delenv("FS_ML_BACKTEST_LOAD_WEEKS", raising=False)

    hook = MagicMock()
    with patch(
        "include.nascar.ml.backtest._active_rules",
        return_value=(1, {"some": "rules"}),
    ), patch(
        "include.nascar.ml.backtest.load_training_frame",
        return_value=[],
    ) as mock_loader:
        run_walk_forward_backtest(hook=hook, series_id=1)

    assert (
        mock_loader.call_args.kwargs.get("max_train_weeks")
        == _BACKTEST_LOAD_WEEKS_DEFAULT
    )
