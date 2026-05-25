"""Unit tests for walk-forward backtest helpers."""

from __future__ import annotations

from include.nascar.ml.backtest_task import run_ml_backtest_task


def test_backtest_task_skips_without_env() -> None:
    result = run_ml_backtest_task()
    assert result["skipped"] is True
