"""Unit tests for walk-forward backtest helpers."""

from __future__ import annotations

from unittest.mock import patch

from include.nascar.ml.backtest_task import run_ml_backtest_task


def test_backtest_task_calls_walk_forward() -> None:
    with patch(
        "include.nascar.ml.backtest_task.run_walk_forward_backtest",
        return_value={"n_weeks": 0},
    ) as mock_run:
        result = run_ml_backtest_task()

    mock_run.assert_called_once()
    assert result == {"n_weeks": 0}
