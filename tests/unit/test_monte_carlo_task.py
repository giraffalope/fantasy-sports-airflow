"""Unit tests for the Monte Carlo Airflow task wrapper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from include.nascar.ml import monte_carlo_task as mod


def test_skips_without_active_rules() -> None:
    with (
        patch(
            "include.nascar.ml.monte_carlo_task.NascarMySqlHook",
            return_value=MagicMock(),
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._active_rules",
            return_value=None,
        ),
    ):
        result = mod.run_monte_carlo_simulation_task()
    assert result == {"skipped": True, "reason": "no_active_rules"}


def test_skips_without_predict_snapshot() -> None:
    with (
        patch(
            "include.nascar.ml.monte_carlo_task.NascarMySqlHook",
            return_value=MagicMock(),
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._latest_predict_snapshot",
            return_value=None,
        ),
    ):
        result = mod.run_monte_carlo_simulation_task()
    assert result == {"skipped": True, "reason": "no_predict_snapshot"}


def test_skips_without_ml_predictions() -> None:
    hook = MagicMock()
    with (
        patch(
            "include.nascar.ml.monte_carlo_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5, "series_id": 1},
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._load_predictions",
            return_value=[],
        ),
    ):
        result = mod.run_monte_carlo_simulation_task()
    assert result["skipped"] is True
    assert result["reason"] == "no_ml_predictions"


def test_skips_when_primary_target_sigma_missing() -> None:
    hook = MagicMock()
    with (
        patch(
            "include.nascar.ml.monte_carlo_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5, "series_id": 1},
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._load_predictions",
            return_value=[
                {
                    "car_driver_id": 1,
                    "r_start_position": 5.0,
                    "predicted_targets": {"r_finish_position": 5.0},
                }
            ],
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._load_target_sigma",
            return_value=None,
        ),
    ):
        result = mod.run_monte_carlo_simulation_task()
    assert result["skipped"] is True
    assert result["reason"] == "no_primary_target_sigma"


def test_n_simulations_env_override() -> None:
    with patch.dict("os.environ", {"FS_MC_N_SIMULATIONS": "5000"}, clear=False):
        assert mod.n_simulations() == 5000
    with patch.dict("os.environ", {"FS_MC_N_SIMULATIONS": ""}, clear=False):
        assert mod.n_simulations() == mod.MIN_SIMULATIONS
    with patch.dict("os.environ", {"FS_MC_N_SIMULATIONS": "not-an-int"}, clear=False):
        assert mod.n_simulations() == mod.MIN_SIMULATIONS


def test_random_seed_uses_snapshot_when_env_missing() -> None:
    with patch.dict("os.environ", {}, clear=False):
        # In case the env var is set in the dev shell, force-clear it for this test.
        with patch.dict("os.environ", {"FS_MC_RANDOM_SEED": ""}, clear=False):
            seed_a = mod.random_seed(123)
            seed_b = mod.random_seed(123)
            seed_c = mod.random_seed(124)
    assert seed_a == seed_b
    assert seed_a != seed_c


def test_random_seed_env_override() -> None:
    with patch.dict("os.environ", {"FS_MC_RANDOM_SEED": "42"}, clear=False):
        assert mod.random_seed(123) == 42


def test_validate_raises_when_n_simulations_below_minimum() -> None:
    hook = MagicMock()
    hook.get_records.side_effect = [
        [(7, 500)],  # batch row: n_simulations = 500
    ]
    with (
        patch(
            "include.nascar.ml.monte_carlo_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5, "series_id": 1},
        ),
    ):
        with pytest.raises(RuntimeError, match="n_simulations=500"):
            mod.validate_simulation_batch_task()


def test_validate_raises_when_summary_count_mismatches_predictions() -> None:
    hook = MagicMock()
    hook.get_records.side_effect = [
        [(7, mod.MIN_SIMULATIONS)],  # batch row
        [(9,)],  # summary count
        [(10,)],  # prediction count
    ]
    with (
        patch(
            "include.nascar.ml.monte_carlo_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5, "series_id": 1},
        ),
    ):
        with pytest.raises(RuntimeError, match="summaries=9"):
            mod.validate_simulation_batch_task()


def test_validate_returns_metrics_on_success() -> None:
    hook = MagicMock()
    hook.get_records.side_effect = [
        [(7, mod.MIN_SIMULATIONS)],  # batch
        [(40,)],  # summaries
        [(40,)],  # predictions
    ]
    with (
        patch(
            "include.nascar.ml.monte_carlo_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5, "series_id": 1},
        ),
    ):
        result = mod.validate_simulation_batch_task()
    assert result == {
        "skipped": False,
        "snapshot_id": 99,
        "batch_id": 7,
        "n_simulations": mod.MIN_SIMULATIONS,
        "drivers": 40,
    }
