"""Unit tests for the Monte Carlo Airflow task wrapper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from include.nascar.ml import monte_carlo_task as mod
from include.nascar.ml.monte_carlo import (
    MonteCarloBatchResult,
    MonteCarloDriverSummary,
)


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


def _stub_batch_result() -> MonteCarloBatchResult:
    summaries = [
        MonteCarloDriverSummary(
            car_driver_id=cid,
            n_simulations=10,
            mean_dfs_points=30.0,
            std_dfs_points=5.0,
            min_dfs_points=10.0,
            p05_dfs_points=20.0,
            p50_dfs_points=30.0,
            p95_dfs_points=40.0,
            max_dfs_points=50.0,
        )
        for cid in (101, 102, 103)
    ]
    matrix = np.array(
        [
            [25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 28.0, 32.0, 31.0, 29.0],
            [22.0, 27.0, 32.0, 37.0, 42.0, 47.0, 26.0, 30.0, 29.0, 28.0],
            [19.0, 24.0, 29.0, 34.0, 39.0, 44.0, 23.0, 27.0, 26.0, 25.0],
        ],
        dtype=float,
    )
    return MonteCarloBatchResult(
        summaries=summaries,
        joint_dfs_points=matrix,
        driver_index=np.array([101, 102, 103], dtype=np.int64),
    )


def test_write_joint_samples_artifact_round_trips(tmp_path: Path) -> None:
    """The artifact must be loadable back into the same shape and ids.

    This is the canary that protects the lineup-task ``_load_joint_samples_from_artifact``
    code path: as long as the writer here and the loader there agree on
    array names + dtypes, the joint-samples pipeline stays end-to-end.
    """
    batch_result = _stub_batch_result()
    with patch.dict(
        "os.environ", {"FS_MC_ARTIFACT_ROOT": str(tmp_path)}, clear=False
    ):
        uri = mod._write_joint_samples_artifact(
            batch_result,
            batch_id=42,
            n_simulations_value=10,
            random_seed_value=99,
            snapshot_id=7,
        )
    assert uri is not None
    path = Path(uri)
    assert path.exists()
    with np.load(path, allow_pickle=False) as data:
        np.testing.assert_array_equal(
            data["joint_dfs_points"], batch_result.joint_dfs_points
        )
        np.testing.assert_array_equal(
            data["car_driver_ids"], batch_result.driver_index
        )
        meta = data["meta"].item()
        # meta is a void-record tuple ordered batch_id / snapshot_id / n_sims / seed
        assert meta[0] == 42
        assert meta[1] == 7
        assert meta[2] == 10
        assert meta[3] == 99


def test_write_joint_samples_artifact_returns_none_on_failure(
    tmp_path: Path,
) -> None:
    """Artifact write failures must be swallowed so the per-driver summary
    persistence remains the source of truth even on degraded paths."""
    batch_result = _stub_batch_result()
    with (
        patch.dict("os.environ", {"FS_MC_ARTIFACT_ROOT": str(tmp_path)}, clear=False),
        patch("numpy.savez_compressed", side_effect=OSError("disk full")),
    ):
        uri = mod._write_joint_samples_artifact(
            batch_result,
            batch_id=11,
            n_simulations_value=10,
            random_seed_value=1,
            snapshot_id=2,
        )
    assert uri is None


def test_mc_artifact_root_env_override(tmp_path: Path) -> None:
    with patch.dict(
        "os.environ", {"FS_MC_ARTIFACT_ROOT": str(tmp_path)}, clear=False
    ):
        assert mod.mc_artifact_root() == tmp_path


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
