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
            "include.nascar.ml.monte_carlo_task._post_race_mc_already_present",
            return_value=False,
        ),
        patch(
            "include.nascar.ml.monte_carlo_task._load_predictions",
            return_value=[],
        ),
    ):
        result = mod.run_monte_carlo_simulation_task()
    assert result["skipped"] is True
    assert result["reason"] == "no_ml_predictions"


def test_post_race_mc_already_present_helper() -> None:
    hook = MagicMock()

    hook.get_records.return_value = [(1, 1)]
    assert mod._post_race_mc_already_present(
        hook, week_id=42, snapshot_id=99
    ) is True

    hook.get_records.return_value = [(0, 1)]
    assert mod._post_race_mc_already_present(
        hook, week_id=42, snapshot_id=99
    ) is False

    hook.get_records.return_value = [(1, 0)]
    assert mod._post_race_mc_already_present(
        hook, week_id=42, snapshot_id=99
    ) is False

    hook.get_records.return_value = []
    assert mod._post_race_mc_already_present(
        hook, week_id=42, snapshot_id=99
    ) is False


def test_skips_post_race_when_mc_batch_already_present() -> None:
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
            "include.nascar.ml.monte_carlo_task._post_race_mc_already_present",
            return_value=True,
        ) as mock_post_race,
        patch(
            "include.nascar.ml.monte_carlo_task._load_predictions",
        ) as mock_load_predictions,
    ):
        result = mod.run_monte_carlo_simulation_task()
    assert result["skipped"] is True
    assert result["reason"] == "mc_already_present_post_race"
    assert result["snapshot_id"] == 99
    assert result["week_id"] == 5
    mock_post_race.assert_called_once()
    mock_load_predictions.assert_not_called()


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
            "include.nascar.ml.monte_carlo_task._post_race_mc_already_present",
            return_value=False,
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
        assert mod.n_simulations() == mod.DEFAULT_SIMULATIONS
    with patch.dict("os.environ", {"FS_MC_N_SIMULATIONS": "not-an-int"}, clear=False):
        assert mod.n_simulations() == mod.DEFAULT_SIMULATIONS


def test_n_simulations_default_is_ten_thousand() -> None:
    """Pin the production default at 10000.

    Tests intentionally keep ``MIN_SIMULATIONS=1000`` as the fast
    scaffold; this assertion guards the *runtime* default visible to
    operators (no env override) so a regression below 10000 surfaces
    here rather than as a quiet portfolio-fidelity regression.
    """
    assert mod.DEFAULT_SIMULATIONS == 10000


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
            mean_r_finish_position=12.0,
            std_r_finish_position=3.0,
            min_r_finish_position=1.0,
            p05_r_finish_position=4.0,
            p50_r_finish_position=12.0,
            p95_r_finish_position=20.0,
            max_r_finish_position=25.0,
            mean_r_total_fastest_laps=4.0,
            std_r_total_fastest_laps=1.0,
            min_r_total_fastest_laps=0.0,
            p05_r_total_fastest_laps=1.0,
            p50_r_total_fastest_laps=4.0,
            p95_r_total_fastest_laps=7.0,
            max_r_total_fastest_laps=10.0,
            mean_r_total_laps_led=30.0,
            std_r_total_laps_led=6.0,
            min_r_total_laps_led=15.0,
            p05_r_total_laps_led=18.0,
            p50_r_total_laps_led=30.0,
            p95_r_total_laps_led=42.0,
            max_r_total_laps_led=45.0,
            dnf_pct=0.05 + 0.01 * (cid - 101),
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
    finish_matrix = np.tile(np.arange(1, 11, dtype=float), (3, 1))
    fastest_laps_matrix = np.zeros_like(matrix)
    laps_led_matrix = np.full_like(matrix, 25.0)
    dnf_matrix = np.zeros_like(matrix, dtype=np.int8)
    dnf_matrix[0, 0] = 1  # first driver DNFed in iteration 0 to exercise the int8 path
    return MonteCarloBatchResult(
        summaries=summaries,
        joint_dfs_points=matrix,
        driver_index=np.array([101, 102, 103], dtype=np.int64),
        joint_finish_position=finish_matrix,
        joint_total_fastest_laps=fastest_laps_matrix,
        joint_total_laps_led=laps_led_matrix,
        joint_is_dnf=dnf_matrix,
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
        np.testing.assert_array_equal(
            data["joint_finish_position"], batch_result.joint_finish_position
        )
        np.testing.assert_array_equal(
            data["joint_total_fastest_laps"], batch_result.joint_total_fastest_laps
        )
        np.testing.assert_array_equal(
            data["joint_total_laps_led"], batch_result.joint_total_laps_led
        )
        np.testing.assert_array_equal(
            data["joint_is_dnf"], batch_result.joint_is_dnf
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


# ---------------------------------------------------------------------------
# DNF / per-target column wiring
# ---------------------------------------------------------------------------


def test_build_driver_inputs_strips_metadata_keys_and_threads_p_dnf() -> None:
    """``r_is_dnf`` must land on :attr:`MonteCarloDriverInput.p_dnf` and be
    stripped from the simulator-facing ``predicted_targets`` dict, alongside
    the existing ``r_finish_position_raw`` D1 audit float."""
    predictions = [
        {
            "car_driver_id": 1001,
            "r_start_position": 7.0,
            "predicted_targets": {
                "r_finish_position": 8.0,
                "r_finish_position_raw": 8.7,
                "r_total_laps_led": 30.0,
                "r_total_fastest_laps": 4.0,
                "r_is_dnf": 0.22,
            },
            "blend_metadata": {},
        },
        {
            "car_driver_id": 1002,
            "r_start_position": 18.0,
            "predicted_targets": {
                "r_finish_position": 19.0,
                "r_finish_position_raw": 19.4,
                "r_total_laps_led": 1.0,
                "r_total_fastest_laps": 0.0,
                # No ``r_is_dnf`` for this driver -> p_dnf must be None.
            },
            "blend_metadata": {},
        },
    ]
    target_sigmas = {
        "r_finish_position": 4.0,
        "r_total_laps_led": 10.0,
        "r_total_fastest_laps": 2.0,
    }
    drivers = mod._build_driver_inputs(
        predictions, target_sigmas, method="global_mae"
    )
    assert len(drivers) == 2
    assert drivers[0].p_dnf == pytest.approx(0.22)
    assert "r_is_dnf" not in drivers[0].predicted_targets
    assert "r_finish_position_raw" not in drivers[0].predicted_targets
    assert drivers[1].p_dnf is None
    assert "r_is_dnf" not in drivers[1].predicted_targets


def test_persist_batch_insert_uses_extended_column_list() -> None:
    """The driver-summaries INSERT must list all 22 new columns and pass an
    equally-sized parameter tuple per row. We exercise ``_persist_batch``
    against a ``MagicMock`` cursor so a future schema drift (e.g. someone
    adds a column to the table but forgets to widen the INSERT) trips the
    assertions below.
    """
    summaries = _stub_batch_result().summaries
    captured: dict[str, object] = {}

    class _FakeCursor:
        lastrowid = 555

        def execute(self, _sql: str, _params: tuple) -> None:
            return None

        def executemany(self, sql: str, rows: list) -> None:
            captured["sql"] = sql
            captured["rows"] = rows

    fake_cursor = _FakeCursor()
    hook = MagicMock()

    def _fake_run_in_transaction(callable_):
        callable_(object(), fake_cursor)

    hook.run_in_transaction.side_effect = _fake_run_in_transaction

    inserted_batch_id = mod._persist_batch(
        hook,
        snapshot_id=42,
        n_sims=mod.MIN_SIMULATIONS,
        seed=7,
        pipeline_ver="test",
        summaries=summaries,
    )
    assert inserted_batch_id == 555
    sql = captured["sql"]  # type: ignore[assignment]
    rows = captured["rows"]  # type: ignore[assignment]
    assert "ml_race_simulation_driver_summaries" in sql
    # Sanity-check every new column appears in the INSERT column list.
    for expected_col in (
        "mean_r_finish_position",
        "p95_r_finish_position",
        "max_r_finish_position",
        "mean_r_total_fastest_laps",
        "p50_r_total_fastest_laps",
        "max_r_total_fastest_laps",
        "mean_r_total_laps_led",
        "p05_r_total_laps_led",
        "max_r_total_laps_led",
        "dnf_pct",
    ):
        assert expected_col in sql
    # Parameter tuples must have exactly 32 values: 3 ids/n_sims + 7 DFS stats
    # + 7 finish stats + 7 fastest_laps stats + 7 laps_led stats + dnf_pct.
    assert len(rows) == len(summaries)
    for row in rows:
        assert len(row) == 32
    # Spot-check the dnf_pct value lands at the end of each tuple and is
    # rounded to 4 decimals.
    for row, summary in zip(rows, summaries, strict=True):
        assert row[-1] == round(summary.dnf_pct, 4)
