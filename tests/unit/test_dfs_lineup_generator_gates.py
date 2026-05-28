"""Unit tests for lineup generator skip gates."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from include.nascar.dfs import lineup_generator_task as mod
from include.nascar.dfs.lineup_generator_task import (
    _PoolEntry,
    _insert_lineup_on_cursor,
    _load_joint_samples_from_artifact,
    _resolve_joint_samples,
    generate_draftkings_lineups_task,
)
from include.nascar.dfs.lineup_optimizer import LineupPlayer
from include.nascar.dfs.lineup_portfolio import CandidateScore


def test_skips_without_predict_snapshot() -> None:
    hook = MagicMock()
    with (
        patch(
            "include.nascar.dfs.lineup_generator_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_predict_snapshot",
            return_value=None,
        ),
    ):
        result = generate_draftkings_lineups_task()
    assert result == {"skipped": True, "reason": "no_predict_snapshot"}


def test_post_race_lineups_already_present_helper() -> None:
    hook = MagicMock()

    hook.get_records.return_value = [(1, 1)]
    assert mod._post_race_lineups_already_present(
        hook, week_id=42, dfs_id=1
    ) is True

    hook.get_records.return_value = [(0, 1)]
    assert mod._post_race_lineups_already_present(
        hook, week_id=42, dfs_id=1
    ) is False

    hook.get_records.return_value = [(1, 0)]
    assert mod._post_race_lineups_already_present(
        hook, week_id=42, dfs_id=1
    ) is False

    hook.get_records.return_value = []
    assert mod._post_race_lineups_already_present(
        hook, week_id=42, dfs_id=1
    ) is False


def test_skips_post_race_when_lineups_already_present() -> None:
    hook = MagicMock()
    with (
        patch(
            "include.nascar.dfs.lineup_generator_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5},
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._post_race_lineups_already_present",
            return_value=True,
        ) as mock_post_race,
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_mc_batch",
        ) as mock_latest_mc,
    ):
        result = generate_draftkings_lineups_task()
    assert result["skipped"] is True
    assert result["reason"] == "lineups_already_present_post_race"
    assert result["snapshot_id"] == 99
    assert result["week_id"] == 5
    mock_post_race.assert_called_once()
    mock_latest_mc.assert_not_called()


def test_skips_without_mc_summaries() -> None:
    hook = MagicMock()
    with (
        patch(
            "include.nascar.dfs.lineup_generator_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5},
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._post_race_lineups_already_present",
            return_value=False,
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_mc_batch",
            return_value=None,
        ),
    ):
        result = generate_draftkings_lineups_task()
    assert result["skipped"] is True
    assert result["reason"] == "no_mc_summaries"
    assert result["snapshot_id"] == 99


def test_skips_without_dk_salaries() -> None:
    hook = MagicMock()

    def get_records_side_effect(sql: str, parameters: tuple | None = None) -> list:
        if "SELECT r.id" in sql:
            return [(42,)]
        if "SELECT 1" in sql and "dfs_salaries" in sql:
            return []
        return []

    hook.get_records.side_effect = get_records_side_effect
    with (
        patch(
            "include.nascar.dfs.lineup_generator_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5},
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_mc_batch",
            return_value=(7, 1000, None),
        ),
    ):
        result = generate_draftkings_lineups_task()
    assert result["skipped"] is True
    assert result["reason"] == "no_dk_salaries"
    assert result["week_id"] == 5


def test_player_pool_sql_targets_mc_summaries_with_week_id() -> None:
    """Pool SQL must join salaries by week and read MC summaries by batch id."""
    sql = mod._PLAYER_POOL_SQL_TEMPLATE.format(
        objective_column=mod._MC_OBJECTIVE_COLUMNS[mod._DEFAULT_MC_OBJECTIVE]
    )
    assert "r.week_id = %s" in sql
    assert "ml_race_simulation_driver_summaries" in sql
    assert "ml_race_simulation_batch_id = %s" in sql
    assert "ml_predictions" not in sql
    # mean/std are pulled along with the objective column so the
    # summary-fallback path doesn't need a second round-trip.
    assert "mean_dfs_points" in sql
    assert "std_dfs_points" in sql
    assert "r.week_id = %s" in mod._SALARIES_EXIST_SQL


def test_mc_objective_defaults_to_p50_and_validates_input() -> None:
    with patch.dict("os.environ", {"FS_DK_MC_OBJECTIVE": ""}, clear=False):
        assert mod._mc_objective() == "p50"
    with patch.dict("os.environ", {"FS_DK_MC_OBJECTIVE": "mean"}, clear=False):
        assert mod._mc_objective() == "mean"
    with patch.dict("os.environ", {"FS_DK_MC_OBJECTIVE": "P95"}, clear=False):
        assert mod._mc_objective() == "p95"
    with patch.dict("os.environ", {"FS_DK_MC_OBJECTIVE": "bogus"}, clear=False):
        assert mod._mc_objective() == "p50"


def _six_pool() -> list[_PoolEntry]:
    return [
        _PoolEntry(
            player=LineupPlayer(
                car_driver_id=i,
                salary=5000 + i * 100,
                projected_points=40.0 - i,
                dfs_driver_id=100 + i,
            ),
            mean=40.0 - i,
            std=5.0,
        )
        for i in range(1, 7)
    ]


def _score_for(indices: tuple[int, ...]) -> CandidateScore:
    return CandidateScore(
        indices=indices,
        mean=180.0,
        std=12.0,
        p10=160.0,
        p50=180.0,
        p90=200.0,
        sharpe=15.0,
    )


def test_insert_lineup_on_cursor_persists_mc_fingerprint_columns() -> None:
    """The widened INSERT must carry all 8 MC fingerprint columns + the audit method."""
    cursor = MagicMock()
    cursor.lastrowid = 123
    pool = _six_pool()
    score = _score_for((0, 1, 2, 3, 4, 5))
    lineup_id = _insert_lineup_on_cursor(
        cursor,
        dfs_id=1,
        race_id=42,
        dfs_strategy_id=1,
        pool=pool,
        score=score,
        objective_metric="joint_p10",
        score_method="joint_p10",
    )
    assert lineup_id == 123
    assert cursor.execute.call_count == 2
    lineup_sql, lineup_params = cursor.execute.call_args_list[0][0]
    assert "dfs_race_lineups" in lineup_sql
    # 12 params: dfs_id, race_id, projected_total, salary_total, mean, std,
    # p10, p50, p90, sharpe, objective_value, mc_score_method.
    assert len(lineup_params) == 12
    assert "mc_mean_dfs_points" in lineup_sql
    assert "mc_std_dfs_points" in lineup_sql
    assert "mc_p10_dfs_points" in lineup_sql
    assert "mc_p50_dfs_points" in lineup_sql
    assert "mc_p90_dfs_points" in lineup_sql
    assert "mc_sharpe" in lineup_sql
    assert "objective_value" in lineup_sql
    assert "mc_score_method" in lineup_sql
    assert lineup_params[-1] == "joint_p10"
    assert lineup_params[-2] == 160.0  # objective_value for joint_p10 = p10 floor
    # Child rows still wired to the parent lineup_id.
    cursor.executemany.assert_called_once()
    child_rows = cursor.executemany.call_args[0][1]
    assert len(child_rows) == 6
    assert all(row[0] == 123 for row in child_rows)


def test_no_feasible_lineups_when_pool_yields_nothing() -> None:
    hook = MagicMock()

    def get_records_side_effect(sql: str, parameters: tuple | None = None) -> list:
        if "SELECT r.id" in sql:
            return [(42,)]
        if "SELECT 1" in sql and "dfs_salaries" in sql:
            return [(1,)]
        if "ml_race_simulation_driver_summaries" in sql:
            # 6 columns now: car_driver_id, salary, dfs_driver_id,
            # projected_points (objective column), mean, std.
            return [
                (1, 9000, 10, 50.0, 50.0, 5.0),
                (2, 8000, 20, 45.0, 45.0, 5.0),
                (3, 7000, 30, 40.0, 40.0, 5.0),
                (4, 6000, 40, 35.0, 35.0, 5.0),
                (5, 5000, 50, 30.0, 30.0, 5.0),
                (6, 4000, 60, 25.0, 25.0, 5.0),
            ]
        return []

    hook.get_records.side_effect = get_records_side_effect

    with (
        patch(
            "include.nascar.dfs.lineup_generator_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._active_rules",
            return_value=(1, {}),
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_predict_snapshot",
            return_value={"id": 99, "week_id": 5},
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task._latest_mc_batch",
            return_value=(7, 1000, None),
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task.generate_candidate_pool",
            return_value=[],
        ),
    ):
        result = generate_draftkings_lineups_task()

    assert result["skipped"] is True
    assert result["reason"] == "no_feasible_lineups"
    assert result["mc_batch_id"] == 7
    assert result["mc_n_simulations"] == 1000
    # No artifact -> the lineup task synthesized summary-fallback joint samples,
    # so every strategy's stamped score_method should reflect that.
    assert set(result["score_methods"].values()) == {"summary_fallback"}


def test_persist_strategy_lineups_uses_run_in_transaction() -> None:
    hook = MagicMock()
    pool = _six_pool()
    selected = [_score_for((0, 1, 2, 3, 4, 5))]

    def _run_in_transaction(write_fn: object) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = (1,)
        cursor.lastrowid = 123
        write_fn(None, cursor)  # type: ignore[misc]

    hook.run_in_transaction.side_effect = _run_in_transaction

    mod._persist_strategy_lineups(
        hook,
        dfs_id=1,
        race_id=42,
        strategy="DOUBLE_UP",
        pool=pool,
        selected=selected,
        objective_metric="joint_p10",
        score_method="joint_p10",
    )

    hook.run_in_transaction.assert_called_once()


def test_strategy_id_by_name_returns_id_when_row_present() -> None:
    """Happy path: the lookup returns the dfs_strategies.id for a present row.

    Pins both required strategy names so a future rename of either constant
    (or an accidental drop of one from the seed/migration) fails CI before
    it can crash the lineup DAG with ``dfs_strategies row not found``.
    """
    for name, expected_id in (("DOUBLE_UP", 1), ("HIGH_RISK_REWARD", 2)):
        cursor = MagicMock()
        cursor.fetchone.return_value = (expected_id,)

        result = mod._strategy_id_by_name(cursor, name)

        assert result == expected_id
        cursor.execute.assert_called_once()
        sql_arg = cursor.execute.call_args.args[0]
        params_arg = cursor.execute.call_args.args[1]
        assert "FROM nascar.dfs_strategies" in sql_arg
        assert "deleted_at IS NULL" in sql_arg
        assert params_arg == (name,)


def test_strategy_id_by_name_raises_actionable_error_when_missing() -> None:
    """Missing-row failure: error message names the strategy that is absent.

    This is the regression that crashed lineup generation when the DB came
    up from a snapshot pre-dating migration
    ``20260525180000_dfs_strategies_and_race_lineups_pivot.sql``. The fix
    (canonical seed ``9003-insert_dfs_strategies_seed.sql`` plus backfill
    migration ``20260613T140000_dfs_strategies_required_rows.sql``) is in
    the DB repo; this test pins the error message so the failure mode
    stays actionable if the row ever goes missing again.
    """
    cursor = MagicMock()
    cursor.fetchone.return_value = None

    with pytest.raises(RuntimeError, match=r"dfs_strategies row not found for name='DOUBLE_UP'"):
        mod._strategy_id_by_name(cursor, "DOUBLE_UP")


def test_strategy_configs_pick_up_env_overrides() -> None:
    """Per-strategy objective + exposure cap must flow through env vars."""
    overrides = {
        "FS_DK_DOUBLE_UP_OBJECTIVE": "joint_sharpe",
        "FS_DK_DOUBLE_UP_EXPOSURE_CAP": "0.55",
        "FS_DK_HIGH_RISK_REWARD_OBJECTIVE": "joint_mean",
        "FS_DK_HIGH_RISK_REWARD_EXPOSURE_CAP": "0.30",
    }
    with patch.dict("os.environ", overrides, clear=False):
        configs = mod._strategy_configs()
    assert configs["DOUBLE_UP"].objective_metric == "joint_sharpe"
    assert configs["DOUBLE_UP"].exposure_cap == 0.55
    assert configs["HIGH_RISK_REWARD"].objective_metric == "joint_mean"
    assert configs["HIGH_RISK_REWARD"].exposure_cap == 0.30


def test_strategy_configs_default_exposure_caps_follow_python_fallbacks() -> None:
    """Pin the new uniform 0.35 default for both strategies.

    Guard the code-level fallbacks in ``_strategy_configs``. These are
    intentionally distinct from docker-compose/.env defaults.
    """
    import os

    env_without_overrides = {
        k: v
        for k, v in os.environ.items()
        if k
        not in {
            "FS_DK_DOUBLE_UP_OBJECTIVE",
            "FS_DK_DOUBLE_UP_EXPOSURE_CAP",
            "FS_DK_HIGH_RISK_REWARD_OBJECTIVE",
            "FS_DK_HIGH_RISK_REWARD_EXPOSURE_CAP",
        }
    }
    with patch.dict("os.environ", env_without_overrides, clear=True):
        configs = mod._strategy_configs()
    assert configs["DOUBLE_UP"].exposure_cap == 0.35
    assert configs["HIGH_RISK_REWARD"].exposure_cap == 0.35


def test_strategy_configs_reject_unknown_objective() -> None:
    """Typo'd objective falls back to the joint_pXX default rather than crashing."""
    with patch.dict(
        "os.environ", {"FS_DK_DOUBLE_UP_OBJECTIVE": "garbage_token"}, clear=False
    ):
        configs = mod._strategy_configs()
    assert configs["DOUBLE_UP"].objective_metric == "joint_p10"


def test_resolve_score_method_marks_fallback() -> None:
    """``summary_fallback`` overrides the objective name in the audit column."""
    assert mod._resolve_score_method("joint_p10", "") == "joint_p10"
    assert mod._resolve_score_method("joint_p90", "_fallback") == "summary_fallback"


def test_load_joint_samples_reindexes_to_pool_order(tmp_path: Path) -> None:
    """Artifact rows must be reindexed to match the player pool's order.

    Stale row ordering in the artifact (e.g. drivers ordered by car_driver_id
    asc) must not desynchronize the candidate-index based scoring downstream.
    """
    artifact = tmp_path / "5.npz"
    # Artifact stores drivers 7, 3, 5 with distinct row signatures so we can
    # observe the reindex mapping.
    matrix = np.array(
        [
            [70.0, 71.0, 72.0],  # driver 7
            [30.0, 31.0, 32.0],  # driver 3
            [50.0, 51.0, 52.0],  # driver 5
        ],
        dtype=float,
    )
    np.savez_compressed(
        artifact,
        joint_dfs_points=matrix,
        car_driver_ids=np.array([7, 3, 5], dtype=np.int64),
        meta=np.array((5, 1, 3, 0), dtype=[
            ("batch_id", "i8"), ("snapshot_id", "i8"),
            ("n_simulations", "i8"), ("random_seed", "i8"),
        ]),
    )

    pool = [
        _PoolEntry(
            player=LineupPlayer(car_driver_id=3, salary=9000, projected_points=30.0),
            mean=30.0,
            std=2.0,
        ),
        _PoolEntry(
            player=LineupPlayer(car_driver_id=5, salary=8000, projected_points=50.0),
            mean=50.0,
            std=2.0,
        ),
        _PoolEntry(
            player=LineupPlayer(car_driver_id=7, salary=7000, projected_points=70.0),
            mean=70.0,
            std=2.0,
        ),
    ]
    source = _load_joint_samples_from_artifact(
        str(artifact), pool=pool, expected_n_simulations=3
    )
    assert source is not None
    # Row 0 of the aligned matrix should be the driver 3 signature, etc.
    np.testing.assert_array_equal(
        source.matrix,
        np.array(
            [
                [30.0, 31.0, 32.0],
                [50.0, 51.0, 52.0],
                [70.0, 71.0, 72.0],
            ]
        ),
    )
    assert source.score_method_suffix == ""


def test_load_joint_samples_returns_none_when_driver_missing(tmp_path: Path) -> None:
    artifact = tmp_path / "11.npz"
    np.savez_compressed(
        artifact,
        joint_dfs_points=np.array([[1.0, 2.0]]),
        car_driver_ids=np.array([99], dtype=np.int64),
        meta=np.array((11, 1, 2, 0), dtype=[
            ("batch_id", "i8"), ("snapshot_id", "i8"),
            ("n_simulations", "i8"), ("random_seed", "i8"),
        ]),
    )
    pool = [
        _PoolEntry(
            player=LineupPlayer(car_driver_id=1, salary=9000, projected_points=10.0),
            mean=10.0,
            std=1.0,
        ),
    ]
    assert (
        _load_joint_samples_from_artifact(
            str(artifact), pool=pool, expected_n_simulations=2
        )
        is None
    )


def test_resolve_joint_samples_falls_back_when_artifact_missing(
    tmp_path: Path,
) -> None:
    """Pointing at a non-existent path must yield the summary-fallback matrix."""
    pool = [
        _PoolEntry(
            player=LineupPlayer(car_driver_id=1, salary=9000, projected_points=10.0),
            mean=20.0,
            std=3.0,
        ),
        _PoolEntry(
            player=LineupPlayer(car_driver_id=2, salary=8000, projected_points=8.0),
            mean=15.0,
            std=2.0,
        ),
    ]
    source = _resolve_joint_samples(
        pool,
        artifact_uri=str(tmp_path / "does-not-exist.npz"),
        expected_n_simulations=1000,
        snapshot_id=42,
    )
    assert source.score_method_suffix == "_fallback"
    assert source.matrix.shape == (2, mod._SUMMARY_FALLBACK_SIMULATIONS)
    # Means should be close to the pool's per-driver mean (large N).
    assert abs(float(np.mean(source.matrix[0])) - 20.0) < 0.5
    assert abs(float(np.mean(source.matrix[1])) - 15.0) < 0.5
