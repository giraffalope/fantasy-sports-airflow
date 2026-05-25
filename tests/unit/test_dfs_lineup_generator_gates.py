"""Unit tests for lineup generator skip gates."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from include.nascar.dfs import lineup_generator_task as mod
from include.nascar.dfs.lineup_generator_task import (
    _insert_lineup_on_cursor,
    generate_draftkings_lineups_task,
)
from include.nascar.dfs.lineup_optimizer import LineupPlayer


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
            return_value=(7, 1000),
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


def test_insert_lineup_on_cursor_uses_lastrowid_for_child_rows() -> None:
    cursor = MagicMock()
    cursor.lastrowid = 123
    players = [
        LineupPlayer(car_driver_id=1, salary=9000, projected_points=50.0, dfs_driver_id=10),
        LineupPlayer(car_driver_id=2, salary=8000, projected_points=40.0, dfs_driver_id=20),
        LineupPlayer(car_driver_id=3, salary=7000, projected_points=30.0, dfs_driver_id=30),
        LineupPlayer(car_driver_id=4, salary=6000, projected_points=20.0, dfs_driver_id=40),
        LineupPlayer(car_driver_id=5, salary=5000, projected_points=10.0, dfs_driver_id=50),
        LineupPlayer(car_driver_id=6, salary=4000, projected_points=5.0, dfs_driver_id=60),
    ]
    lineup_id = _insert_lineup_on_cursor(
        cursor,
        dfs_id=1,
        race_id=42,
        dfs_strategy_id=1,
        players=players,
        indices=[0, 1, 2, 3, 4, 5],
    )
    assert lineup_id == 123
    assert cursor.execute.call_count == 2
    lineup_sql = cursor.execute.call_args_list[0][0][0]
    pivot_sql = cursor.execute.call_args_list[1][0][0]
    assert "dfs_race_lineups" in lineup_sql
    assert "strategy" not in lineup_sql.lower()
    assert "dfs_strategies_race_lineups" in pivot_sql
    cursor.executemany.assert_called_once()
    child_rows = cursor.executemany.call_args[0][1]
    assert len(child_rows) == 6
    assert all(row[0] == 123 for row in child_rows)


def _six_player_pool() -> list[LineupPlayer]:
    return [
        LineupPlayer(
            car_driver_id=i, salary=5000 + i * 100, projected_points=40.0 - i, dfs_driver_id=100 + i
        )
        for i in range(1, 7)
    ]


def test_no_feasible_lineups_when_optimizer_returns_empty() -> None:
    hook = MagicMock()

    def get_records_side_effect(sql: str, parameters: tuple | None = None) -> list:
        if "SELECT r.id" in sql:
            return [(42,)]
        if "SELECT 1" in sql and "dfs_salaries" in sql:
            return [(1,)]
        if "ml_race_simulation_driver_summaries" in sql:
            return [
                (1, 9000, 10, 50.0),
                (2, 8000, 20, 45.0),
                (3, 7000, 30, 40.0),
                (4, 6000, 40, 35.0),
                (5, 5000, 50, 30.0),
                (6, 4000, 60, 25.0),
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
            return_value=(7, 1000),
        ),
        patch(
            "include.nascar.dfs.lineup_generator_task.generate_top_k_lineups",
            return_value=[],
        ),
    ):
        result = generate_draftkings_lineups_task()

    assert result["skipped"] is True
    assert result["reason"] == "no_feasible_lineups"
    assert result["mc_batch_id"] == 7
    assert result["mc_n_simulations"] == 1000


def test_persist_strategy_lineups_uses_run_in_transaction() -> None:
    hook = MagicMock()
    players = _six_player_pool()
    lineups = [[0, 1, 2, 3, 4, 5]]

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
        players=players,
        lineups=lineups,
    )

    hook.run_in_transaction.assert_called_once()
