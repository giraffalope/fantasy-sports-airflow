"""Unit tests for lineup generator skip gates."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from include.nascar.dfs import lineup_generator_task as mod
from include.nascar.dfs.lineup_generator_task import (
    _insert_lineup,
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


def test_skips_without_ml_predictions() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(0,)]
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
    ):
        result = generate_draftkings_lineups_task()
    assert result["skipped"] is True
    assert result["reason"] == "no_ml_predictions"


def test_skips_without_dk_salaries() -> None:
    hook = MagicMock()

    def get_records_side_effect(sql: str, parameters: tuple | None = None) -> list:
        if "COUNT(*)" in sql:
            return [(10,)]
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
    ):
        result = generate_draftkings_lineups_task()
    assert result["skipped"] is True
    assert result["reason"] == "no_dk_salaries"
    assert result["week_id"] == 5


def test_salary_sql_uses_week_id() -> None:
    assert "r.week_id = %s" in mod._SALARIES_EXIST_SQL
    assert "r.week_id = %s" in mod._PLAYER_POOL_SQL


def test_insert_lineup_uses_run_insert_returning_id() -> None:
    hook = MagicMock()
    hook.run_insert_returning_id.return_value = 123
    players = [
        LineupPlayer(car_driver_id=1, salary=9000, projected_points=50.0, dfs_driver_id=10),
        LineupPlayer(car_driver_id=2, salary=8000, projected_points=40.0, dfs_driver_id=20),
        LineupPlayer(car_driver_id=3, salary=7000, projected_points=30.0, dfs_driver_id=30),
        LineupPlayer(car_driver_id=4, salary=6000, projected_points=20.0, dfs_driver_id=40),
        LineupPlayer(car_driver_id=5, salary=5000, projected_points=10.0, dfs_driver_id=50),
        LineupPlayer(car_driver_id=6, salary=4000, projected_points=5.0, dfs_driver_id=60),
    ]
    lineup_id = _insert_lineup(
        hook,
        dfs_id=1,
        race_id=42,
        strategy="DOUBLE_UP",
        players=players,
        indices=[0, 1, 2, 3, 4, 5],
    )
    assert lineup_id == 123
    hook.run_insert_returning_id.assert_called_once()
    child_calls = [c for c in hook.run.call_args_list if "dfs_race_lineups_car_drivers" in str(c)]
    assert len(child_calls) == 6
    for call in child_calls:
        params = call.kwargs.get("parameters") or call[0][1]
        assert params[0] == 123
