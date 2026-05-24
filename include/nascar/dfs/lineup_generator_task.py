"""Generate DraftKings DFS lineups from ML predictions and imported salaries."""

from __future__ import annotations

import logging
from typing import Any

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.dfs.lineup_optimizer import (
    LineupPlayer,
    dk_lineup_size,
    dk_min_diff_double_up,
    dk_min_diff_high_risk_reward,
    dk_salary_cap,
    dk_top_k,
    generate_top_k_lineups,
)
from include.nascar.ml.predict_task import _active_rules, _latest_predict_snapshot
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID

_LOG = logging.getLogger(__name__)

_STRATEGY_MIN_DIFF = {
    "DOUBLE_UP": dk_min_diff_double_up,
    "HIGH_RISK_REWARD": dk_min_diff_high_risk_reward,
}

_RACE_FOR_WEEK_SQL = """
SELECT r.id
FROM nascar.races r
WHERE r.week_id = %s
  AND r.deleted_at IS NULL
LIMIT 1
"""

_PREDICTION_COUNT_SQL = """
SELECT COUNT(*)
FROM nascar.ml_predictions
WHERE ml_training_snapshot_id = %s
  AND deleted_at IS NULL
"""

_SALARIES_EXIST_SQL = """
SELECT 1
FROM nascar.dfs_salaries
WHERE dfs_id = %s
  AND race_id = %s
  AND deleted_at IS NULL
LIMIT 1
"""

_PLAYER_POOL_SQL = """
SELECT
    mp.car_driver_id,
    ds.salary,
    ds.dfs_driver_id,
    mp.predicted_dfs_points
FROM nascar.ml_predictions mp
JOIN nascar.dfs_salaries ds
  ON ds.car_driver_id = mp.car_driver_id
 AND ds.race_id = %s
 AND ds.dfs_id = %s
 AND ds.deleted_at IS NULL
WHERE mp.ml_training_snapshot_id = %s
  AND mp.deleted_at IS NULL
"""


def _race_id_for_week(hook: NascarMySqlHook, week_id: int) -> int | None:
    rows = hook.get_records(_RACE_FOR_WEEK_SQL, parameters=(week_id,))
    if not rows:
        return None
    return int(rows[0][0])


def _load_player_pool(
    hook: NascarMySqlHook,
    *,
    snapshot_id: int,
    race_id: int,
) -> list[LineupPlayer]:
    rows = hook.get_records(
        _PLAYER_POOL_SQL,
        parameters=(race_id, DRAFTKINGS_DFS_ID, snapshot_id),
    )
    players: list[LineupPlayer] = []
    for car_driver_id, salary, dfs_driver_id, predicted_points in rows:
        players.append(
            LineupPlayer(
                car_driver_id=int(car_driver_id),
                salary=int(salary),
                projected_points=float(predicted_points),
                dfs_driver_id=int(dfs_driver_id) if dfs_driver_id is not None else None,
            )
        )
    return players


def _soft_delete_lineups(
    hook: NascarMySqlHook,
    *,
    dfs_id: int,
    race_id: int,
    strategy: str,
) -> None:
    hook.run(
        """
        UPDATE nascar.dfs_race_lineups
        SET deleted_at = UTC_TIMESTAMP()
        WHERE dfs_id = %s
          AND race_id = %s
          AND strategy = %s
          AND deleted_at IS NULL
        """,
        parameters=(dfs_id, race_id, strategy),
    )


def _insert_lineup(
    hook: NascarMySqlHook,
    *,
    dfs_id: int,
    race_id: int,
    strategy: str,
    players: list[LineupPlayer],
    indices: list[int],
) -> int:
    picked = [players[i] for i in indices]
    projected_total = round(sum(p.projected_points for p in picked), 2)
    salary_total = sum(p.salary for p in picked)

    hook.run(
        """
        INSERT INTO nascar.dfs_race_lineups (
            dfs_id, race_id, strategy, projected_points_total, salary_total
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        parameters=(dfs_id, race_id, strategy, projected_total, salary_total),
    )
    lineup_id_rows = hook.get_records("SELECT LAST_INSERT_ID()")
    lineup_id = int(lineup_id_rows[0][0])

    for player in picked:
        hook.run(
            """
            INSERT INTO nascar.dfs_race_lineups_car_drivers (
                dfs_race_lineup_id,
                car_driver_id,
                dfs_driver_id,
                projected_points,
                salary
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            parameters=(
                lineup_id,
                player.car_driver_id,
                player.dfs_driver_id,
                round(player.projected_points, 2),
                player.salary,
            ),
        )
    return lineup_id


def generate_draftkings_lineups_task(**_context: object) -> dict[str, Any]:
    """Build top-K lineups per strategy when predictions and salaries exist."""
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")

    rules = _active_rules(hook, DRAFTKINGS_DFS_ID)
    if rules is None:
        return {"skipped": True, "reason": "no_active_rules"}
    rules_id, _rules = rules

    snapshot = _latest_predict_snapshot(hook, rules_id)
    if snapshot is None:
        return {"skipped": True, "reason": "no_predict_snapshot"}

    pred_rows = hook.get_records(
        _PREDICTION_COUNT_SQL,
        parameters=(snapshot["id"],),
    )
    prediction_count = int(pred_rows[0][0]) if pred_rows else 0
    if prediction_count <= 0:
        return {"skipped": True, "reason": "no_ml_predictions", "snapshot_id": snapshot["id"]}

    race_id = _race_id_for_week(hook, snapshot["week_id"])
    if race_id is None:
        return {"skipped": True, "reason": "no_race_for_week", "week_id": snapshot["week_id"]}

    salary_rows = hook.get_records(
        _SALARIES_EXIST_SQL,
        parameters=(DRAFTKINGS_DFS_ID, race_id),
    )
    if not salary_rows:
        return {"skipped": True, "reason": "no_dk_salaries", "race_id": race_id}

    players = _load_player_pool(hook, snapshot_id=snapshot["id"], race_id=race_id)
    if len(players) < dk_lineup_size():
        return {
            "skipped": True,
            "reason": "insufficient_player_pool",
            "pool_size": len(players),
            "required": dk_lineup_size(),
        }

    lineup_size = dk_lineup_size()
    salary_cap = dk_salary_cap()
    top_k = dk_top_k()
    inserted_by_strategy: dict[str, int] = {}

    for strategy, min_diff_fn in _STRATEGY_MIN_DIFF.items():
        min_diff = min_diff_fn()
        lineups = generate_top_k_lineups(
            players,
            lineup_size=lineup_size,
            salary_cap=salary_cap,
            top_k=top_k,
            min_diff=min_diff,
        )
        _soft_delete_lineups(
            hook,
            dfs_id=DRAFTKINGS_DFS_ID,
            race_id=race_id,
            strategy=strategy,
        )
        count = 0
        for indices in lineups:
            _insert_lineup(
                hook,
                dfs_id=DRAFTKINGS_DFS_ID,
                race_id=race_id,
                strategy=strategy,
                players=players,
                indices=indices,
            )
            count += 1
        inserted_by_strategy[strategy] = count
        _LOG.info(
            "generate_draftkings_lineups: race_id=%s strategy=%s lineups=%s min_diff=%s",
            race_id,
            strategy,
            count,
            min_diff,
        )

    return {
        "skipped": False,
        "snapshot_id": snapshot["id"],
        "race_id": race_id,
        "pool_size": len(players),
        "lineup_size": lineup_size,
        "salary_cap": salary_cap,
        "top_k": top_k,
        "lineups_inserted": inserted_by_strategy,
    }
