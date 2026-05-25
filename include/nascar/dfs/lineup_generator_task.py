"""Generate DraftKings DFS lineups from ML predictions and imported salaries."""

from __future__ import annotations

import logging
from typing import Any

from airflow_utils.db_cursor import DbCursor
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
FROM nascar.dfs_salaries ds
JOIN nascar.races r
  ON r.id = ds.race_id
 AND r.deleted_at IS NULL
WHERE ds.dfs_id = %s
  AND r.week_id = %s
  AND ds.deleted_at IS NULL
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
 AND ds.dfs_id = %s
 AND ds.deleted_at IS NULL
JOIN nascar.races r
  ON r.id = ds.race_id
 AND r.week_id = %s
 AND r.deleted_at IS NULL
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
    week_id: int,
) -> list[LineupPlayer]:
    rows = hook.get_records(
        _PLAYER_POOL_SQL,
        parameters=(DRAFTKINGS_DFS_ID, week_id, snapshot_id),
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


def _insert_lineup_on_cursor(
    cursor: DbCursor,
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

    cursor.execute(
        """
        INSERT INTO nascar.dfs_race_lineups (
            dfs_id, race_id, strategy, projected_points_total, salary_total
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        (dfs_id, race_id, strategy, projected_total, salary_total),
    )
    lineup_id = int(cursor.lastrowid)
    if not lineup_id:
        raise RuntimeError("INSERT dfs_race_lineups did not return an auto-increment id")

    child_rows = [
        (
            lineup_id,
            player.car_driver_id,
            player.dfs_driver_id,
            round(player.projected_points, 2),
            player.salary,
        )
        for player in picked
    ]
    if child_rows:
        cursor.executemany(
            """
            INSERT INTO nascar.dfs_race_lineups_car_drivers (
                dfs_race_lineup_id,
                car_driver_id,
                dfs_driver_id,
                projected_points,
                salary
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            child_rows,
        )
    return lineup_id


def _persist_strategy_lineups(
    hook: NascarMySqlHook,
    *,
    dfs_id: int,
    race_id: int,
    strategy: str,
    players: list[LineupPlayer],
    lineups: list[list[int]],
) -> int:
    def _write(_conn: object, cursor: DbCursor) -> None:
        cursor.execute(
            """
            UPDATE nascar.dfs_race_lineups
            SET deleted_at = UTC_TIMESTAMP()
            WHERE dfs_id = %s
              AND race_id = %s
              AND strategy = %s
              AND deleted_at IS NULL
            """,
            (dfs_id, race_id, strategy),
        )
        for indices in lineups:
            _insert_lineup_on_cursor(
                cursor,
                dfs_id=dfs_id,
                race_id=race_id,
                strategy=strategy,
                players=players,
                indices=indices,
            )

    hook.run_in_transaction(_write)
    return len(lineups)


def generate_draftkings_lineups_task(**_context: object) -> dict[str, Any]:
    """Build top-K lineups per strategy when predictions and salaries exist."""
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")

    rules = _active_rules(hook, DRAFTKINGS_DFS_ID)
    if rules is None:
        _LOG.warning("generate_draftkings_lineups: skipped reason=no_active_rules")
        return {"skipped": True, "reason": "no_active_rules"}

    rules_id, _rules = rules

    snapshot = _latest_predict_snapshot(hook, rules_id)
    if snapshot is None:
        _LOG.warning("generate_draftkings_lineups: skipped reason=no_predict_snapshot")
        return {"skipped": True, "reason": "no_predict_snapshot"}

    pred_rows = hook.get_records(
        _PREDICTION_COUNT_SQL,
        parameters=(snapshot["id"],),
    )
    prediction_count = int(pred_rows[0][0]) if pred_rows else 0
    if prediction_count <= 0:
        _LOG.warning(
            "generate_draftkings_lineups: skipped reason=no_ml_predictions snapshot_id=%s",
            snapshot["id"],
        )
        return {"skipped": True, "reason": "no_ml_predictions", "snapshot_id": snapshot["id"]}

    week_id = snapshot["week_id"]
    race_id = _race_id_for_week(hook, week_id)
    if race_id is None:
        _LOG.warning(
            "generate_draftkings_lineups: skipped reason=no_race_for_week week_id=%s",
            week_id,
        )
        return {"skipped": True, "reason": "no_race_for_week", "week_id": week_id}

    salary_rows = hook.get_records(
        _SALARIES_EXIST_SQL,
        parameters=(DRAFTKINGS_DFS_ID, week_id),
    )
    if not salary_rows:
        _LOG.warning(
            "generate_draftkings_lineups: skipped reason=no_dk_salaries week_id=%s race_id=%s",
            week_id,
            race_id,
        )
        return {"skipped": True, "reason": "no_dk_salaries", "week_id": week_id, "race_id": race_id}

    players = _load_player_pool(hook, snapshot_id=snapshot["id"], week_id=week_id)
    if len(players) < dk_lineup_size():
        _LOG.warning(
            "generate_draftkings_lineups: skipped reason=insufficient_player_pool "
            "pool_size=%s required=%s week_id=%s",
            len(players),
            dk_lineup_size(),
            week_id,
        )
        return {
            "skipped": True,
            "reason": "insufficient_player_pool",
            "pool_size": len(players),
            "required": dk_lineup_size(),
            "week_id": week_id,
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
        count = _persist_strategy_lineups(
            hook,
            dfs_id=DRAFTKINGS_DFS_ID,
            race_id=race_id,
            strategy=strategy,
            players=players,
            lineups=lineups,
        )
        inserted_by_strategy[strategy] = count
        _LOG.info(
            "generate_draftkings_lineups: race_id=%s strategy=%s lineups=%s min_diff=%s",
            race_id,
            strategy,
            count,
            min_diff,
        )

    total_inserted = sum(inserted_by_strategy.values())
    if total_inserted == 0:
        _LOG.warning(
            "generate_draftkings_lineups: skipped reason=no_feasible_lineups "
            "race_id=%s pool_size=%s salary_cap=%s",
            race_id,
            len(players),
            salary_cap,
        )
        return {
            "skipped": True,
            "reason": "no_feasible_lineups",
            "snapshot_id": snapshot["id"],
            "race_id": race_id,
            "week_id": week_id,
            "pool_size": len(players),
            "lineup_size": lineup_size,
            "salary_cap": salary_cap,
            "top_k": top_k,
            "lineups_inserted": inserted_by_strategy,
        }

    return {
        "skipped": False,
        "snapshot_id": snapshot["id"],
        "race_id": race_id,
        "week_id": week_id,
        "pool_size": len(players),
        "lineup_size": lineup_size,
        "salary_cap": salary_cap,
        "top_k": top_k,
        "lineups_inserted": inserted_by_strategy,
    }
