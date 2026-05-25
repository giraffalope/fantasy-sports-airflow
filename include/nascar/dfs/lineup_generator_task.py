"""Generate DraftKings DFS lineups from Monte Carlo simulation summaries and salaries."""

from __future__ import annotations

import logging
import os
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

# Monte Carlo summary columns the optimizer can target via ``FS_DK_MC_OBJECTIVE``.
# Each entry is the bare summary column name; ``mean`` is the expectation, ``p50``
# the median, ``p95`` the upside-aware percentile, etc.
_MC_OBJECTIVE_COLUMNS: dict[str, str] = {
    "mean": "mean_dfs_points",
    "p05": "p05_dfs_points",
    "p50": "p50_dfs_points",
    "p95": "p95_dfs_points",
    "min": "min_dfs_points",
    "max": "max_dfs_points",
}

_DEFAULT_MC_OBJECTIVE = "p50"


def _mc_objective() -> str:
    raw = os.environ.get("FS_DK_MC_OBJECTIVE")
    if raw is None or raw.strip() == "":
        return _DEFAULT_MC_OBJECTIVE
    key = raw.strip().lower()
    if key not in _MC_OBJECTIVE_COLUMNS:
        _LOG.warning(
            "Invalid FS_DK_MC_OBJECTIVE=%r; falling back to %s. Valid keys: %s",
            raw,
            _DEFAULT_MC_OBJECTIVE,
            sorted(_MC_OBJECTIVE_COLUMNS),
        )
        return _DEFAULT_MC_OBJECTIVE
    return key


_RACE_FOR_WEEK_SQL = """
SELECT r.id
FROM nascar.races r
WHERE r.week_id = %s
  AND r.deleted_at IS NULL
LIMIT 1
"""

_LATEST_MC_BATCH_SQL = """
SELECT id, n_simulations
FROM nascar.ml_race_simulation_batches
WHERE ml_training_snapshot_id = %s
  AND deleted_at IS NULL
ORDER BY id DESC
LIMIT 1
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

# {objective_column} is interpolated server-side from ``_mc_objective()`` and
# validated against ``_MC_OBJECTIVE_COLUMNS`` so no untrusted string ever reaches
# the query.
_PLAYER_POOL_SQL_TEMPLATE = """
SELECT
    s.car_driver_id,
    ds.salary,
    ds.dfs_driver_id,
    s.{objective_column} AS projected_points
FROM nascar.ml_race_simulation_driver_summaries s
JOIN nascar.dfs_salaries ds
  ON ds.car_driver_id = s.car_driver_id
 AND ds.dfs_id = %s
 AND ds.deleted_at IS NULL
JOIN nascar.races r
  ON r.id = ds.race_id
 AND r.week_id = %s
 AND r.deleted_at IS NULL
WHERE s.ml_race_simulation_batch_id = %s
  AND s.deleted_at IS NULL
"""


def _race_id_for_week(hook: NascarMySqlHook, week_id: int) -> int | None:
    rows = hook.get_records(_RACE_FOR_WEEK_SQL, parameters=(week_id,))
    if not rows:
        return None
    return int(rows[0][0])


def _latest_mc_batch(
    hook: NascarMySqlHook, snapshot_id: int
) -> tuple[int, int] | None:
    """Return ``(batch_id, n_simulations)`` for the most recent active MC batch."""
    rows = hook.get_records(_LATEST_MC_BATCH_SQL, parameters=(snapshot_id,))
    if not rows:
        return None
    return int(rows[0][0]), int(rows[0][1])


def _load_player_pool(
    hook: NascarMySqlHook,
    *,
    mc_batch_id: int,
    week_id: int,
    objective_column: str,
) -> list[LineupPlayer]:
    sql = _PLAYER_POOL_SQL_TEMPLATE.format(objective_column=objective_column)
    rows = hook.get_records(
        sql,
        parameters=(DRAFTKINGS_DFS_ID, week_id, mc_batch_id),
    )
    players: list[LineupPlayer] = []
    for car_driver_id, salary, dfs_driver_id, projected_points in rows:
        players.append(
            LineupPlayer(
                car_driver_id=int(car_driver_id),
                salary=int(salary),
                projected_points=float(projected_points),
                dfs_driver_id=int(dfs_driver_id) if dfs_driver_id is not None else None,
            )
        )
    return players


def _strategy_id_by_name(cursor: DbCursor, name: str) -> int:
    cursor.execute(
        """
        SELECT id
        FROM nascar.dfs_strategies
        WHERE name = %s
          AND deleted_at IS NULL
        LIMIT 1
        """,
        (name,),
    )
    row = cursor.fetchone()
    if not row:
        raise RuntimeError(f"dfs_strategies row not found for name={name!r}")
    return int(row[0])


def _insert_lineup_on_cursor(
    cursor: DbCursor,
    *,
    dfs_id: int,
    race_id: int,
    dfs_strategy_id: int,
    players: list[LineupPlayer],
    indices: list[int],
) -> int:
    picked = [players[i] for i in indices]
    projected_total = round(sum(p.projected_points for p in picked), 2)
    salary_total = sum(p.salary for p in picked)

    cursor.execute(
        """
        INSERT INTO nascar.dfs_race_lineups (
            dfs_id, race_id, projected_points_total, salary_total
        ) VALUES (%s, %s, %s, %s)
        """,
        (dfs_id, race_id, projected_total, salary_total),
    )
    lineup_id = int(cursor.lastrowid)
    if not lineup_id:
        raise RuntimeError("INSERT dfs_race_lineups did not return an auto-increment id")

    cursor.execute(
        """
        INSERT INTO nascar.dfs_strategies_race_lineups (
            dfs_strategy_id, dfs_race_lineup_id
        ) VALUES (%s, %s)
        """,
        (dfs_strategy_id, lineup_id),
    )

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
        dfs_strategy_id = _strategy_id_by_name(cursor, strategy)
        cursor.execute(
            """
            UPDATE nascar.dfs_race_lineups drl
            JOIN nascar.dfs_strategies_race_lineups dsrl
                ON dsrl.dfs_race_lineup_id = drl.id
                AND dsrl.deleted_at IS NULL
            JOIN nascar.dfs_strategies ds
                ON ds.id = dsrl.dfs_strategy_id
                AND ds.deleted_at IS NULL
            SET drl.deleted_at = UTC_TIMESTAMP(),
                dsrl.deleted_at = UTC_TIMESTAMP()
            WHERE drl.dfs_id = %s
              AND drl.race_id = %s
              AND ds.name = %s
              AND drl.deleted_at IS NULL
            """,
            (dfs_id, race_id, strategy),
        )
        for indices in lineups:
            _insert_lineup_on_cursor(
                cursor,
                dfs_id=dfs_id,
                race_id=race_id,
                dfs_strategy_id=dfs_strategy_id,
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

    mc_batch = _latest_mc_batch(hook, snapshot["id"])
    if mc_batch is None:
        _LOG.warning(
            "generate_draftkings_lineups: skipped reason=no_mc_summaries snapshot_id=%s",
            snapshot["id"],
        )
        return {"skipped": True, "reason": "no_mc_summaries", "snapshot_id": snapshot["id"]}
    mc_batch_id, mc_n_simulations = mc_batch

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

    objective = _mc_objective()
    objective_column = _MC_OBJECTIVE_COLUMNS[objective]
    players = _load_player_pool(
        hook,
        mc_batch_id=mc_batch_id,
        week_id=week_id,
        objective_column=objective_column,
    )
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
            "mc_batch_id": mc_batch_id,
            "mc_n_simulations": mc_n_simulations,
            "mc_objective": objective,
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
        "mc_batch_id": mc_batch_id,
        "mc_n_simulations": mc_n_simulations,
        "mc_objective": objective,
        "lineups_inserted": inserted_by_strategy,
    }
