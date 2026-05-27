"""Generate DraftKings DFS lineups from Monte Carlo simulations and salaries.

End-to-end flow per run:

1. Discover the latest predict snapshot, its active Monte Carlo batch, and the
   DraftKings salary table for the race week.
2. Build the player pool from the per-driver MC summary table (so legacy
   ``mc_objective`` knob still controls which summary column CP-SAT optimizes
   against during candidate-pool generation).
3. Load the joint MC draws from the ``.npz`` sidecar referenced by
   ``ml_race_simulation_batches.joint_samples_artifact_uri``. When the artifact
   is missing (legacy batch, write failure, etc.) synthesize an independent
   Gaussian joint matrix from the per-driver summary columns and stamp every
   resulting lineup ``mc_score_method='summary_fallback'`` so the audit trail
   distinguishes degraded scoring.
4. For each strategy:
   * Solve CP-SAT for a candidate pool of size ``top_k * pool_multiplier``.
   * Score every candidate against the joint matrix (lineup mean / std / p10 /
     p50 / p90 / sharpe).
   * Apply :func:`greedy_diversify` with the strategy-specific objective metric
     and exposure cap to pick the final ``top_k`` lineups under
     per-driver exposure budgets.
5. Persist the resulting lineups along with the 8-column MC fingerprint
   (mean / std / p10 / p50 / p90 / sharpe / objective_value / mc_score_method).

The strategy split (DOUBLE_UP cash games vs HIGH_RISK_REWARD GPP tournaments)
is what differentiates this from the legacy single-objective optimizer: each
strategy gets its own objective metric (high-floor vs high-ceiling) and
exposure cap (loose vs strict) so the same MC batch yields two portfolios
with materially different risk profiles.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from airflow_utils.db_cursor import DbCursor
from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.dfs.lineup_optimizer import (
    LineupPlayer,
    dk_candidate_pool_multiplier,
    dk_lineup_size,
    dk_min_diff_double_up,
    dk_min_diff_high_risk_reward,
    dk_salary_cap,
    dk_top_k,
    generate_candidate_pool,
)
from include.nascar.dfs.lineup_portfolio import (
    JOINT_METRICS,
    SUMMARY_FALLBACK_METHOD,
    CandidateScore,
    greedy_diversify,
    objective_value,
    score_candidates_against_joint_samples,
)
from include.nascar.ml.predict_task import _active_rules, _latest_predict_snapshot
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID

_LOG = logging.getLogger(__name__)

# Synthetic-fallback sample count when no joint .npz artifact exists. Big
# enough that empirical percentiles stabilize without making the in-task
# memory footprint silly (n_drivers x 1000 x 8 bytes for a typical 40-driver
# pool is ~320 KB).
_SUMMARY_FALLBACK_SIMULATIONS = 1000


# Monte Carlo summary columns the candidate-pool CP-SAT can target via
# ``FS_DK_MC_OBJECTIVE``. Each entry is the bare summary column name; this
# now only controls the CP-SAT objective signal used to *order* the
# candidate pool, not the final lineup ranking — that's the
# portfolio-level joint-samples objective in :class:`StrategyConfig`.
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


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        _LOG.warning("Invalid %s=%r; falling back to %.2f", name, raw, default)
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        _LOG.warning("Invalid %s=%r; falling back to %d", name, raw, default)
        return default


def _resolve_metric(env_name: str, default_metric: str) -> str:
    """Read a strategy objective env var with validation against ``JOINT_METRICS``."""
    raw = os.environ.get(env_name)
    if raw is None or raw.strip() == "":
        return default_metric
    key = raw.strip().lower()
    if key not in JOINT_METRICS:
        _LOG.warning(
            "Invalid %s=%r; falling back to %s. Valid keys: %s",
            env_name,
            raw,
            default_metric,
            sorted(JOINT_METRICS),
        )
        return default_metric
    return key


@dataclass(frozen=True)
class StrategyConfig:
    """Per-strategy portfolio knobs resolved from env at task invocation time.

    The two strategies represent fundamentally different DFS contest
    structures:

    * ``DOUBLE_UP`` (cash / 50-50 contests): you cash if you finish in the
      top half of entrants. Floor matters far more than ceiling, so the
      objective is ``joint_p10`` (the 10th percentile lineup outcome) and
      the exposure cap is loose (``0.70``) — duplicating winning floor
      drivers across multiple cash lineups is fine.
    * ``HIGH_RISK_REWARD`` (GPP / tournaments): payout is heavily
      top-skewed, so the objective is ``joint_p90`` (high ceiling) and
      the exposure cap is tight (``0.40``) so no single bust derails the
      whole portfolio.
    """

    name: str
    objective_metric: str
    exposure_cap: float
    min_diff: int


def _strategy_configs() -> dict[str, StrategyConfig]:
    """Resolve all strategy configs from env vars."""
    return {
        "DOUBLE_UP": StrategyConfig(
            name="DOUBLE_UP",
            objective_metric=_resolve_metric(
                "FS_DK_DOUBLE_UP_OBJECTIVE", "joint_p10"
            ),
            exposure_cap=_env_float("FS_DK_DOUBLE_UP_EXPOSURE_CAP", 0.70),
            min_diff=dk_min_diff_double_up(),
        ),
        "HIGH_RISK_REWARD": StrategyConfig(
            name="HIGH_RISK_REWARD",
            objective_metric=_resolve_metric(
                "FS_DK_HIGH_RISK_REWARD_OBJECTIVE", "joint_p90"
            ),
            exposure_cap=_env_float(
                "FS_DK_HIGH_RISK_REWARD_EXPOSURE_CAP", 0.40
            ),
            min_diff=dk_min_diff_high_risk_reward(),
        ),
    }


_RACE_FOR_WEEK_SQL = """
SELECT r.id
FROM nascar.races r
WHERE r.week_id = %s
  AND r.deleted_at IS NULL
LIMIT 1
"""

_LATEST_MC_BATCH_SQL = """
SELECT id, n_simulations, joint_samples_artifact_uri
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

# Note: this query now also returns mean/std so the summary-fallback path
# can synthesize independent Gaussian draws without a second SELECT. The
# ``{objective_column}`` interpolation is bounded to the keys of
# ``_MC_OBJECTIVE_COLUMNS`` so no untrusted string ever reaches the
# server.
_PLAYER_POOL_SQL_TEMPLATE = """
SELECT
    s.car_driver_id,
    ds.salary,
    ds.dfs_driver_id,
    s.{objective_column} AS projected_points,
    s.mean_dfs_points,
    s.std_dfs_points
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


@dataclass(frozen=True)
class _PoolEntry:
    """Player pool row with the per-driver summary stats kept around for fallback."""

    player: LineupPlayer
    mean: float
    std: float


@dataclass(frozen=True)
class _JointSampleSource:
    """Joint-sample matrix already reindexed to the player-pool row order."""

    matrix: np.ndarray  # (n_drivers, n_simulations)
    score_method_suffix: str  # "" for real joint draws, "_fallback" for synth


def _race_id_for_week(hook: NascarMySqlHook, week_id: int) -> int | None:
    rows = hook.get_records(_RACE_FOR_WEEK_SQL, parameters=(week_id,))
    if not rows:
        return None
    return int(rows[0][0])


def _latest_mc_batch(
    hook: NascarMySqlHook, snapshot_id: int
) -> tuple[int, int, str | None] | None:
    """Return ``(batch_id, n_simulations, joint_samples_artifact_uri)`` or None."""
    rows = hook.get_records(_LATEST_MC_BATCH_SQL, parameters=(snapshot_id,))
    if not rows:
        return None
    batch_id = int(rows[0][0])
    n_sims = int(rows[0][1])
    artifact_uri = rows[0][2]
    if artifact_uri is not None and not isinstance(artifact_uri, str):
        artifact_uri = str(artifact_uri)
    return batch_id, n_sims, artifact_uri


def _load_player_pool(
    hook: NascarMySqlHook,
    *,
    mc_batch_id: int,
    week_id: int,
    objective_column: str,
) -> list[_PoolEntry]:
    sql = _PLAYER_POOL_SQL_TEMPLATE.format(objective_column=objective_column)
    rows = hook.get_records(
        sql,
        parameters=(DRAFTKINGS_DFS_ID, week_id, mc_batch_id),
    )
    pool: list[_PoolEntry] = []
    for row in rows:
        (
            car_driver_id,
            salary,
            dfs_driver_id,
            projected_points,
            mean_dfs_points,
            std_dfs_points,
        ) = row
        pool.append(
            _PoolEntry(
                player=LineupPlayer(
                    car_driver_id=int(car_driver_id),
                    salary=int(salary),
                    projected_points=float(projected_points),
                    dfs_driver_id=(
                        int(dfs_driver_id) if dfs_driver_id is not None else None
                    ),
                ),
                mean=float(mean_dfs_points),
                std=float(std_dfs_points),
            )
        )
    return pool


def _load_joint_samples_from_artifact(
    artifact_uri: str,
    *,
    pool: list[_PoolEntry],
    expected_n_simulations: int,
) -> _JointSampleSource | None:
    """Load the per-batch ``.npz`` artifact and reindex its rows to ``pool`` order.

    Returns ``None`` when the file is missing, unreadable, the shape doesn't
    match the expected ``n_simulations``, or the pool contains a driver
    absent from the artifact's ``car_driver_ids`` index. In those cases the
    caller falls back to summary-derived independent samples.
    """
    path = Path(artifact_uri)
    if not path.exists():
        _LOG.warning(
            "generate_draftkings_lineups: joint_samples_artifact_uri=%s does "
            "not exist on disk; falling back to summary-derived independent samples",
            artifact_uri,
        )
        return None
    try:
        with np.load(path, allow_pickle=False) as data:
            joint = np.asarray(data["joint_dfs_points"], dtype=float)
            ids = np.asarray(data["car_driver_ids"], dtype=np.int64)
    except Exception:
        _LOG.exception(
            "generate_draftkings_lineups: failed to load joint_samples artifact %s; "
            "falling back to summary-derived independent samples",
            artifact_uri,
        )
        return None

    if joint.ndim != 2 or joint.shape[0] != ids.shape[0]:
        _LOG.warning(
            "generate_draftkings_lineups: joint_samples artifact shape mismatch "
            "joint=%s ids=%s artifact=%s; falling back to summaries",
            joint.shape,
            ids.shape,
            artifact_uri,
        )
        return None
    if joint.shape[1] != expected_n_simulations:
        _LOG.warning(
            "generate_draftkings_lineups: joint_samples artifact n_simulations=%s "
            "differs from batch metadata n_simulations=%s artifact=%s; falling back",
            joint.shape[1],
            expected_n_simulations,
            artifact_uri,
        )
        return None

    id_to_row = {int(driver_id): row_idx for row_idx, driver_id in enumerate(ids)}
    aligned = np.zeros((len(pool), joint.shape[1]), dtype=float)
    for pool_idx, entry in enumerate(pool):
        source_row = id_to_row.get(entry.player.car_driver_id)
        if source_row is None:
            _LOG.warning(
                "generate_draftkings_lineups: joint_samples artifact missing "
                "car_driver_id=%s; falling back to summaries",
                entry.player.car_driver_id,
            )
            return None
        aligned[pool_idx] = joint[source_row]

    _LOG.info(
        "generate_draftkings_lineups: loaded joint_samples artifact=%s "
        "n_drivers=%s n_simulations=%s",
        artifact_uri,
        aligned.shape[0],
        aligned.shape[1],
    )
    return _JointSampleSource(matrix=aligned, score_method_suffix="")


def _synthesize_joint_samples_from_summaries(
    pool: list[_PoolEntry],
    *,
    n_simulations: int,
    snapshot_id: int,
) -> _JointSampleSource:
    """Independent Gaussian fallback for batches without a joint artifact.

    Each driver gets ``n_simulations`` independent samples from
    ``Normal(mean, max(std, eps))``. This is materially worse than true
    rank-coupled draws for portfolio selection (it inflates lineup
    variance and ignores the negative correlation between drivers'
    finish slots), so the lineup task stamps every resulting row's
    ``mc_score_method`` with the ``summary_fallback`` token.

    Deterministic seed derived from ``snapshot_id`` so re-runs over the
    same predict snapshot produce identical fallback portfolios.
    """
    rng = np.random.default_rng(snapshot_id * 1_000_003 + 31)
    n_drivers = len(pool)
    matrix = np.zeros((n_drivers, n_simulations), dtype=float)
    for i, entry in enumerate(pool):
        std = max(entry.std, 1e-6)
        matrix[i] = rng.normal(entry.mean, std, size=n_simulations)
    _LOG.info(
        "generate_draftkings_lineups: synthesized summary-fallback joint matrix "
        "n_drivers=%s n_simulations=%s",
        n_drivers,
        n_simulations,
    )
    return _JointSampleSource(matrix=matrix, score_method_suffix="_fallback")


def _resolve_joint_samples(
    pool: list[_PoolEntry],
    *,
    artifact_uri: str | None,
    expected_n_simulations: int,
    snapshot_id: int,
) -> _JointSampleSource:
    """Try the artifact first, fall through to independent Gaussian on any error."""
    if artifact_uri:
        source = _load_joint_samples_from_artifact(
            artifact_uri,
            pool=pool,
            expected_n_simulations=expected_n_simulations,
        )
        if source is not None:
            return source
    return _synthesize_joint_samples_from_summaries(
        pool,
        n_simulations=_SUMMARY_FALLBACK_SIMULATIONS,
        snapshot_id=snapshot_id,
    )


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


def _resolve_score_method(
    objective_metric: str, suffix: str
) -> str:
    """``joint_p10`` + ``""`` -> ``joint_p10``; + ``"_fallback"`` -> ``summary_fallback``."""
    if suffix == "_fallback":
        return SUMMARY_FALLBACK_METHOD
    return objective_metric


def _insert_lineup_on_cursor(
    cursor: DbCursor,
    *,
    dfs_id: int,
    race_id: int,
    dfs_strategy_id: int,
    pool: list[_PoolEntry],
    score: CandidateScore,
    objective_metric: str,
    score_method: str,
) -> int:
    """Insert one lineup + child rows; widened with the 8 MC fingerprint columns."""
    picked = [pool[i] for i in score.indices]
    projected_total = round(sum(entry.player.projected_points for entry in picked), 2)
    salary_total = sum(entry.player.salary for entry in picked)
    obj_value = objective_value(score, objective_metric)

    cursor.execute(
        """
        INSERT INTO nascar.dfs_race_lineups (
            dfs_id,
            race_id,
            projected_points_total,
            salary_total,
            mc_mean_dfs_points,
            mc_std_dfs_points,
            mc_p10_dfs_points,
            mc_p50_dfs_points,
            mc_p90_dfs_points,
            mc_sharpe,
            objective_value,
            mc_score_method
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            dfs_id,
            race_id,
            projected_total,
            salary_total,
            round(score.mean, 3),
            round(score.std, 3),
            round(score.p10, 3),
            round(score.p50, 3),
            round(score.p90, 3),
            round(score.sharpe, 4),
            round(obj_value, 3),
            score_method,
        ),
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
            entry.player.car_driver_id,
            entry.player.dfs_driver_id,
            round(entry.player.projected_points, 2),
            entry.player.salary,
        )
        for entry in picked
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
    pool: list[_PoolEntry],
    selected: list[CandidateScore],
    objective_metric: str,
    score_method: str,
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
        for score in selected:
            _insert_lineup_on_cursor(
                cursor,
                dfs_id=dfs_id,
                race_id=race_id,
                dfs_strategy_id=dfs_strategy_id,
                pool=pool,
                score=score,
                objective_metric=objective_metric,
                score_method=score_method,
            )

    hook.run_in_transaction(_write)
    return len(selected)


def generate_draftkings_lineups_task(**_context: object) -> dict[str, Any]:
    """Build portfolio-aware top-K lineups per strategy with joint MC scoring."""
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
    mc_batch_id, mc_n_simulations, mc_artifact_uri = mc_batch

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
    pool = _load_player_pool(
        hook,
        mc_batch_id=mc_batch_id,
        week_id=week_id,
        objective_column=objective_column,
    )
    if len(pool) < dk_lineup_size():
        _LOG.warning(
            "generate_draftkings_lineups: skipped reason=insufficient_player_pool "
            "pool_size=%s required=%s week_id=%s",
            len(pool),
            dk_lineup_size(),
            week_id,
        )
        return {
            "skipped": True,
            "reason": "insufficient_player_pool",
            "pool_size": len(pool),
            "required": dk_lineup_size(),
            "week_id": week_id,
        }

    joint_source = _resolve_joint_samples(
        pool,
        artifact_uri=mc_artifact_uri,
        expected_n_simulations=mc_n_simulations,
        snapshot_id=int(snapshot["id"]),
    )

    lineup_size = dk_lineup_size()
    salary_cap = dk_salary_cap()
    top_k = dk_top_k()
    pool_multiplier = max(1, dk_candidate_pool_multiplier())
    strategy_configs = _strategy_configs()
    players = [entry.player for entry in pool]

    inserted_by_strategy: dict[str, int] = {}
    method_by_strategy: dict[str, str] = {}

    for strategy_name, strategy_cfg in strategy_configs.items():
        candidates = generate_candidate_pool(
            players,
            pool_size=top_k * pool_multiplier,
            lineup_size=lineup_size,
            salary_cap=salary_cap,
            min_diff=strategy_cfg.min_diff,
        )
        scored = score_candidates_against_joint_samples(
            candidates, joint_source.matrix
        )
        selected = greedy_diversify(
            scored,
            target_count=top_k,
            exposure_cap=strategy_cfg.exposure_cap,
            objective_metric=strategy_cfg.objective_metric,
        )
        score_method = _resolve_score_method(
            strategy_cfg.objective_metric, joint_source.score_method_suffix
        )
        count = _persist_strategy_lineups(
            hook,
            dfs_id=DRAFTKINGS_DFS_ID,
            race_id=race_id,
            strategy=strategy_name,
            pool=pool,
            selected=selected,
            objective_metric=strategy_cfg.objective_metric,
            score_method=score_method,
        )
        inserted_by_strategy[strategy_name] = count
        method_by_strategy[strategy_name] = score_method
        _LOG.info(
            "generate_draftkings_lineups: race_id=%s strategy=%s lineups=%s "
            "min_diff=%s exposure_cap=%.2f objective=%s score_method=%s "
            "candidate_pool=%s",
            race_id,
            strategy_name,
            count,
            strategy_cfg.min_diff,
            strategy_cfg.exposure_cap,
            strategy_cfg.objective_metric,
            score_method,
            len(candidates),
        )

    total_inserted = sum(inserted_by_strategy.values())
    if total_inserted == 0:
        _LOG.warning(
            "generate_draftkings_lineups: skipped reason=no_feasible_lineups "
            "race_id=%s pool_size=%s salary_cap=%s",
            race_id,
            len(pool),
            salary_cap,
        )
        return {
            "skipped": True,
            "reason": "no_feasible_lineups",
            "snapshot_id": snapshot["id"],
            "race_id": race_id,
            "week_id": week_id,
            "pool_size": len(pool),
            "lineup_size": lineup_size,
            "salary_cap": salary_cap,
            "top_k": top_k,
            "mc_batch_id": mc_batch_id,
            "mc_n_simulations": mc_n_simulations,
            "mc_objective": objective,
            "mc_artifact_uri": mc_artifact_uri,
            "score_methods": method_by_strategy,
            "lineups_inserted": inserted_by_strategy,
        }

    return {
        "skipped": False,
        "snapshot_id": snapshot["id"],
        "race_id": race_id,
        "week_id": week_id,
        "pool_size": len(pool),
        "lineup_size": lineup_size,
        "salary_cap": salary_cap,
        "top_k": top_k,
        "mc_batch_id": mc_batch_id,
        "mc_n_simulations": mc_n_simulations,
        "mc_objective": objective,
        "mc_artifact_uri": mc_artifact_uri,
        "score_methods": method_by_strategy,
        "lineups_inserted": inserted_by_strategy,
    }
