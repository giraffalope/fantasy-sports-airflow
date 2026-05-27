"""Top-K DraftKings lineup generation via OR-Tools CP-SAT."""

from __future__ import annotations

import os
from dataclasses import dataclass

from ortools.sat.python import cp_model

POINTS_SCALE = 100


@dataclass(frozen=True)
class LineupPlayer:
    car_driver_id: int
    salary: int
    projected_points: float
    dfs_driver_id: int | None = None


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def dk_lineup_size() -> int:
    return _env_int("FS_DK_LINEUP_SIZE", 6)


def dk_salary_cap() -> int:
    return _env_int("FS_DK_SALARY_CAP", 50_000)


def dk_top_k() -> int:
    return _env_int("FS_DK_TOP_K", 20)


def dk_min_diff_double_up() -> int:
    return _env_int("FS_DK_MIN_DIFF_DOUBLE_UP", 2)


def dk_min_diff_high_risk_reward() -> int:
    return _env_int("FS_DK_MIN_DIFF_HIGH_RISK_REWARD", 1)


def dk_candidate_pool_multiplier() -> int:
    """How many CP-SAT solves per strategy as multiplier of ``top_k``.

    Default ``5`` so the default ``FS_DK_TOP_K=20`` yields a 100-lineup
    candidate pool — large enough that the downstream
    :func:`include.nascar.dfs.lineup_portfolio.greedy_diversify` selector
    has real choice when applying per-driver exposure caps, but small
    enough that CP-SAT's wall time stays sub-second on the production
    cohort sizes (~30-40 drivers in the active predict pool).
    """
    return _env_int("FS_DK_CANDIDATE_POOL_MULTIPLIER", 5)


def generate_candidate_pool(
    players: list[LineupPlayer],
    *,
    pool_size: int,
    lineup_size: int | None = None,
    salary_cap: int | None = None,
    min_diff: int = 1,
) -> list[list[int]]:
    """Return up to ``pool_size`` salary-cap-respecting lineups via CP-SAT.

    The optimizer maximizes ``sum(projected_points)`` over the salary cap
    in each iteration and adds a forward-looking ``min_diff`` overlap
    blocker against prior solves. The default ``min_diff=1`` produces the
    widest pool (any two lineups must differ in at least one slot) so the
    portfolio selector — which enforces driver-level exposure caps and a
    strategy-specific objective on top — has maximum coverage to choose
    from. Strategies that want a stronger CP-SAT-level diversity prior
    (e.g. DOUBLE_UP at ``min_diff=2``) can override.

    The pool is ordered by CP-SAT's objective signal (descending
    projected points), but ordering is not contractual: the portfolio
    layer reorders by its own strategy-specific objective.
    """
    if not players:
        return []

    n = len(players)
    size = lineup_size if lineup_size is not None else dk_lineup_size()
    cap = salary_cap if salary_cap is not None else dk_salary_cap()
    if size > n or pool_size <= 0:
        return []

    scaled_points = [int(round(p.projected_points * POINTS_SCALE)) for p in players]
    salaries = [int(p.salary) for p in players]

    lineups: list[list[int]] = []
    for _ in range(pool_size):
        model = cp_model.CpModel()
        picks = [model.new_bool_var(f"pick_{i}") for i in range(n)]
        model.add(sum(picks) == size)
        model.add(sum(salaries[i] * picks[i] for i in range(n)) <= cap)
        model.maximize(sum(scaled_points[i] * picks[i] for i in range(n)))

        for prior in lineups:
            overlap = sum(picks[i] for i in prior)
            model.add(overlap <= size - min_diff)

        solver = cp_model.CpSolver()
        status = solver.solve(model)
        if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            break

        chosen = [i for i in range(n) if solver.value(picks[i])]
        if len(chosen) != size:
            break
        lineups.append(chosen)

    return lineups


def generate_top_k_lineups(
    players: list[LineupPlayer],
    *,
    lineup_size: int | None = None,
    salary_cap: int | None = None,
    top_k: int | None = None,
    min_diff: int,
) -> list[list[int]]:
    """Return up to ``top_k`` lineups as lists of indices into ``players``.

    Thin wrapper around :func:`generate_candidate_pool` retained for
    backwards compatibility with the existing test suite and any external
    callers that still want a CP-SAT-only top-K with the legacy ``min_diff``
    semantics. New code paths should call :func:`generate_candidate_pool`
    directly and feed the result through the portfolio scorer.
    """
    k = top_k if top_k is not None else dk_top_k()
    return generate_candidate_pool(
        players,
        pool_size=k,
        lineup_size=lineup_size,
        salary_cap=salary_cap,
        min_diff=min_diff,
    )
