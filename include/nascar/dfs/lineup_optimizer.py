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


def generate_top_k_lineups(
    players: list[LineupPlayer],
    *,
    lineup_size: int | None = None,
    salary_cap: int | None = None,
    top_k: int | None = None,
    min_diff: int,
) -> list[list[int]]:
    """Return up to ``top_k`` lineups as lists of indices into ``players``."""
    if not players:
        return []

    n = len(players)
    size = lineup_size if lineup_size is not None else dk_lineup_size()
    cap = salary_cap if salary_cap is not None else dk_salary_cap()
    k = top_k if top_k is not None else dk_top_k()
    if size > n:
        return []

    scaled_points = [int(round(p.projected_points * POINTS_SCALE)) for p in players]
    salaries = [int(p.salary) for p in players]

    lineups: list[list[int]] = []
    for _ in range(k):
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
