"""Salary-feasible lineup sampling helpers shared by optimizer and field cutoff."""

from __future__ import annotations

from itertools import combinations
from typing import Iterator

import numpy as np

_MAX_RANDOM_ATTEMPTS = 2_000


def feasible_lineup_indices(
    n_drivers: int,
    salaries: np.ndarray,
    *,
    lineup_size: int,
    salary_cap: int,
) -> Iterator[tuple[int, ...]]:
    """Yield every salary-feasible lineup (for small driver pools)."""
    for combo in combinations(range(n_drivers), lineup_size):
        if int(np.sum(salaries[list(combo)])) <= salary_cap:
            yield tuple(int(i) for i in combo)


def random_feasible_lineup_indices(
    n_drivers: int,
    salaries: np.ndarray,
    *,
    lineup_size: int,
    salary_cap: int,
    rng: np.random.Generator,
    feasible_cache: list[tuple[int, ...]] | None = None,
) -> tuple[int, ...] | None:
    """Return a random feasible lineup index tuple."""
    if lineup_size > n_drivers:
        return None

    if feasible_cache is not None:
        if not feasible_cache:
            return None
        return feasible_cache[int(rng.integers(0, len(feasible_cache)))]

    # Small pools: enumerate once (C(n,6) stays tractable for n <= 18).
    if n_drivers <= 18:
        options = list(
            feasible_lineup_indices(
                n_drivers,
                salaries,
                lineup_size=lineup_size,
                salary_cap=salary_cap,
            )
        )
        if not options:
            return None
        return options[int(rng.integers(0, len(options)))]

    cheap_k = min(n_drivers, max(lineup_size + 6, 12))
    cheap = np.argsort(salaries)[:cheap_k]
    for _ in range(_MAX_RANDOM_ATTEMPTS):
        chosen = rng.choice(cheap, size=lineup_size, replace=False)
        if int(np.sum(salaries[chosen])) <= salary_cap:
            return tuple(int(i) for i in sorted(chosen))
    return None
