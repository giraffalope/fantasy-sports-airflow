"""Salary-feasible lineup variants via bounded driver swaps from high-scoring seeds."""

from __future__ import annotations

import os
from itertools import combinations
from typing import Sequence

import numpy as np

from include.nascar.dfs.lineup_optimizer import dk_lineup_size, dk_salary_cap, dk_top_k


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def dk_variant_seed_count() -> int:
    """How many top joint-scored lineups seed 1..N driver swaps (0 disables)."""
    return _env_int("FS_DK_VARIANT_SEED_COUNT", 25)


def dk_variant_max_swaps() -> int:
    """Max drivers replaced per seed (2 supports write ``min_driver_diff=3``)."""
    return max(1, _env_int("FS_DK_VARIANT_MAX_SWAPS", 2))


def dk_variant_pool_cap() -> int:
    """Hard cap on variant lineups generated per race."""
    return _env_int("FS_DK_VARIANT_POOL_CAP", max(dk_top_k() * 5, 120))


def generate_lineup_variants(
    salaries: np.ndarray,
    seed_lineups: Sequence[Sequence[int]],
    *,
    lineup_size: int | None = None,
    salary_cap: int | None = None,
    max_variants: int | None = None,
    max_swaps: int | None = None,
) -> list[list[int]]:
    """Return salary-feasible variants by swapping 1..``max_swaps`` drivers per seed.

    Single-swap variants differ from the seed in two roster spots (one out,
    one in), i.e. ``min_driver_diff=2`` vs the seed. Two-swap variants support
    ``min_driver_diff=3`` without leaving the high-projection neighborhood.
    """
    if max_variants is not None and max_variants <= 0:
        return []

    n = int(salaries.shape[0])
    size = lineup_size if lineup_size is not None else dk_lineup_size()
    cap = salary_cap if salary_cap is not None else dk_salary_cap()
    swap_cap = max_swaps if max_swaps is not None else dk_variant_max_swaps()
    limit = max_variants if max_variants is not None else dk_variant_pool_cap()

    if size > n or not seed_lineups:
        return []

    seen: set[tuple[int, ...]] = set()
    variants: list[list[int]] = []

    for seed in seed_lineups:
        base = tuple(sorted(int(i) for i in seed))
        if len(base) != size:
            continue
        if base in seen:
            continue
        seen.add(base)

        outside = [i for i in range(n) if i not in base]
        if len(outside) < swap_cap:
            continue

        base_salary = int(np.sum(salaries[list(base)]))
        for swap_count in range(1, min(swap_cap, size) + 1):
            for drop in combinations(base, swap_count):
                drop_salary = int(np.sum(salaries[list(drop)]))
                slack = cap - base_salary + drop_salary
                remain = [i for i in base if i not in drop]
                for add in combinations(outside, swap_count):
                    if int(np.sum(salaries[list(add)])) > slack:
                        continue
                    new_lineup = tuple(sorted(remain + list(add)))
                    if len(new_lineup) != size or new_lineup in seen:
                        continue
                    seen.add(new_lineup)
                    variants.append(list(new_lineup))
                    if len(variants) >= limit:
                        return variants

    return variants
