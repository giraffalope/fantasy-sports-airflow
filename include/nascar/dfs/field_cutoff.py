"""Synthetic contest-field cutoff estimation from joint Monte Carlo samples.

Uses random salary-feasible lineups scored on the same ``joint_dfs_points``
matrix as candidate lineups so rank-coupling is preserved. Does not invoke
the Monte Carlo simulator.
"""

from __future__ import annotations

import os
from typing import Sequence

import numpy as np

from include.nascar.dfs.lineup_feasible import (
    feasible_lineup_indices,
    random_feasible_lineup_indices,
)

_DEFAULT_FIELD_SAMPLES = 500
_DEFAULT_CASH_PERCENTILE = 0.55
_DEFAULT_LINEUP_SIZE = 6
_DEFAULT_SALARY_CAP = 50_000


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def field_sample_count() -> int:
    return _env_int("FS_DK_FIELD_SAMPLE_COUNT", _DEFAULT_FIELD_SAMPLES)


def _lineup_size() -> int:
    return _env_int("FS_DK_LINEUP_SIZE", _DEFAULT_LINEUP_SIZE)


def _salary_cap() -> int:
    return _env_int("FS_DK_SALARY_CAP", _DEFAULT_SALARY_CAP)


def estimate_field_cutoffs(
    joint_samples: np.ndarray,
    salaries: Sequence[int],
    *,
    lineup_size: int | None = None,
    salary_cap: int | None = None,
    n_field_samples: int | None = None,
    cash_percentile: float = _DEFAULT_CASH_PERCENTILE,
    rng: np.random.Generator | None = None,
) -> np.ndarray:
    """Per-simulation field cash cutoffs from random feasible opponent lineups.

    For each Monte Carlo draw ``s``, samples ``M`` random 6-driver lineups
    that respect the salary cap, sums their joint DFS points in draw ``s``,
    and sets::

        cutoff[s] = percentile(field_totals[:, s], cash_percentile * 100)

    A lineup cashes in draw ``s`` when ``lineup_total[s] >= cutoff[s]``.

    ``cash_percentile`` is the fraction of the synthetic field that scores
    *below* the cutoff (e.g. ``0.55`` ⇒ top 45% of field scores cash).

    Parameters
    ----------
    joint_samples:
        ``(n_drivers, n_simulations)`` matrix aligned with ``salaries``.
    salaries:
        Per-driver DraftKings salary (same row order as ``joint_samples``).
    """
    if joint_samples.ndim != 2:
        raise ValueError(
            f"joint_samples must be 2D; got shape {joint_samples.shape}"
        )
    n_drivers, n_simulations = joint_samples.shape
    if n_drivers == 0 or n_simulations == 0:
        return np.array([], dtype=np.float64)

    salary_arr = np.asarray(salaries, dtype=np.int64)
    if salary_arr.shape[0] != n_drivers:
        raise ValueError(
            f"salaries length {salary_arr.shape[0]} != n_drivers {n_drivers}"
        )

    size = lineup_size if lineup_size is not None else _lineup_size()
    cap = salary_cap if salary_cap is not None else _salary_cap()
    m = n_field_samples if n_field_samples is not None else field_sample_count()
    pct = float(np.clip(cash_percentile, 0.0, 1.0))
    gen = rng if rng is not None else np.random.default_rng()

    feasible_cache: list[tuple[int, ...]] | None = None
    if n_drivers <= 18:
        feasible_cache = list(
            feasible_lineup_indices(
                n_drivers,
                salary_arr,
                lineup_size=size,
                salary_cap=cap,
            )
        )

    field_lineups: list[tuple[int, ...]] = []
    seen: set[tuple[int, ...]] = set()
    attempts = 0
    max_attempts = max(m * 20, m + 10)
    while len(field_lineups) < m and attempts < max_attempts:
        attempts += 1
        indices = random_feasible_lineup_indices(
            n_drivers,
            salary_arr,
            lineup_size=size,
            salary_cap=cap,
            rng=gen,
            feasible_cache=feasible_cache,
        )
        if indices is None:
            break
        if indices in seen:
            continue
        seen.add(indices)
        field_lineups.append(indices)

    if not field_lineups:
        # Degenerate pool: fall back to per-draw median of all driver sums / 6.
        field_totals = np.mean(joint_samples, axis=0) * size
        return np.asarray(field_totals, dtype=np.float64)

    stacked = np.stack(
        [np.sum(joint_samples[list(indices), :], axis=0) for indices in field_lineups],
        axis=0,
    )
    return np.percentile(stacked, pct * 100.0, axis=0).astype(np.float64)
