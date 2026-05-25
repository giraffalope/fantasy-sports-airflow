"""Parametric residual Monte Carlo simulator for upcoming-week DFS-point outcomes.

This module is intentionally free of Airflow / MySQL imports so the sim core can
be exercised end-to-end with synthetic inputs in :mod:`tests.unit.test_monte_carlo`.
The Airflow task in :mod:`include.nascar.ml.monte_carlo_task` wraps this with
database IO (loading predictions and per-target sigmas, persisting batch +
summaries) and is the only caller in production.

Inputs (per driver) come from ``ml_predictions.predicted_targets_json`` plus a
per-target sigma vector derived from the latest ``ml_model_evaluations`` row.
For each simulation draw the targets are perturbed independently with
``Normal(0, sigma)`` and re-scored via ``compute_dfs_points_from_targets`` so
the simulated DFS distribution reflects scoring-rule non-linearities (the
finish-position table dominates) rather than a single dispersion around the
point estimate.

Constraints applied per draw:

* ``r_finish_position`` is clipped to ``[1, field_size]`` and rounded to int.
* ``r_total_laps_led`` and ``r_total_fastest_laps`` are clipped at ``0`` and
  rounded to int.

Drivers are treated as independent conditional on features (v1). The plan
documents this as a known limitation; rank coupling (e.g. Gumbel-max on finish
logits) would land in a later iteration.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np

from include.nascar.ml.scoring import compute_dfs_points_from_targets

MIN_SIMULATIONS = 1000

FINISH_POSITION_TARGET = "r_finish_position"
NON_NEGATIVE_INT_TARGETS: frozenset[str] = frozenset(
    {"r_total_laps_led", "r_total_fastest_laps"}
)


@dataclass(frozen=True)
class MonteCarloDriverInput:
    """One driver's predicted targets and the σ for each target's perturbation.

    ``predicted_targets`` mirrors ``ml_predictions.predicted_targets_json``; any
    target absent from ``target_sigmas`` is held at its predicted value (σ = 0)
    so missing model metrics degrade gracefully instead of skipping the driver.
    """

    car_driver_id: int
    r_start_position: float | None
    predicted_targets: Mapping[str, float]
    target_sigmas: Mapping[str, float]


@dataclass(frozen=True)
class MonteCarloDriverSummary:
    """Aggregate DFS-point statistics for one driver across ``n_simulations`` draws."""

    car_driver_id: int
    n_simulations: int
    mean_dfs_points: float
    std_dfs_points: float
    min_dfs_points: float
    p05_dfs_points: float
    p50_dfs_points: float
    p95_dfs_points: float
    max_dfs_points: float


def _apply_constraints(target: str, value: float, *, field_size: int) -> float:
    """Project a perturbed target value back into its physically valid range."""
    if target == FINISH_POSITION_TARGET:
        rounded = int(round(value))
        if rounded < 1:
            rounded = 1
        if rounded > field_size:
            rounded = field_size
        return float(rounded)
    if target in NON_NEGATIVE_INT_TARGETS:
        rounded = int(round(value))
        if rounded < 0:
            rounded = 0
        return float(rounded)
    return float(value)


def _simulate_driver_dfs_points(
    driver: MonteCarloDriverInput,
    *,
    scoring_rules: Mapping[str, Any],
    n_simulations: int,
    field_size: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """Return an ``(n_simulations,)`` array of simulated DFS points for one driver."""
    samples = np.empty(n_simulations, dtype=float)
    targets = dict(driver.predicted_targets)
    for s in range(n_simulations):
        perturbed: dict[str, float] = {}
        for target, mean in targets.items():
            sigma = float(driver.target_sigmas.get(target, 0.0) or 0.0)
            raw = float(mean) + (rng.normal(0.0, sigma) if sigma > 0.0 else 0.0)
            perturbed[target] = _apply_constraints(target, raw, field_size=field_size)
        total, _breakdown = compute_dfs_points_from_targets(
            driver.r_start_position,
            perturbed,
            scoring_rules,
        )
        samples[s] = float(total)
    return samples


def _summarize(car_driver_id: int, samples: np.ndarray) -> MonteCarloDriverSummary:
    n = int(samples.shape[0])
    percentiles = np.percentile(samples, [5, 50, 95])
    return MonteCarloDriverSummary(
        car_driver_id=car_driver_id,
        n_simulations=n,
        mean_dfs_points=float(np.mean(samples)),
        std_dfs_points=float(np.std(samples, ddof=0)),
        min_dfs_points=float(np.min(samples)),
        p05_dfs_points=float(percentiles[0]),
        p50_dfs_points=float(percentiles[1]),
        p95_dfs_points=float(percentiles[2]),
        max_dfs_points=float(np.max(samples)),
    )


def simulate_driver_distributions(
    drivers: list[MonteCarloDriverInput],
    *,
    scoring_rules: Mapping[str, Any],
    n_simulations: int,
    field_size: int | None = None,
    seed: int,
) -> list[MonteCarloDriverSummary]:
    """Run ``n_simulations`` draws per driver and aggregate DFS-point statistics.

    ``field_size`` defaults to ``len(drivers)`` (the active predict pool size,
    same field the lineup generator sees). ``seed`` constructs the
    ``numpy.random.Generator`` so each batch is reproducible.

    Raises ``ValueError`` for ``n_simulations < MIN_SIMULATIONS``; the validate
    task also asserts this so the constraint surfaces twice.
    """
    if n_simulations < MIN_SIMULATIONS:
        raise ValueError(
            f"n_simulations must be >= {MIN_SIMULATIONS} (got {n_simulations})"
        )
    if not drivers:
        return []
    resolved_field_size = field_size if field_size is not None else len(drivers)
    if resolved_field_size < 1:
        raise ValueError(f"field_size must be >= 1 (got {resolved_field_size})")

    rng = np.random.default_rng(seed)
    summaries: list[MonteCarloDriverSummary] = []
    for driver in drivers:
        samples = _simulate_driver_dfs_points(
            driver,
            scoring_rules=scoring_rules,
            n_simulations=n_simulations,
            field_size=resolved_field_size,
            rng=rng,
        )
        summaries.append(_summarize(driver.car_driver_id, samples))
    return summaries
