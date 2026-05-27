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

E2 rank-coupled Monte Carlo: the simulator argsorts each iteration's raw
finish-position draws across the entire field so the per-iteration
``r_finish_position`` assignment is always a permutation of
``1..field_size``. Two drivers can no longer share the same finish slot in
a draw, which matches NASCAR rules and de-biases the finish-table component
of the DFS score (which is the dominant term).

Constraints applied per draw:

* ``r_finish_position`` raw samples are jointly argsort'd across drivers,
  then assigned integer ranks ``1..field_size`` (lowest raw sample =
  predicted winner = rank 1).
* ``r_total_laps_led`` and ``r_total_fastest_laps`` are clipped at ``0`` and
  rounded to int **independently per driver** because counts can co-vary
  freely.

Per-driver σ supplied on :class:`MonteCarloDriverInput.target_sigmas` is the
recommended path so the Phase E1 conformal intervals can size each driver's
spread independently. Callers without conformal store (cold start) can keep
broadcasting a single σ across all drivers; the math degrades gracefully.
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


@dataclass(frozen=True)
class MonteCarloBatchResult:
    """Aggregated per-driver summaries plus the joint per-draw matrix.

    The joint matrix is what makes downstream **lineup-level** scoring honest
    about driver-driver correlation. Phase E2 rank-coupling means a given
    iteration's finish-position vector is a permutation across the field, so
    two drivers' DFS-point samples are negatively correlated. Aggregating
    each driver to a scalar (mean / std / pXX) before summing into a lineup
    discards that correlation; carrying the joint matrix to the lineup task
    preserves it.

    ``joint_dfs_points`` shape: ``(n_drivers, n_simulations)``. Row ``i``
    corresponds to ``driver_index[i]`` (a ``car_driver_id``); column ``s`` is
    the s-th simulation draw of the cohort. Same iteration across rows is a
    coupled draw — that's the whole point.
    """

    summaries: list[MonteCarloDriverSummary]
    joint_dfs_points: np.ndarray
    driver_index: np.ndarray  # int64, shape (n_drivers,), aligned with summaries


def _apply_count_constraint(value: float) -> float:
    """Round a non-negative count target to int with a 0 floor."""
    rounded = int(round(value))
    if rounded < 0:
        rounded = 0
    return float(rounded)


def _apply_constraints(target: str, value: float, *, field_size: int) -> float:
    """Project a perturbed target value back into its physically valid range.

    Kept for backwards compatibility with helpers / tests that expect the
    legacy per-target clip semantics. The E2 rank-coupled simulator path
    replaces the ``r_finish_position`` branch with a cross-driver argsort
    inside :func:`simulate_driver_distributions`, so this function is no
    longer called for finishes in the production loop — it lives here as
    a labeled helper for tooling that operates on a single value
    (lineage replay, DFS scoring smoke tests).
    """
    if target == FINISH_POSITION_TARGET:
        rounded = int(round(value))
        if rounded < 1:
            rounded = 1
        if rounded > field_size:
            rounded = field_size
        return float(rounded)
    if target in NON_NEGATIVE_INT_TARGETS:
        return _apply_count_constraint(value)
    return float(value)


def _reconcile_finish_ranks(raw_finishes: np.ndarray) -> np.ndarray:
    """E2: argsort raw finish-position samples and reassign integer ranks.

    Returns an integer array of shape ``(n_drivers,)`` whose ``i``-th entry
    is the predicted rank ``1..n_drivers`` for the ``i``-th driver in this
    iteration. Lower raw sample => predicted-better finish => lower rank
    (NASCAR convention). Stable argsort breaks ties by driver index, which
    keeps the math deterministic for a given seed.
    """
    n = raw_finishes.size
    if n == 0:
        return np.zeros(0, dtype=int)
    order = np.argsort(raw_finishes, kind="stable")
    ranks = np.empty(n, dtype=int)
    ranks[order] = np.arange(1, n + 1, dtype=int)
    return ranks


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
) -> MonteCarloBatchResult:
    """Run ``n_simulations`` draws per driver, returning summaries + joint matrix.

    ``field_size`` defaults to ``len(drivers)`` (the active predict pool size,
    same field the lineup generator sees). ``seed`` constructs the
    ``numpy.random.Generator`` so each batch is reproducible.

    E2 rank-coupling: each iteration draws all drivers' raw finish positions
    in parallel, argsorts them within the iteration, and assigns integer
    ranks ``1..field_size`` so no two drivers share a finish. Secondary
    targets (``r_total_laps_led``, ``r_total_fastest_laps``) are still drawn
    independently per driver because counts can co-vary freely.

    Returns a :class:`MonteCarloBatchResult` carrying both the per-driver
    summaries (for the existing ``ml_race_simulation_driver_summaries``
    persistence) and the joint ``(n_drivers, n_simulations)`` DFS-point
    matrix (so the lineup task can score candidate lineups against the
    true coupled distribution rather than a sum-of-independent-scalars
    approximation). Earlier callers received only the summaries list;
    those callers should unpack ``.summaries`` explicitly.

    Raises ``ValueError`` for ``n_simulations < MIN_SIMULATIONS``; the validate
    task also asserts this so the constraint surfaces twice.
    """
    if n_simulations < MIN_SIMULATIONS:
        raise ValueError(
            f"n_simulations must be >= {MIN_SIMULATIONS} (got {n_simulations})"
        )
    if not drivers:
        return MonteCarloBatchResult(
            summaries=[],
            joint_dfs_points=np.zeros((0, 0), dtype=float),
            driver_index=np.zeros(0, dtype=np.int64),
        )
    resolved_field_size = field_size if field_size is not None else len(drivers)
    if resolved_field_size < 1:
        raise ValueError(f"field_size must be >= 1 (got {resolved_field_size})")

    rng = np.random.default_rng(seed)
    n_drivers = len(drivers)

    # Pre-extract per-driver vectors for vectorized sampling. ``finish_mean``
    # and ``finish_sigma`` are the only target-coupled across drivers; the
    # rest are sampled independently below.
    finish_mean = np.zeros(n_drivers, dtype=float)
    finish_sigma = np.zeros(n_drivers, dtype=float)
    for i, driver in enumerate(drivers):
        finish_mean[i] = float(
            driver.predicted_targets.get(FINISH_POSITION_TARGET, 0.0) or 0.0
        )
        finish_sigma[i] = float(
            driver.target_sigmas.get(FINISH_POSITION_TARGET, 0.0) or 0.0
        )

    secondary_target_specs: list[tuple[str, np.ndarray, np.ndarray]] = []
    secondary_target_names = set()
    for driver in drivers:
        secondary_target_names.update(driver.predicted_targets.keys())
    secondary_target_names.discard(FINISH_POSITION_TARGET)
    for target in sorted(secondary_target_names):
        means = np.array(
            [
                float(driver.predicted_targets.get(target, 0.0) or 0.0)
                for driver in drivers
            ],
            dtype=float,
        )
        sigmas = np.array(
            [
                float(driver.target_sigmas.get(target, 0.0) or 0.0)
                for driver in drivers
            ],
            dtype=float,
        )
        secondary_target_specs.append((target, means, sigmas))

    samples_per_driver = np.zeros((n_drivers, n_simulations), dtype=float)
    start_positions = [driver.r_start_position for driver in drivers]

    for s in range(n_simulations):
        # Vectorized draws across the entire field for the finish target.
        # ``rng.normal`` sized at the whole field at once keeps the seed
        # progression identical to ``np.random.default_rng`` semantics
        # (single advance per iteration).
        finish_noise = rng.normal(0.0, 1.0, size=n_drivers) * finish_sigma
        raw_finishes = finish_mean + finish_noise
        # E2: cross-driver argsort -> integer permutation ranks.
        ranks = _reconcile_finish_ranks(raw_finishes)

        # Cap secondary draws here once per iteration so the per-driver
        # rng draws below stay consistent and deterministic.
        per_target_iter_samples: list[tuple[str, np.ndarray]] = []
        for target, means, sigmas in secondary_target_specs:
            noise = rng.normal(0.0, 1.0, size=n_drivers) * sigmas
            raw = means + noise
            per_target_iter_samples.append((target, raw))

        for i in range(n_drivers):
            perturbed: dict[str, float] = {FINISH_POSITION_TARGET: float(ranks[i])}
            for target, raw_vec in per_target_iter_samples:
                value = float(raw_vec[i])
                if target in NON_NEGATIVE_INT_TARGETS:
                    perturbed[target] = _apply_count_constraint(value)
                else:
                    perturbed[target] = value
            total, _breakdown = compute_dfs_points_from_targets(
                start_positions[i],
                perturbed,
                scoring_rules,
            )
            samples_per_driver[i, s] = float(total)

    summaries: list[MonteCarloDriverSummary] = []
    driver_index = np.zeros(n_drivers, dtype=np.int64)
    for i, driver in enumerate(drivers):
        summaries.append(_summarize(driver.car_driver_id, samples_per_driver[i]))
        driver_index[i] = int(driver.car_driver_id)
    return MonteCarloBatchResult(
        summaries=summaries,
        joint_dfs_points=samples_per_driver,
        driver_index=driver_index,
    )
