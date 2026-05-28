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
# Production default. Kept distinct from ``MIN_SIMULATIONS`` so the floor
# (used by :func:`run_monte_carlo_for_drivers`'s validate path and as a
# fast scaffold in the unit suite) stays at 1000, while operators who do
# not override ``FS_MC_N_SIMULATIONS`` get the higher-fidelity default
# the joint MC artifact + lineup portfolio selector were tuned against.
DEFAULT_SIMULATIONS = 10000

FINISH_POSITION_TARGET = "r_finish_position"
LAPS_LED_TARGET = "r_total_laps_led"
FASTEST_LAPS_TARGET = "r_total_fastest_laps"
NON_NEGATIVE_INT_TARGETS: frozenset[str] = frozenset(
    {LAPS_LED_TARGET, FASTEST_LAPS_TARGET}
)


@dataclass(frozen=True)
class MonteCarloDriverInput:
    """One driver's predicted targets and the σ for each target's perturbation.

    ``predicted_targets`` mirrors ``ml_predictions.predicted_targets_json``; any
    target absent from ``target_sigmas`` is held at its predicted value (σ = 0)
    so missing model metrics degrade gracefully instead of skipping the driver.

    ``p_dnf`` is the per-driver DNF probability blended from the two-stage
    classifier upstream (``predict_task.DNF_PROBABILITY_KEY``). When ``None``
    the simulator treats it as ``0.0`` and the DNF Bernoulli draw never fires —
    behavior is identical to pre-DNF callers (and to snapshots whose two-stage
    classifier was unavailable).
    """

    car_driver_id: int
    r_start_position: float | None
    predicted_targets: Mapping[str, float]
    target_sigmas: Mapping[str, float]
    p_dnf: float | None = None


@dataclass(frozen=True)
class MonteCarloDriverSummary:
    """Aggregate distribution stats for one driver across ``n_simulations`` draws.

    The DFS-points block (``*_dfs_points``) is the original primary signal.
    The three target blocks (``*_r_finish_position``, ``*_r_total_fastest_laps``,
    ``*_r_total_laps_led``) summarize the underlying sampled targets the DFS
    scorer ingested per draw. ``dnf_pct`` is the empirical Bernoulli rate of
    the per-iteration DNF draw (zero when ``p_dnf`` was not supplied).

    All per-target fields default to ``0.0`` for backward compat with helpers
    that construct stub summaries (see :mod:`tests.unit.test_monte_carlo_task`).
    """

    car_driver_id: int
    n_simulations: int
    mean_dfs_points: float
    std_dfs_points: float
    min_dfs_points: float
    p05_dfs_points: float
    p50_dfs_points: float
    p95_dfs_points: float
    max_dfs_points: float
    mean_r_finish_position: float = 0.0
    std_r_finish_position: float = 0.0
    min_r_finish_position: float = 0.0
    p05_r_finish_position: float = 0.0
    p50_r_finish_position: float = 0.0
    p95_r_finish_position: float = 0.0
    max_r_finish_position: float = 0.0
    mean_r_total_fastest_laps: float = 0.0
    std_r_total_fastest_laps: float = 0.0
    min_r_total_fastest_laps: float = 0.0
    p05_r_total_fastest_laps: float = 0.0
    p50_r_total_fastest_laps: float = 0.0
    p95_r_total_fastest_laps: float = 0.0
    max_r_total_fastest_laps: float = 0.0
    mean_r_total_laps_led: float = 0.0
    std_r_total_laps_led: float = 0.0
    min_r_total_laps_led: float = 0.0
    p05_r_total_laps_led: float = 0.0
    p50_r_total_laps_led: float = 0.0
    p95_r_total_laps_led: float = 0.0
    max_r_total_laps_led: float = 0.0
    dnf_pct: float = 0.0


@dataclass(frozen=True)
class MonteCarloBatchResult:
    """Aggregated per-driver summaries plus the joint per-draw matrices.

    The joint matrices are what make downstream **lineup-level** scoring honest
    about driver-driver correlation. Phase E2 rank-coupling means a given
    iteration's finish-position vector is a permutation across the field, so
    two drivers' DFS-point samples are negatively correlated. Aggregating
    each driver to a scalar (mean / std / pXX) before summing into a lineup
    discards that correlation; carrying the joint matrix to the lineup task
    preserves it.

    Each joint matrix has shape ``(n_drivers, n_simulations)``. Row ``i``
    corresponds to ``driver_index[i]`` (a ``car_driver_id``); column ``s`` is
    the s-th simulation draw of the cohort. Same iteration across rows is a
    coupled draw — that's the whole point.

    ``joint_is_dnf`` is the per-iteration Bernoulli outcome (0 / 1 int8) for
    each driver. When a driver is DNFed in iteration ``s`` the corresponding
    column of ``joint_finish_position`` records the tail position they were
    permuted into and ``joint_total_fastest_laps`` / ``joint_total_laps_led``
    record the completion-fraction-scaled counts the DFS scorer ingested.
    """

    summaries: list[MonteCarloDriverSummary]
    joint_dfs_points: np.ndarray
    driver_index: np.ndarray  # int64, shape (n_drivers,), aligned with summaries
    joint_finish_position: np.ndarray | None = None
    joint_total_fastest_laps: np.ndarray | None = None
    joint_total_laps_led: np.ndarray | None = None
    joint_is_dnf: np.ndarray | None = None


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


def _summarize_vector(samples: np.ndarray) -> tuple[float, float, float, float, float, float, float]:
    """Compute the 7-stat distribution summary (mean, std, min, p05, p50, p95, max)
    for a 1-D draws vector. Target-agnostic so the same helper feeds the DFS
    block and each per-target block on :class:`MonteCarloDriverSummary`.
    """
    percentiles = np.percentile(samples, [5, 50, 95])
    return (
        float(np.mean(samples)),
        float(np.std(samples, ddof=0)),
        float(np.min(samples)),
        float(percentiles[0]),
        float(percentiles[1]),
        float(percentiles[2]),
        float(np.max(samples)),
    )


def _summarize(
    car_driver_id: int,
    dfs_samples: np.ndarray,
    finish_samples: np.ndarray | None = None,
    fastest_laps_samples: np.ndarray | None = None,
    laps_led_samples: np.ndarray | None = None,
    dnf_samples: np.ndarray | None = None,
) -> MonteCarloDriverSummary:
    """Build a flat :class:`MonteCarloDriverSummary` from per-target draw vectors.

    Only ``dfs_samples`` is required; the per-target vectors default to all
    zeros (and ``dnf_pct = 0``) so the helper is still usable from legacy
    callers and tests that only carry the DFS-points block.
    """
    n = int(dfs_samples.shape[0])
    dfs = _summarize_vector(dfs_samples)
    if finish_samples is not None:
        fin = _summarize_vector(finish_samples)
    else:
        fin = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    if fastest_laps_samples is not None:
        fl = _summarize_vector(fastest_laps_samples)
    else:
        fl = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    if laps_led_samples is not None:
        ll = _summarize_vector(laps_led_samples)
    else:
        ll = (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    if dnf_samples is not None and dnf_samples.size > 0:
        dnf_pct = float(np.mean(dnf_samples))
    else:
        dnf_pct = 0.0
    return MonteCarloDriverSummary(
        car_driver_id=car_driver_id,
        n_simulations=n,
        mean_dfs_points=dfs[0],
        std_dfs_points=dfs[1],
        min_dfs_points=dfs[2],
        p05_dfs_points=dfs[3],
        p50_dfs_points=dfs[4],
        p95_dfs_points=dfs[5],
        max_dfs_points=dfs[6],
        mean_r_finish_position=fin[0],
        std_r_finish_position=fin[1],
        min_r_finish_position=fin[2],
        p05_r_finish_position=fin[3],
        p50_r_finish_position=fin[4],
        p95_r_finish_position=fin[5],
        max_r_finish_position=fin[6],
        mean_r_total_fastest_laps=fl[0],
        std_r_total_fastest_laps=fl[1],
        min_r_total_fastest_laps=fl[2],
        p05_r_total_fastest_laps=fl[3],
        p50_r_total_fastest_laps=fl[4],
        p95_r_total_fastest_laps=fl[5],
        max_r_total_fastest_laps=fl[6],
        mean_r_total_laps_led=ll[0],
        std_r_total_laps_led=ll[1],
        min_r_total_laps_led=ll[2],
        p05_r_total_laps_led=ll[3],
        p50_r_total_laps_led=ll[4],
        p95_r_total_laps_led=ll[5],
        max_r_total_laps_led=ll[6],
        dnf_pct=dnf_pct,
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
        empty_f = np.zeros((0, 0), dtype=float)
        empty_int = np.zeros((0, 0), dtype=np.int8)
        return MonteCarloBatchResult(
            summaries=[],
            joint_dfs_points=empty_f,
            driver_index=np.zeros(0, dtype=np.int64),
            joint_finish_position=empty_f.copy(),
            joint_total_fastest_laps=empty_f.copy(),
            joint_total_laps_led=empty_f.copy(),
            joint_is_dnf=empty_int,
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
    p_dnf_per_driver = np.zeros(n_drivers, dtype=float)
    any_dnf_modeled = False
    for i, driver in enumerate(drivers):
        finish_mean[i] = float(
            driver.predicted_targets.get(FINISH_POSITION_TARGET, 0.0) or 0.0
        )
        finish_sigma[i] = float(
            driver.target_sigmas.get(FINISH_POSITION_TARGET, 0.0) or 0.0
        )
        if driver.p_dnf is not None:
            any_dnf_modeled = True
            p_dnf_per_driver[i] = float(np.clip(driver.p_dnf, 0.0, 1.0))

    secondary_target_specs: list[tuple[str, np.ndarray, np.ndarray]] = []
    secondary_target_names: set[str] = set()
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
    finish_samples = np.zeros((n_drivers, n_simulations), dtype=float)
    fastest_laps_samples = np.zeros((n_drivers, n_simulations), dtype=float)
    laps_led_samples = np.zeros((n_drivers, n_simulations), dtype=float)
    dnf_samples = np.zeros((n_drivers, n_simulations), dtype=np.int8)
    start_positions = [driver.r_start_position for driver in drivers]

    for s in range(n_simulations):
        # 1. DNF Bernoulli draw (always advance the RNG by exactly ``n_drivers``
        # uniforms per iteration, even when no driver has a DNF model attached,
        # so seed sequences stay deterministic across pipeline versions).
        dnf_uniforms = rng.random(n_drivers)
        if any_dnf_modeled:
            is_dnf = (dnf_uniforms < p_dnf_per_driver).astype(np.int8)
        else:
            is_dnf = np.zeros(n_drivers, dtype=np.int8)
        k = int(is_dnf.sum())
        dnf_samples[:, s] = is_dnf

        # 2. Vectorized finish-noise draw across the entire field. Run regardless
        # of ``k`` so the rng advances the same number of times per iteration.
        finish_noise = rng.normal(0.0, 1.0, size=n_drivers) * finish_sigma
        raw_finishes_all = finish_mean + finish_noise

        # Non-DNFed subset is rank-reconciled to dense ranks 1..(n_drivers - k).
        # DNFed subset receives a uniformly random permutation of the tail
        # positions (n_drivers - k + 1) .. n_drivers, so each iteration is still
        # a strict permutation of 1..n_drivers (E2 invariant preserved).
        ranks = np.zeros(n_drivers, dtype=int)
        nondnf_indices = np.flatnonzero(is_dnf == 0)
        if nondnf_indices.size > 0:
            ranks[nondnf_indices] = _reconcile_finish_ranks(
                raw_finishes_all[nondnf_indices]
            )
        if k > 0:
            dnf_indices = np.flatnonzero(is_dnf == 1)
            # ``rng.permutation(k)`` gives a uniform random permutation of
            # ``[0..k-1]``; shifting by ``n_drivers - k + 1`` turns it into a
            # uniform random permutation of the tail positions.
            tail_perm = rng.permutation(k) + (n_drivers - k + 1)
            ranks[dnf_indices] = tail_perm

        # 3. Per-driver completion-fraction draw. Always draw the full-field
        # vector for deterministic seed advancement; the values are only
        # applied to DNFed drivers' secondary targets. The same ``c`` is
        # shared between fastest_laps and laps_led for a given (driver, iter)
        # because "the race ended early at fraction c and both leaderboards
        # froze together."
        completion_fraction = rng.random(n_drivers)
        if k > 0:
            dnf_mask = is_dnf == 1
        else:
            dnf_mask = None

        # 4. Secondary target draws (full field) and, for DNFed drivers,
        # multiply by the shared completion fraction before
        # ``_apply_count_constraint`` rounds + floors at 0.
        per_target_iter_samples: list[tuple[str, np.ndarray]] = []
        for target, means, sigmas in secondary_target_specs:
            noise = rng.normal(0.0, 1.0, size=n_drivers) * sigmas
            raw = means + noise
            if dnf_mask is not None and target in NON_NEGATIVE_INT_TARGETS:
                # Operate on a copy so the unscaled draws stay available for
                # any future audit; ``raw[dnf_mask] *=`` would also be fine
                # (raw is freshly allocated above).
                raw = raw.copy()
                raw[dnf_mask] = raw[dnf_mask] * completion_fraction[dnf_mask]
            per_target_iter_samples.append((target, raw))

        for i in range(n_drivers):
            perturbed: dict[str, float] = {FINISH_POSITION_TARGET: float(ranks[i])}
            for target, raw_vec in per_target_iter_samples:
                value = float(raw_vec[i])
                if target in NON_NEGATIVE_INT_TARGETS:
                    constrained = _apply_count_constraint(value)
                    perturbed[target] = constrained
                    if target == FASTEST_LAPS_TARGET:
                        fastest_laps_samples[i, s] = constrained
                    elif target == LAPS_LED_TARGET:
                        laps_led_samples[i, s] = constrained
                else:
                    perturbed[target] = value
            total, _breakdown = compute_dfs_points_from_targets(
                start_positions[i],
                perturbed,
                scoring_rules,
            )
            samples_per_driver[i, s] = float(total)
            finish_samples[i, s] = float(ranks[i])

    summaries: list[MonteCarloDriverSummary] = []
    driver_index = np.zeros(n_drivers, dtype=np.int64)
    for i, driver in enumerate(drivers):
        summaries.append(
            _summarize(
                driver.car_driver_id,
                samples_per_driver[i],
                finish_samples=finish_samples[i],
                fastest_laps_samples=fastest_laps_samples[i],
                laps_led_samples=laps_led_samples[i],
                dnf_samples=dnf_samples[i],
            )
        )
        driver_index[i] = int(driver.car_driver_id)
    return MonteCarloBatchResult(
        summaries=summaries,
        joint_dfs_points=samples_per_driver,
        driver_index=driver_index,
        joint_finish_position=finish_samples,
        joint_total_fastest_laps=fastest_laps_samples,
        joint_total_laps_led=laps_led_samples,
        joint_is_dnf=dnf_samples,
    )
