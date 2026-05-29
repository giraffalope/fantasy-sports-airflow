"""Portfolio-aware lineup scoring and greedy diversity selection.

This module is intentionally free of Airflow / MySQL imports so the joint MC
scoring + exposure-capped selection logic can be exercised end-to-end with
synthetic ndarrays in :mod:`tests.unit.test_lineup_portfolio`. The Airflow
task in :mod:`include.nascar.dfs.lineup_generator_task` wraps these helpers
with the parquet/npz IO and DB persistence.

Why a separate module
---------------------

The legacy lineup task collapsed each driver to a single Monte Carlo
percentile column before solving (default ``p50_dfs_points``). That throws
away two pieces of signal that matter for portfolio construction:

1. **Driver-driver correlation under rank coupling.** Phase E2 makes each
   simulated iteration a permutation of finish positions across the field —
   when driver A's finish improves, driver B's must worsen. Summing
   pre-aggregated scalars treats those draws as independent, which
   inflates lineup variance and biases percentile-based ranking. Scoring
   the candidate's six driver rows against the *joint* draw matrix
   recovers the true lineup distribution.

2. **Variance / certainty.** The legacy objective was a single scalar
   (mean or median); there's no way to express "high floor, low
   variance" (cash games) vs "high ceiling" (GPP tournaments) inside
   one column. With joint scoring we get the lineup's full
   distribution, so the selector can pick a strategy-appropriate
   summary stat (e.g. ``p10`` for floor, ``p90`` for ceiling) and
   apply portfolio-level diversification on top.

Public surface
--------------

* :func:`score_candidates_against_joint_samples` — joint-MC scoring.
* :func:`objective_value` — maps a metric string to a scalar from a score.
* :func:`greedy_diversify` — exposure-capped greedy selection.

Submission-time selection (Double Up / High Risk Reward) lives in
:mod:`include.nascar.dfs.submission_portfolio` and
:mod:`include.nascar.dfs.field_cutoff`.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Iterable

import numpy as np

_LOG = logging.getLogger(__name__)

# Std-floor for the sharpe ratio so a degenerate (constant-sample) lineup
# does not blow up to +/- inf. Same epsilon used in the SQL ``mc_sharpe``
# comment so application code and audit semantics stay aligned.
_SHARPE_STD_FLOOR = 1e-6

# Metric tokens accepted by :func:`objective_value` and persisted in
# ``dfs_race_lineups.mc_score_method``. Kept as a frozenset so a typo in an
# env var falls back instead of silently breaking the selector.
JOINT_METRICS: frozenset[str] = frozenset(
    {"joint_p10", "joint_p50", "joint_p90", "joint_mean", "joint_sharpe"}
)

# Summary-fallback marker used when no joint sample artifact is available.
# The lineup task synthesizes independent Gaussians from the summary table
# and stamps this string on the persisted row so downstream audits can
# distinguish true joint scoring from the degraded fallback path.
SUMMARY_FALLBACK_METHOD = "summary_fallback"


@dataclass(frozen=True)
class CandidateScore:
    """Joint-distribution stats for one candidate lineup."""

    indices: tuple[int, ...]  # immutable so scores are hashable / set-able
    mean: float
    std: float
    p10: float
    p50: float
    p90: float
    sharpe: float


def score_candidates_against_joint_samples(
    candidates: Iterable[list[int]],
    joint_samples: np.ndarray,
) -> list[CandidateScore]:
    """Score every candidate against the joint ``(n_drivers, n_simulations)`` matrix.

    For each candidate (a list of row indices into ``joint_samples``):

    1. Sum the candidate's driver rows column-wise into an
       ``(n_simulations,)`` vector of per-iteration lineup totals.
    2. Compute ``mean``, ``std``, ``p10``, ``p50``, ``p90`` across the
       simulation axis.
    3. ``sharpe = mean / max(std, _SHARPE_STD_FLOOR)``.

    ``joint_samples`` must be float (the MC task writes ``float64``) and
    its rows must be aligned with the indices the candidates carry. The
    caller is responsible for reindexing the matrix to the player-pool
    order before invoking this; ``_load_joint_samples`` in
    :mod:`lineup_generator_task` does that alignment.
    """
    if joint_samples.ndim != 2:
        raise ValueError(
            f"joint_samples must be 2D (n_drivers, n_simulations); got shape "
            f"{joint_samples.shape}"
        )
    if joint_samples.size == 0:
        return []

    scores: list[CandidateScore] = []
    for candidate in candidates:
        if not candidate:
            continue
        # Sum driver rows along the n_drivers axis; result shape =
        # (n_simulations,). Using ``np.sum(axis=0)`` is meaningfully faster
        # than a Python ``sum(rows)`` loop for the candidate-pool sizes we
        # care about (~100 candidates x 6 drivers x 1000 samples).
        lineup_totals = np.sum(joint_samples[candidate, :], axis=0)
        # Single ``np.percentile`` call computes p10 / p50 / p90 in one
        # pass; cheaper than three separate calls and matches the audit
        # column triple stored on the lineup row.
        p10, p50, p90 = np.percentile(lineup_totals, [10, 50, 90])
        mean = float(np.mean(lineup_totals))
        std = float(np.std(lineup_totals, ddof=0))
        sharpe = mean / max(std, _SHARPE_STD_FLOOR)
        scores.append(
            CandidateScore(
                indices=tuple(int(i) for i in candidate),
                mean=mean,
                std=std,
                p10=float(p10),
                p50=float(p50),
                p90=float(p90),
                sharpe=float(sharpe),
            )
        )
    return scores


def objective_value(score: CandidateScore, metric: str) -> float:
    """Project a :class:`CandidateScore` onto the scalar the selector ranks on.

    Falls back to ``score.p50`` for unknown metrics so a typo in
    ``FS_DK_DOUBLE_UP_OBJECTIVE`` cannot crash the lineup task — the
    caller is responsible for emitting a warning (see ``_resolve_metric``
    in the lineup task) before reaching this helper.
    """
    if metric == "joint_p10":
        return score.p10
    if metric == "joint_p50":
        return score.p50
    if metric == "joint_p90":
        return score.p90
    if metric == "joint_mean":
        return score.mean
    if metric == "joint_sharpe":
        return score.sharpe
    return score.p50


def _per_driver_budget(target_count: int, exposure_cap: float) -> int:
    """``ceil(target_count * exposure_cap)`` clamped to ``[1, target_count]``.

    Inclusive ceiling so a 0.40 cap with 20 lineups admits up to 8
    appearances per driver, not 7 — matching common DFS portfolio
    semantics where the cap is the maximum *visible* exposure.
    """
    if target_count <= 0:
        return 0
    if exposure_cap <= 0.0:
        return 0
    budget = max(1, math.ceil(target_count * exposure_cap))
    return min(budget, target_count)


def greedy_diversify(
    candidates: list[CandidateScore],
    *,
    target_count: int,
    exposure_cap: float,
    objective_metric: str,
) -> list[CandidateScore]:
    """Greedy exposure-capped selection from a scored candidate pool.

    Algorithm:

    1. Sort the input pool by ``objective_value(score, objective_metric)``
       descending. (We do not assume the pool is pre-sorted because the
       upstream CP-SAT pool is sorted by ``projected_points``, which may
       differ from the strategy-specific objective.)
    2. Walk the sorted pool: accept a candidate iff every driver in the
       lineup still has remaining budget under
       ``ceil(target_count * exposure_cap)``.
    3. Stop when ``target_count`` lineups are selected or the pool is
       exhausted.

    If the cap is too tight to fill the slot count, the cap is relaxed
    in ``+0.05`` increments (up to ``1.0``) and the walk is restarted
    over the still-unaccepted candidates. Each relaxation logs a single
    warning so operators see the cap being widened rather than silently
    receiving a short portfolio. The final selection is returned in the
    original objective-descending order so the highest-objective
    candidate is index ``0`` of the returned list.

    Pool may contain duplicate lineups (the candidate-pool helper uses
    ``min_diff=1`` against prior CP-SAT solves which can still emit
    identical solutions when the model is symmetric); duplicates are
    skipped naturally because their drivers' budgets will already have
    been consumed by the prior accepted copy.
    """
    if target_count <= 0 or not candidates:
        return []

    sorted_pool = sorted(
        candidates,
        key=lambda score: objective_value(score, objective_metric),
        reverse=True,
    )

    seen_indices: set[tuple[int, ...]] = set()
    accepted: list[CandidateScore] = []
    rejected: list[CandidateScore] = []
    effective_cap = min(max(exposure_cap, 0.0), 1.0)
    relaxation_step = 0.05

    def _attempt_fill(pool: list[CandidateScore], cap: float) -> None:
        """Walk a pool, accepting candidates while budget remains."""
        budget = _per_driver_budget(target_count, cap)
        if budget <= 0:
            # Nothing can be admitted; leave everything for the next
            # relaxation pass (or the empty-return guard at the end).
            for score in pool:
                if score.indices not in seen_indices:
                    rejected.append(score)
                    seen_indices.add(score.indices)
            return
        exposure: dict[int, int] = {}
        for score in pool:
            if score.indices in seen_indices:
                continue
            # Reject when any driver in the candidate has hit the budget.
            if any(exposure.get(driver, 0) >= budget for driver in score.indices):
                rejected.append(score)
                seen_indices.add(score.indices)
                continue
            accepted.append(score)
            seen_indices.add(score.indices)
            for driver in score.indices:
                exposure[driver] = exposure.get(driver, 0) + 1
            if len(accepted) >= target_count:
                return

    _attempt_fill(sorted_pool, effective_cap)

    # Cap-relaxation safety net. When the pool can't fill the slot count
    # under the requested cap, widen the cap in fixed increments so the
    # operator sees the relaxation in the log and still receives a full
    # portfolio (subject to the pool actually containing enough distinct
    # lineups). Each pass walks only the currently-rejected pool to avoid
    # reconsidering already-accepted slots.
    while len(accepted) < target_count and effective_cap < 1.0:
        relaxed = min(1.0, round(effective_cap + relaxation_step, 4))
        if relaxed <= effective_cap:
            # Floating-point safety: don't loop forever if rounding kept
            # us at the same effective cap.
            break
        _LOG.warning(
            "greedy_diversify: relaxing exposure cap %.2f -> %.2f to fill %s lineups "
            "(currently accepted %s; %s candidates remain rejected)",
            effective_cap,
            relaxed,
            target_count,
            len(accepted),
            len(rejected),
        )
        effective_cap = relaxed
        # Replay the rejected pool against the relaxed cap. ``seen_indices``
        # already records the accepted lineups so they don't get double-counted.
        replay_pool = rejected
        rejected = []
        # Re-derive the exposure counter from the already-accepted slate
        # so the relaxed pass enforces the new (wider) budget against the
        # cumulative portfolio rather than starting from zero.
        carried_exposure: dict[int, int] = {}
        for chosen in accepted:
            for driver in chosen.indices:
                carried_exposure[driver] = carried_exposure.get(driver, 0) + 1
        budget_relaxed = _per_driver_budget(target_count, effective_cap)
        for score in replay_pool:
            if score.indices in seen_indices and score not in accepted:
                # Already rejected once; only revisit if not yet accepted.
                pass
            # Skip duplicates already in the accepted slate.
            if any(
                already.indices == score.indices for already in accepted
            ):
                continue
            if any(
                carried_exposure.get(driver, 0) >= budget_relaxed
                for driver in score.indices
            ):
                rejected.append(score)
                continue
            accepted.append(score)
            for driver in score.indices:
                carried_exposure[driver] = carried_exposure.get(driver, 0) + 1
            if len(accepted) >= target_count:
                return accepted[:target_count]

    return accepted[:target_count]
