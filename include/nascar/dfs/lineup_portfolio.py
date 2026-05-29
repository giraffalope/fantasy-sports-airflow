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
import os
from dataclasses import dataclass
from typing import Iterable

import numpy as np

from include.nascar.dfs.lineup_constraints import min_driver_diff_ok

_LOG = logging.getLogger(__name__)

# Std-floor for the sharpe ratio so a degenerate (constant-sample) lineup
# does not blow up to +/- inf. Same epsilon used in the SQL ``mc_sharpe``
# comment so application code and audit semantics stay aligned.
_SHARPE_STD_FLOOR = 1e-6
_CORR_STD_FLOOR = 1e-9

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
    lineup_totals: np.ndarray | None = None


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def dk_write_quality_floor_enabled() -> bool:
    raw = os.environ.get("FS_DK_WRITE_QUALITY_FLOOR", "1")
    return raw.strip().lower() not in ("0", "false", "no", "")


def dk_write_max_lineup_correlation() -> float:
    return float(np.clip(_env_float("FS_DK_WRITE_MAX_LINEUP_CORR", 0.85), 0.0, 1.0))


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
                lineup_totals=lineup_totals,
            )
        )
    return scores


def merge_scored_candidates(
    *pools: Iterable[CandidateScore],
) -> list[CandidateScore]:
    """Dedupe scored candidates by ``indices``, keeping the higher ``p50``."""
    merged: dict[tuple[int, ...], CandidateScore] = {}
    for pool in pools:
        for score in pool:
            key = score.indices
            prior = merged.get(key)
            if prior is None or score.p50 > prior.p50:
                merged[key] = score
    return list(merged.values())


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


def _accepted_indices(accepted: list[CandidateScore]) -> list[tuple[int, ...]]:
    return [score.indices for score in accepted]


def _exposure_from_accepted(accepted: list[CandidateScore]) -> dict[int, int]:
    exposure: dict[int, int] = {}
    for chosen in accepted:
        for driver in chosen.indices:
            exposure[driver] = exposure.get(driver, 0) + 1
    return exposure


def _pearson_corr(a: np.ndarray, b: np.ndarray) -> float:
    a_std = float(np.std(a, ddof=0))
    b_std = float(np.std(b, ddof=0))
    if a_std < _CORR_STD_FLOOR or b_std < _CORR_STD_FLOOR:
        return 1.0 if np.allclose(a, b) else 0.0
    return float(np.corrcoef(a, b)[0, 1])


def _max_corr_with_accepted(
    candidate: CandidateScore,
    accepted: list[CandidateScore],
) -> float:
    if not accepted or candidate.lineup_totals is None:
        return 0.0
    corrs = [
        _pearson_corr(candidate.lineup_totals, other.lineup_totals)
        for other in accepted
        if other.lineup_totals is not None
    ]
    return max(corrs) if corrs else 0.0


def _apply_write_quality_floor(
    pool: list[CandidateScore],
    objective_metric: str,
) -> list[CandidateScore]:
    if not pool:
        return pool
    values = [objective_value(score, objective_metric) for score in pool]
    median = float(np.median(values))
    filtered = [
        score
        for score in pool
        if objective_value(score, objective_metric) >= median
    ]
    return filtered or pool


def greedy_diversify(
    candidates: list[CandidateScore],
    *,
    target_count: int,
    exposure_cap: float,
    objective_metric: str,
    min_driver_diff: int = 1,
    lineup_size: int = 6,
    relax_min_driver_diff: bool = True,
    use_quality_floor: bool | None = None,
    max_lineup_correlation: float | None = None,
) -> list[CandidateScore]:
    """Greedy exposure- and overlap-capped selection from a scored candidate pool.

    Algorithm:

    1. Optionally drop candidates below the pool median on ``objective_metric``
       (``FS_DK_WRITE_QUALITY_FLOOR``, default on).
    2. Sort the input pool by ``objective_value(score, objective_metric)``
       descending.
    3. Walk the sorted pool: accept a candidate iff every driver still has
       exposure budget, the lineup differs from every accepted lineup by
       at least ``min_driver_diff`` drivers, and pairwise MC correlation on
       lineup totals is at most ``max_lineup_correlation``.
    4. Stop when ``target_count`` lineups are selected or the pool is
       exhausted.

    If ``min_driver_diff`` is too tight to fill the slot count, relax it
    down to ``1`` (when ``relax_min_driver_diff`` is True). If the exposure
    cap is still too tight, widen it in ``+0.05`` increments up to ``1.0``.
    Each relaxation logs a warning.
    """
    if target_count <= 0 or not candidates:
        return []

    quality_floor = (
        dk_write_quality_floor_enabled()
        if use_quality_floor is None
        else use_quality_floor
    )
    max_corr = (
        dk_write_max_lineup_correlation()
        if max_lineup_correlation is None
        else float(np.clip(max_lineup_correlation, 0.0, 1.0))
    )

    working = list(candidates)
    if quality_floor:
        working = _apply_write_quality_floor(working, objective_metric)

    sorted_pool = sorted(
        working,
        key=lambda score: objective_value(score, objective_metric),
        reverse=True,
    )

    effective_min_diff = max(1, min(min_driver_diff, lineup_size))
    effective_cap = min(max(exposure_cap, 0.0), 1.0)
    relaxation_step = 0.05

    accepted: list[CandidateScore] = []
    rejected: list[CandidateScore] = []
    seen_indices: set[tuple[int, ...]] = set()

    def _can_accept(
        score: CandidateScore,
        exposure: dict[int, int],
        budget: int,
        min_diff: int,
    ) -> bool:
        if not min_driver_diff_ok(
            score.indices,
            _accepted_indices(accepted),
            min_diff=min_diff,
            lineup_size=lineup_size,
        ):
            return False
        if max_corr < 1.0 and _max_corr_with_accepted(score, accepted) > max_corr:
            return False
        return not any(exposure.get(driver, 0) >= budget for driver in score.indices)

    def _attempt_fill(pool: list[CandidateScore], cap: float, min_diff: int) -> None:
        nonlocal rejected
        budget = _per_driver_budget(target_count, cap)
        exposure = _exposure_from_accepted(accepted)
        for score in pool:
            if score.indices in seen_indices:
                continue
            if budget <= 0 or not _can_accept(score, exposure, budget, min_diff):
                rejected.append(score)
                seen_indices.add(score.indices)
                continue
            accepted.append(score)
            seen_indices.add(score.indices)
            for driver in score.indices:
                exposure[driver] = exposure.get(driver, 0) + 1
            if len(accepted) >= target_count:
                return

    _attempt_fill(sorted_pool, effective_cap, effective_min_diff)

    if relax_min_driver_diff:
        while len(accepted) < target_count and effective_min_diff > 1:
            relaxed_diff = effective_min_diff - 1
            _LOG.warning(
                "greedy_diversify: relaxing min_driver_diff %s -> %s to fill %s lineups "
                "(currently accepted %s; %s candidates rejected)",
                effective_min_diff,
                relaxed_diff,
                target_count,
                len(accepted),
                len(rejected),
            )
            effective_min_diff = relaxed_diff
            replay_pool = rejected
            rejected = []
            for score in replay_pool:
                seen_indices.discard(score.indices)
            _attempt_fill(replay_pool, effective_cap, effective_min_diff)

    while len(accepted) < target_count and effective_cap < 1.0:
        relaxed_cap = min(1.0, round(effective_cap + relaxation_step, 4))
        if relaxed_cap <= effective_cap:
            break
        _LOG.warning(
            "greedy_diversify: relaxing exposure cap %.2f -> %.2f to fill %s lineups "
            "(currently accepted %s; %s candidates remain rejected)",
            effective_cap,
            relaxed_cap,
            target_count,
            len(accepted),
            len(rejected),
        )
        effective_cap = relaxed_cap
        replay_pool = rejected
        rejected = []
        budget_relaxed = _per_driver_budget(target_count, effective_cap)
        exposure = _exposure_from_accepted(accepted)
        for score in replay_pool:
            if any(already.indices == score.indices for already in accepted):
                continue
            if not _can_accept(score, exposure, budget_relaxed, effective_min_diff):
                rejected.append(score)
                continue
            accepted.append(score)
            seen_indices.add(score.indices)
            for driver in score.indices:
                exposure[driver] = exposure.get(driver, 0) + 1
            if len(accepted) >= target_count:
                break

    return accepted[:target_count]
