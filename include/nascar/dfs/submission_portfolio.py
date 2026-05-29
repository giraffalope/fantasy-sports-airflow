"""Submission-time portfolio selection for Double Up and High Risk Reward contests.

Selects ``n`` lineups from a scored candidate pool using strategy-specific
constraints (exposure, min driver difference, correlation limits) and
objectives (expected cashing lineup count vs synthetic field cutoffs).

Monte Carlo simulation is not modified here; callers pass the joint draw
matrix produced upstream.
"""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from typing import Mapping, Sequence

import numpy as np

from include.nascar.dfs.field_cutoff import estimate_field_cutoffs
from include.nascar.dfs.lineup_constraints import driver_overlap, min_driver_diff_ok
from include.nascar.dfs.lineup_portfolio import (
    CandidateScore,
    _per_driver_budget,
    objective_value,
    score_candidates_against_joint_samples,
)

_LOG = logging.getLogger(__name__)

STRATEGY_DOUBLE_UP = "DOUBLE_UP"
STRATEGY_HIGH_RISK_REWARD = "HIGH_RISK_REWARD"

_CORR_STD_FLOOR = 1e-9
_DEFAULT_DNF_CAP_WEIGHT = 0.5
_DEFAULT_LINEUP_SIZE = 6


@dataclass(frozen=True)
class ScoredCandidate:
    """Candidate with joint MC vector and cash-rate vs a field cutoff."""

    score: CandidateScore
    lineup_totals: np.ndarray  # shape (n_simulations,)
    expected_cash_rate: float  # mean(1[total >= cutoff])


@dataclass(frozen=True)
class SubmissionStrategyConfig:
    """Knobs for :func:`select_portfolio_for_submission`."""

    strategy_name: str
    exposure_cap: float
    min_driver_diff: int
    field_cash_percentile: float
    max_lineup_correlation: float
    quality_metric: str
    use_quality_median_floor: bool
    ceiling_metric: str | None  # tie-break / bonus for GPP
    ceiling_weight: float
    dnf_cap_weight: float
    relax_min_driver_diff: bool = True
    relax_exposure_cap: bool = True


@dataclass(frozen=True)
class PortfolioDiagnostics:
    """Exposure and overlap summary for a submitted slate."""

    n_lineups: int
    lineup_size: int
    max_driver_exposure: int
    mean_pairwise_shared_drivers: float
    max_pairwise_correlation: float
    mean_expected_cash_rate: float
    driver_appearances: dict[int, int]


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _lineup_size(lineup_size: int | None) -> int:
    if lineup_size is not None:
        return lineup_size
    return _env_int("FS_DK_LINEUP_SIZE", _DEFAULT_LINEUP_SIZE)


def default_exposure_cap_for_strategy(strategy_name: str, n_lineups: int) -> float:
    """Resolve submission exposure cap from env or strategy defaults."""
    if strategy_name == STRATEGY_DOUBLE_UP:
        return _env_float("FS_DK_SUBMIT_DOUBLE_UP_EXPOSURE_CAP", 0.25)
    if strategy_name == STRATEGY_HIGH_RISK_REWARD:
        return _env_float("FS_DK_SUBMIT_HIGH_RISK_REWARD_EXPOSURE_CAP", 0.35)
    return _env_float("FS_DK_SUBMIT_EXPOSURE_CAP", max(1.0 / 3.0, 0.25))


def default_min_driver_diff(strategy_name: str, n_lineups: int) -> int:
    if strategy_name == STRATEGY_DOUBLE_UP:
        base = _env_int("FS_DK_SUBMIT_DOUBLE_UP_MIN_DIFF", 3)
    elif strategy_name == STRATEGY_HIGH_RISK_REWARD:
        base = _env_int("FS_DK_SUBMIT_HIGH_RISK_REWARD_MIN_DIFF", 2)
    else:
        base = _env_int("FS_DK_SUBMIT_MIN_DIFF", 2)
    if n_lineups <= 15:
        return max(base, 2 if strategy_name == STRATEGY_HIGH_RISK_REWARD else 3)
    return max(1, base - 1)


def submission_strategy_config(
    strategy_name: str,
    n_lineups: int,
    *,
    exposure_cap: float | None = None,
    min_driver_diff: int | None = None,
) -> SubmissionStrategyConfig:
    """Built-in configs for Double Up (cash) vs High Risk Reward (GPP min-cash)."""
    cap = (
        exposure_cap
        if exposure_cap is not None
        else default_exposure_cap_for_strategy(strategy_name, n_lineups)
    )
    min_diff = (
        min_driver_diff
        if min_driver_diff is not None
        else default_min_driver_diff(strategy_name, n_lineups)
    )

    if strategy_name == STRATEGY_DOUBLE_UP:
        return SubmissionStrategyConfig(
            strategy_name=strategy_name,
            exposure_cap=cap,
            min_driver_diff=min_diff,
            field_cash_percentile=_env_float(
                "FS_DK_DOUBLE_UP_FIELD_CASH_PERCENTILE", 0.55
            ),
            max_lineup_correlation=_env_float("FS_DK_DOUBLE_UP_MAX_LINEUP_CORR", 0.85),
            quality_metric="joint_p10",
            use_quality_median_floor=True,
            ceiling_metric=None,
            ceiling_weight=0.0,
            dnf_cap_weight=_env_float(
                "FS_DK_DOUBLE_UP_DNF_CAP_WEIGHT", _DEFAULT_DNF_CAP_WEIGHT
            ),
        )

    if strategy_name == STRATEGY_HIGH_RISK_REWARD:
        return SubmissionStrategyConfig(
            strategy_name=strategy_name,
            exposure_cap=cap,
            min_driver_diff=min_diff,
            field_cash_percentile=_env_float(
                "FS_DK_HIGH_RISK_REWARD_FIELD_CASH_PERCENTILE", 0.82
            ),
            max_lineup_correlation=_env_float(
                "FS_DK_HIGH_RISK_REWARD_MAX_LINEUP_CORR", 0.90
            ),
            quality_metric="joint_p50",
            use_quality_median_floor=True,
            ceiling_metric="joint_p90",
            ceiling_weight=_env_float("FS_DK_HIGH_RISK_REWARD_CEILING_WEIGHT", 0.15),
            dnf_cap_weight=_env_float(
                "FS_DK_HIGH_RISK_REWARD_DNF_CAP_WEIGHT", _DEFAULT_DNF_CAP_WEIGHT * 0.5
            ),
        )

    return SubmissionStrategyConfig(
        strategy_name=strategy_name,
        exposure_cap=cap,
        min_driver_diff=min_diff,
        field_cash_percentile=_env_float("FS_DK_FIELD_CASH_PERCENTILE", 0.55),
        max_lineup_correlation=_env_float("FS_DK_MAX_LINEUP_CORR", 0.85),
        quality_metric="joint_p50",
        use_quality_median_floor=True,
        ceiling_metric=None,
        ceiling_weight=0.0,
        dnf_cap_weight=_env_float("FS_DK_DNF_CAP_WEIGHT", _DEFAULT_DNF_CAP_WEIGHT),
    )


def score_candidates_for_submission(
    candidates: Sequence[Sequence[int]],
    joint_samples: np.ndarray,
    field_cutoffs: np.ndarray,
) -> list[ScoredCandidate]:
    """Score candidates and attach per-draw totals plus expected cash rate."""
    base_scores = score_candidates_against_joint_samples(candidates, joint_samples)
    if not base_scores:
        return []

    if field_cutoffs.shape != (joint_samples.shape[1],):
        raise ValueError(
            f"field_cutoffs shape {field_cutoffs.shape} != "
            f"(n_simulations,) {(joint_samples.shape[1],)}"
        )

    detailed: list[ScoredCandidate] = []
    for base in base_scores:
        totals = np.sum(joint_samples[list(base.indices), :], axis=0)
        cash = totals >= field_cutoffs
        detailed.append(
            ScoredCandidate(
                score=base,
                lineup_totals=totals,
                expected_cash_rate=float(np.mean(cash)),
            )
        )
    return detailed


def _pearson_corr(a: np.ndarray, b: np.ndarray) -> float:
    a_std = float(np.std(a, ddof=0))
    b_std = float(np.std(b, ddof=0))
    if a_std < _CORR_STD_FLOOR or b_std < _CORR_STD_FLOOR:
        return 1.0 if np.allclose(a, b) else 0.0
    return float(np.corrcoef(a, b)[0, 1])


def _max_corr_with_accepted(
    candidate: ScoredCandidate,
    accepted: Sequence[ScoredCandidate],
) -> float:
    if not accepted:
        return 0.0
    return max(
        _pearson_corr(candidate.lineup_totals, other.lineup_totals)
        for other in accepted
    )


def _driver_budget(
    driver: int,
    *,
    target_count: int,
    exposure_cap: float,
    driver_pdnf: Mapping[int, float] | None,
    dnf_cap_weight: float,
) -> int:
    base = _per_driver_budget(target_count, exposure_cap)
    if driver_pdnf is None or dnf_cap_weight <= 0.0:
        return base
    p_dnf = float(driver_pdnf.get(driver, 0.0))
    p_dnf = min(max(p_dnf, 0.0), 1.0)
    adjusted = max(1, int(math.floor(base * (1.0 - dnf_cap_weight * p_dnf))))
    return min(adjusted, target_count)


def _portfolio_expected_cash_count(
    accepted: Sequence[ScoredCandidate],
    field_cutoffs: np.ndarray,
) -> float:
    if not accepted:
        return 0.0
    stacked = np.stack([c.lineup_totals for c in accepted], axis=0)
    cash = stacked >= field_cutoffs
    return float(np.mean(np.sum(cash, axis=0)))


def _marginal_cash_gain(
    candidate: ScoredCandidate,
    accepted: Sequence[ScoredCandidate],
    field_cutoffs: np.ndarray,
) -> float:
    if not accepted:
        return candidate.expected_cash_rate
    stacked = np.stack([c.lineup_totals for c in accepted], axis=0)
    prior_cash = np.sum(stacked >= field_cutoffs, axis=0)
    with_candidate = prior_cash + (candidate.lineup_totals >= field_cutoffs)
    return float(np.mean(with_candidate - prior_cash))


def _apply_quality_floor(
    pool: list[ScoredCandidate],
    config: SubmissionStrategyConfig,
) -> list[ScoredCandidate]:
    if not config.use_quality_median_floor or not pool:
        return pool
    values = [objective_value(c.score, config.quality_metric) for c in pool]
    median = float(np.median(values))
    return [
        c
        for c in pool
        if objective_value(c.score, config.quality_metric) >= median
    ] or pool


def _selection_sort_key(
    candidate: ScoredCandidate,
    config: SubmissionStrategyConfig,
) -> float:
    key = candidate.expected_cash_rate
    if config.ceiling_metric and config.ceiling_weight > 0.0:
        ceiling = objective_value(candidate.score, config.ceiling_metric)
        key += config.ceiling_weight * ceiling / 100.0
    return key


def select_portfolio_for_submission(
    candidates: Sequence[Sequence[int]],
    joint_samples: np.ndarray,
    *,
    n_lineups: int,
    strategy_name: str = STRATEGY_DOUBLE_UP,
    salaries: Sequence[int] | None = None,
    field_cutoffs: np.ndarray | None = None,
    config: SubmissionStrategyConfig | None = None,
    driver_pdnf: Mapping[int, float] | None = None,
    lineup_size: int | None = None,
) -> list[ScoredCandidate]:
    """Select ``n_lineups`` for contest submission.

    Double Up (``DOUBLE_UP``): tight exposure, ``min_driver_diff`` ≥ 3 for
    small slates, field cutoff at ~top 45% of synthetic field, maximize
    expected count of cashing lineups.

    High Risk Reward (``HIGH_RISK_REWARD``): looser exposure, higher field
    cutoff (~top 18% min-cash in GPP), quality floor on ``joint_p50`` with a
    ``joint_p90`` ceiling bonus, same cash-count objective at the GPP bar.
    """
    if n_lineups <= 0 or not candidates:
        return []

    cfg = config or submission_strategy_config(strategy_name, n_lineups)
    size = _lineup_size(lineup_size)

    if field_cutoffs is None:
        if salaries is None:
            raise ValueError(
                "salaries required when field_cutoffs are not provided"
            )
        field_cutoffs = estimate_field_cutoffs(
            joint_samples,
            salaries,
            lineup_size=size,
            cash_percentile=cfg.field_cash_percentile,
        )

    pool = score_candidates_for_submission(candidates, joint_samples, field_cutoffs)
    pool = _apply_quality_floor(pool, cfg)

    effective_min_diff = cfg.min_driver_diff
    effective_cap = min(max(cfg.exposure_cap, 0.0), 1.0)
    relaxation_step = 0.05

    def _fill_portfolio(
        accepted: list[ScoredCandidate],
        *,
        min_diff: int,
        exposure_cap: float,
    ) -> None:
        exposure: dict[int, int] = {}
        for cand in accepted:
            for driver in cand.score.indices:
                exposure[driver] = exposure.get(driver, 0) + 1
        seen = {c.score.indices for c in accepted}
        remaining = [c for c in pool if c.score.indices not in seen]

        while len(accepted) < n_lineups and remaining:
            best_idx: int | None = None
            best_gain = -1.0
            for idx, cand in enumerate(remaining):
                if not min_driver_diff_ok(
                    cand.score.indices,
                    [other.score.indices for other in accepted],
                    min_diff=min_diff,
                    lineup_size=size,
                ):
                    continue
                if _max_corr_with_accepted(cand, accepted) > cfg.max_lineup_correlation:
                    continue
                budget_ok = all(
                    exposure.get(driver, 0)
                    < _driver_budget(
                        driver,
                        target_count=n_lineups,
                        exposure_cap=exposure_cap,
                        driver_pdnf=driver_pdnf,
                        dnf_cap_weight=cfg.dnf_cap_weight,
                    )
                    for driver in cand.score.indices
                )
                if not budget_ok:
                    continue
                gain = _marginal_cash_gain(cand, accepted, field_cutoffs)
                if gain > best_gain:
                    best_gain = gain
                    best_idx = idx

            if best_idx is None:
                break

            chosen = remaining.pop(best_idx)
            seen.add(chosen.score.indices)
            accepted.append(chosen)
            for driver in chosen.score.indices:
                exposure[driver] = exposure.get(driver, 0) + 1

    accepted: list[ScoredCandidate] = []
    _fill_portfolio(accepted, min_diff=effective_min_diff, exposure_cap=effective_cap)

    if cfg.relax_min_driver_diff:
        while len(accepted) < n_lineups and effective_min_diff > 1:
            effective_min_diff -= 1
            _LOG.warning(
                "select_portfolio_for_submission: relaxing min_driver_diff to %s "
                "(%s/%s lineups filled, strategy=%s)",
                effective_min_diff,
                len(accepted),
                n_lineups,
                cfg.strategy_name,
            )
            _fill_portfolio(
                accepted, min_diff=effective_min_diff, exposure_cap=effective_cap
            )

    if cfg.relax_exposure_cap:
        while len(accepted) < n_lineups and effective_cap < 1.0:
            relaxed = min(1.0, round(effective_cap + relaxation_step, 4))
            if relaxed <= effective_cap:
                break
            _LOG.warning(
                "select_portfolio_for_submission: relaxing exposure cap %.2f -> %.2f "
                "(%s/%s lineups, strategy=%s)",
                effective_cap,
                relaxed,
                len(accepted),
                n_lineups,
                cfg.strategy_name,
            )
            effective_cap = relaxed
            _fill_portfolio(
                accepted, min_diff=effective_min_diff, exposure_cap=effective_cap
            )

    return accepted[:n_lineups]


def portfolio_diagnostics(
    selected: Sequence[ScoredCandidate],
    *,
    lineup_size: int | None = None,
) -> PortfolioDiagnostics:
    """Summarize overlap, exposure, and correlation for a submitted slate."""
    size = _lineup_size(lineup_size)
    n = len(selected)
    if n == 0:
        return PortfolioDiagnostics(
            n_lineups=0,
            lineup_size=size,
            max_driver_exposure=0,
            mean_pairwise_shared_drivers=0.0,
            max_pairwise_correlation=0.0,
            mean_expected_cash_rate=0.0,
            driver_appearances={},
        )

    appearances: dict[int, int] = {}
    for cand in selected:
        for driver in cand.score.indices:
            appearances[driver] = appearances.get(driver, 0) + 1

    shared: list[int] = []
    corrs: list[float] = []
    for i in range(n):
        for j in range(i + 1, n):
            shared.append(
                driver_overlap(
                    selected[i].score.indices,
                    selected[j].score.indices,
                )
            )
            corrs.append(
                _pearson_corr(
                    selected[i].lineup_totals,
                    selected[j].lineup_totals,
                )
            )

    return PortfolioDiagnostics(
        n_lineups=n,
        lineup_size=size,
        max_driver_exposure=max(appearances.values()) if appearances else 0,
        mean_pairwise_shared_drivers=float(np.mean(shared)) if shared else 0.0,
        max_pairwise_correlation=max(corrs) if corrs else 0.0,
        mean_expected_cash_rate=float(
            np.mean([c.expected_cash_rate for c in selected])
        ),
        driver_appearances=appearances,
    )


def compare_portfolio_selectors(
    candidates: Sequence[Sequence[int]],
    joint_samples: np.ndarray,
    salaries: Sequence[int],
    *,
    n_lineups: int,
    strategies: Sequence[str] = (STRATEGY_DOUBLE_UP, STRATEGY_HIGH_RISK_REWARD),
) -> dict[str, dict[str, float]]:
    """Backtest helper: metrics per strategy for the submission selector."""
    results: dict[str, dict[str, float]] = {}
    for name in strategies:
        selected = select_portfolio_for_submission(
            candidates,
            joint_samples,
            n_lineups=n_lineups,
            strategy_name=name,
            salaries=salaries,
        )
        diag = portfolio_diagnostics(selected)
        cutoffs = estimate_field_cutoffs(
            joint_samples,
            salaries,
            cash_percentile=submission_strategy_config(name, n_lineups).field_cash_percentile,
        )
        results[name] = {
            "n_selected": float(len(selected)),
            "portfolio_expected_cash_count": _portfolio_expected_cash_count(
                selected, cutoffs
            ),
            "mean_lineup_cash_rate": diag.mean_expected_cash_rate,
            "max_driver_exposure": float(diag.max_driver_exposure),
            "mean_pairwise_shared_drivers": diag.mean_pairwise_shared_drivers,
            "max_pairwise_correlation": diag.max_pairwise_correlation,
        }
    return results
