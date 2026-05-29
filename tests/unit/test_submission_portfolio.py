"""Unit tests for submission-time portfolio selection."""

from __future__ import annotations

import math
from dataclasses import replace

import numpy as np
import pytest

from include.nascar.dfs.field_cutoff import estimate_field_cutoffs
from include.nascar.dfs.submission_portfolio import (
    STRATEGY_DOUBLE_UP,
    STRATEGY_HIGH_RISK_REWARD,
    ScoredCandidate,
    compare_portfolio_selectors,
    portfolio_diagnostics,
    score_candidates_for_submission,
    select_portfolio_for_submission,
    submission_strategy_config,
)


def _make_joint(
    rng: np.random.Generator,
    *,
    n_drivers: int = 12,
    n_simulations: int = 500,
) -> np.ndarray:
    means = rng.uniform(20.0, 40.0, size=n_drivers)
    return rng.normal(means[:, None], 3.0, size=(n_drivers, n_simulations))


def _lineup(indices: tuple[int, ...]) -> list[int]:
    return list(indices)


def test_estimate_field_cutoffs_shape() -> None:
    rng = np.random.default_rng(1)
    joint = _make_joint(rng)
    salaries = [8000 + 200 * i for i in range(joint.shape[0])]
    cutoffs = estimate_field_cutoffs(
        joint, salaries, n_field_samples=50, cash_percentile=0.55
    )
    assert cutoffs.shape == (joint.shape[1],)


def test_double_up_respects_tight_exposure() -> None:
    """Exposure cap limits chalk driver 0 to one lineup when budget is 1."""
    rng = np.random.default_rng(42)
    joint = _make_joint(rng, n_drivers=12)
    salaries = [8000] * 12
    candidates = [
        _lineup((0, 1, 2, 3, 4, 5)),
        _lineup((0, 1, 2, 3, 4, 6)),
        _lineup((0, 1, 2, 3, 5, 6)),
        _lineup((6, 7, 8, 9, 10, 11)),
        _lineup((7, 8, 9, 10, 11, 5)),
    ]
    cutoffs = np.full(joint.shape[1], 0.0)
    cfg = replace(
        submission_strategy_config(STRATEGY_DOUBLE_UP, 3),
        min_driver_diff=1,
        max_lineup_correlation=1.0,
        exposure_cap=0.25,
        use_quality_median_floor=False,
        relax_min_driver_diff=False,
        relax_exposure_cap=False,
    )
    selected = select_portfolio_for_submission(
        candidates,
        joint,
        n_lineups=3,
        field_cutoffs=cutoffs,
        config=cfg,
    )
    assert len(selected) == 2
    appearances_0 = sum(1 for s in selected if 0 in s.score.indices)
    assert appearances_0 == 1


def test_correlation_rejects_redundant_lineup() -> None:
    """Two lineups differ by one driver but share identical MC totals."""
    n_simulations = 400
    row_a = np.linspace(50.0, 60.0, n_simulations)
    row_c = np.full(n_simulations, 25.0)
    joint = np.stack([row_a, row_a, row_c, row_c, row_c, row_c, row_c])

    salaries = [8000] * 7
    candidates = [
        _lineup((0, 2, 3, 4, 5, 6)),
        _lineup((1, 2, 3, 4, 5, 6)),  # identical totals to lineup 0 every draw
        _lineup((0, 1, 2, 3, 4, 5)),
    ]
    cutoffs = np.full(n_simulations, 90.0)
    selected = select_portfolio_for_submission(
        candidates,
        joint,
        n_lineups=2,
        strategy_name=STRATEGY_DOUBLE_UP,
        field_cutoffs=cutoffs,
        config=submission_strategy_config(
            STRATEGY_DOUBLE_UP,
            2,
            exposure_cap=1.0,
            min_driver_diff=1,
        ),
    )
    index_sets = {s.score.indices for s in selected}
    assert (0, 2, 3, 4, 5, 6) in index_sets
    assert (1, 2, 3, 4, 5, 6) not in index_sets


def test_high_risk_reward_uses_higher_field_bar() -> None:
    rng = np.random.default_rng(99)
    joint = _make_joint(rng)
    salaries = [7000 + 300 * i for i in range(joint.shape[0])]
    candidates = [_lineup(tuple(range(6))), _lineup(tuple(range(1, 7)))]
    du_cfg = submission_strategy_config(STRATEGY_DOUBLE_UP, 2)
    hrr_cfg = submission_strategy_config(STRATEGY_HIGH_RISK_REWARD, 2)
    assert hrr_cfg.field_cash_percentile > du_cfg.field_cash_percentile

    du_cut = estimate_field_cutoffs(
        joint, salaries, cash_percentile=du_cfg.field_cash_percentile
    )
    hrr_cut = estimate_field_cutoffs(
        joint, salaries, cash_percentile=hrr_cfg.field_cash_percentile
    )
    assert float(np.mean(hrr_cut)) > float(np.mean(du_cut))

    du_scored = score_candidates_for_submission(
        candidates, joint, du_cut
    )
    hrr_scored = score_candidates_for_submission(
        candidates, joint, hrr_cut
    )
    assert du_scored[0].expected_cash_rate >= hrr_scored[0].expected_cash_rate


def test_dnf_weighted_cap_limits_risky_driver() -> None:
    rng = np.random.default_rng(5)
    joint = _make_joint(rng, n_drivers=8)
    salaries = [8000] * 8
    candidates = [
        _lineup((0, 1, 2, 3, 4, 5)),
        _lineup((0, 1, 2, 3, 4, 6)),
        _lineup((0, 1, 2, 3, 5, 6)),
        _lineup((7, 1, 2, 3, 4, 5)),
        _lineup((7, 1, 2, 3, 4, 6)),
    ]
    cutoffs = estimate_field_cutoffs(joint, salaries, n_field_samples=40)
    cfg = submission_strategy_config(
        STRATEGY_DOUBLE_UP, 4, exposure_cap=0.5, min_driver_diff=1
    )
    selected_high_dnf = select_portfolio_for_submission(
        candidates,
        joint,
        n_lineups=4,
        strategy_name=STRATEGY_DOUBLE_UP,
        field_cutoffs=cutoffs,
        config=cfg,
        driver_pdnf={0: 0.9},
    )
    appearances_0 = sum(1 for s in selected_high_dnf if 0 in s.score.indices)
    selected_low_dnf = select_portfolio_for_submission(
        candidates,
        joint,
        n_lineups=4,
        strategy_name=STRATEGY_DOUBLE_UP,
        field_cutoffs=cutoffs,
        config=cfg,
        driver_pdnf={0: 0.0},
    )
    appearances_0_low = sum(1 for s in selected_low_dnf if 0 in s.score.indices)
    assert appearances_0 <= appearances_0_low


def test_high_risk_reward_config_differs_from_double_up() -> None:
    du = submission_strategy_config(STRATEGY_DOUBLE_UP, 10)
    hrr = submission_strategy_config(STRATEGY_HIGH_RISK_REWARD, 10)
    assert hrr.field_cash_percentile > du.field_cash_percentile
    assert hrr.exposure_cap > du.exposure_cap
    assert hrr.ceiling_metric == "joint_p90"
    assert du.quality_metric == "joint_p10"
    assert hrr.min_driver_diff <= du.min_driver_diff


def test_compare_portfolio_selectors_returns_both_strategies() -> None:
    rng = np.random.default_rng(11)
    joint = _make_joint(rng)
    salaries = [7500 + 200 * i for i in range(joint.shape[0])]
    candidates = [_lineup(tuple(range(6))), _lineup((0, 1, 2, 7, 8, 9))]
    results = compare_portfolio_selectors(
        candidates, joint, salaries, n_lineups=2
    )
    assert STRATEGY_DOUBLE_UP in results
    assert STRATEGY_HIGH_RISK_REWARD in results


def test_portfolio_diagnostics_empty() -> None:
    diag = portfolio_diagnostics([])
    assert diag.n_lineups == 0
    assert diag.max_driver_exposure == 0
