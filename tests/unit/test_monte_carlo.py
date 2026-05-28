"""Unit tests for the pure Monte Carlo simulation core."""

from __future__ import annotations

import json
from unittest.mock import patch

import numpy as np
import pytest

from include.nascar.ml.monte_carlo import (
    MIN_SIMULATIONS,
    MonteCarloDriverInput,
    _apply_constraints,
    simulate_driver_distributions,
)


_DK_RULES_JSON = json.dumps(
    {
        "components": [
            {
                "name": "place_finish",
                "type": "finish_table",
                "source": "r_finish_position",
                "formula": {"subtract_position": True, "base": 45, "min": 1},
            },
            {
                "name": "laps_led",
                "type": "linear",
                "source": "r_total_laps_led",
                "multiplier": 0.25,
            },
            {
                "name": "fastest_laps",
                "type": "linear",
                "source": "r_total_fastest_laps",
                "multiplier": 0.5,
            },
        ]
    }
)


def _driver(
    car_driver_id: int,
    *,
    finish: float = 5.0,
    laps_led: float = 30.0,
    fastest_laps: float = 4.0,
    sigma_finish: float = 4.0,
    sigma_laps_led: float = 12.0,
    sigma_fastest_laps: float = 2.0,
    start: float = 5.0,
) -> MonteCarloDriverInput:
    return MonteCarloDriverInput(
        car_driver_id=car_driver_id,
        r_start_position=start,
        predicted_targets={
            "r_finish_position": finish,
            "r_total_laps_led": laps_led,
            "r_total_fastest_laps": fastest_laps,
        },
        target_sigmas={
            "r_finish_position": sigma_finish,
            "r_total_laps_led": sigma_laps_led,
            "r_total_fastest_laps": sigma_fastest_laps,
        },
    )


def test_apply_constraints_clamps_finish_to_field() -> None:
    assert _apply_constraints("r_finish_position", -3.4, field_size=40) == 1.0
    assert _apply_constraints("r_finish_position", 999, field_size=40) == 40.0
    assert _apply_constraints("r_finish_position", 7.4, field_size=40) == 7.0
    assert _apply_constraints("r_finish_position", 7.6, field_size=40) == 8.0


def test_apply_constraints_non_negative_int_targets() -> None:
    assert _apply_constraints("r_total_laps_led", -2.6, field_size=40) == 0.0
    assert _apply_constraints("r_total_fastest_laps", 3.49, field_size=40) == 3.0
    assert _apply_constraints("r_total_fastest_laps", 3.50, field_size=40) == 4.0


def test_simulate_rejects_below_minimum() -> None:
    with pytest.raises(ValueError):
        simulate_driver_distributions(
            [_driver(1)],
            scoring_rules=_DK_RULES_JSON,
            n_simulations=MIN_SIMULATIONS - 1,
            seed=1,
        )


def test_simulate_returns_one_summary_per_driver() -> None:
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(10)]
    result = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=42,
    )
    summaries = result.summaries
    assert [s.car_driver_id for s in summaries] == [d.car_driver_id for d in drivers]
    assert all(s.n_simulations == MIN_SIMULATIONS for s in summaries)
    # Joint-samples matrix is now part of the return contract: shape lines up
    # with the summaries list and the driver index is aligned by position.
    assert result.joint_dfs_points.shape == (len(drivers), MIN_SIMULATIONS)
    assert result.driver_index.tolist() == [d.car_driver_id for d in drivers]


def test_simulate_percentiles_are_ordered_for_each_driver() -> None:
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(5)]
    summaries = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=7,
    ).summaries
    for s in summaries:
        assert s.min_dfs_points <= s.p05_dfs_points <= s.p50_dfs_points <= s.p95_dfs_points
        assert s.p95_dfs_points <= s.max_dfs_points
        assert s.std_dfs_points >= 0.0
        # Per-target percentile ordering must hold for the three new
        # distribution blocks too; same invariants as the DFS-points block.
        assert (
            s.min_r_finish_position
            <= s.p05_r_finish_position
            <= s.p50_r_finish_position
            <= s.p95_r_finish_position
            <= s.max_r_finish_position
        )
        assert s.std_r_finish_position >= 0.0
        assert (
            s.min_r_total_fastest_laps
            <= s.p05_r_total_fastest_laps
            <= s.p50_r_total_fastest_laps
            <= s.p95_r_total_fastest_laps
            <= s.max_r_total_fastest_laps
        )
        assert s.std_r_total_fastest_laps >= 0.0
        assert (
            s.min_r_total_laps_led
            <= s.p05_r_total_laps_led
            <= s.p50_r_total_laps_led
            <= s.p95_r_total_laps_led
            <= s.max_r_total_laps_led
        )
        assert s.std_r_total_laps_led >= 0.0
        # No DNF model attached -> dnf_pct must be exactly zero.
        assert s.dnf_pct == 0.0


def test_simulate_is_deterministic_given_seed() -> None:
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(8)]
    a = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=123
    ).summaries
    b = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=123
    ).summaries
    for left, right in zip(a, b, strict=True):
        assert left == right


def test_simulate_changes_with_different_seed() -> None:
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(8)]
    a = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=1
    ).summaries
    b = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=2
    ).summaries
    assert any(left.mean_dfs_points != right.mean_dfs_points for left, right in zip(a, b))


def test_simulate_holds_target_constant_when_sigma_is_zero() -> None:
    """A driver with all-zero sigmas must produce a degenerate distribution."""
    drivers = [
        MonteCarloDriverInput(
            car_driver_id=99,
            r_start_position=10.0,
            predicted_targets={
                "r_finish_position": 8.0,
                "r_total_laps_led": 50.0,
                "r_total_fastest_laps": 5.0,
            },
            target_sigmas={
                "r_finish_position": 0.0,
                "r_total_laps_led": 0.0,
                "r_total_fastest_laps": 0.0,
            },
        )
    ]
    summaries = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=0
    ).summaries
    assert summaries[0].std_dfs_points == pytest.approx(0.0)
    assert summaries[0].min_dfs_points == summaries[0].max_dfs_points


def test_simulate_finish_position_stays_inside_field() -> None:
    """The simulator must never re-score with finish positions outside ``[1, field_size]``.

    E2 rank-coupling: the simulator argsorts each iteration's raw finish
    draws across the entire field so every ``r_finish_position`` it sends
    to the scorer is by construction an integer permutation of
    ``1..len(drivers)``. We assert that here on a multi-driver field so
    the permutation invariant — not the legacy per-driver clip — is what
    keeps finishes inside ``[1, field_size]``.
    """
    drivers = [
        _driver(i + 1, finish=float(i + 1), sigma_finish=20.0,
                sigma_laps_led=0.0, sigma_fastest_laps=0.0)
        for i in range(10)
    ]
    captured: list[float] = []

    real_compute = (
        __import__("include.nascar.ml.monte_carlo", fromlist=["compute_dfs_points_from_targets"]).compute_dfs_points_from_targets
    )

    def _capture(start: object, targets: dict, rules: object):
        captured.append(targets["r_finish_position"])
        return real_compute(start, targets, rules)

    with patch(
        "include.nascar.ml.monte_carlo.compute_dfs_points_from_targets",
        side_effect=_capture,
    ):
        simulate_driver_distributions(
            drivers,
            scoring_rules=_DK_RULES_JSON,
            n_simulations=MIN_SIMULATIONS,
            field_size=len(drivers),
            seed=5,
        )

    assert captured, "expected scoring to be called at least once"
    assert all(1.0 <= finish <= float(len(drivers)) for finish in captured)
    assert all(float(finish).is_integer() for finish in captured)


def test_simulate_empty_drivers_returns_empty_result() -> None:
    result = simulate_driver_distributions(
        [],
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=0,
    )
    assert result.summaries == []
    assert result.joint_dfs_points.shape == (0, 0)
    assert result.driver_index.shape == (0,)


def test_simulate_rejects_zero_field_size() -> None:
    with pytest.raises(ValueError):
        simulate_driver_distributions(
            [_driver(1)],
            scoring_rules=_DK_RULES_JSON,
            n_simulations=MIN_SIMULATIONS,
            field_size=0,
            seed=1,
        )


def test_summary_mean_is_finite_and_in_range() -> None:
    """E2 rank-coupling: summary stats must stay finite and consistent.

    The legacy version of this test cross-checked the simulator against
    a manual numpy replay tied to the per-driver loop ordering. The E2
    refactor switched to a per-iteration-then-per-driver loop with
    vectorized rng draws, which makes the manual replay brittle. The
    invariants this test now defends are the only ones we actually
    promise to downstream consumers: every per-driver summary is
    finite, mean lies between min and max, and percentile ordering is
    consistent.
    """
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(8)]
    summaries = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=99,
    ).summaries
    for s in summaries:
        assert np.isfinite(s.mean_dfs_points)
        assert s.min_dfs_points <= s.mean_dfs_points <= s.max_dfs_points
        assert s.p05_dfs_points <= s.p50_dfs_points <= s.p95_dfs_points


def test_simulate_joint_samples_match_summaries() -> None:
    """Joint matrix per-driver stats must agree with the summarized row.

    Catches regressions where the matrix and the summaries drift out
    of alignment (e.g. someone reorders the matrix without updating
    ``_summarize``).
    """
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(6)]
    result = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=11,
    )
    for row_idx, summary in enumerate(result.summaries):
        row = result.joint_dfs_points[row_idx]
        assert summary.mean_dfs_points == pytest.approx(float(np.mean(row)))
        assert summary.std_dfs_points == pytest.approx(float(np.std(row)))
        assert summary.car_driver_id == int(result.driver_index[row_idx])

    # New per-target joint matrices must also align with their summaries.
    assert result.joint_finish_position is not None
    assert result.joint_total_fastest_laps is not None
    assert result.joint_total_laps_led is not None
    assert result.joint_is_dnf is not None
    for row_idx, summary in enumerate(result.summaries):
        fin_row = result.joint_finish_position[row_idx]
        fl_row = result.joint_total_fastest_laps[row_idx]
        ll_row = result.joint_total_laps_led[row_idx]
        assert summary.mean_r_finish_position == pytest.approx(float(np.mean(fin_row)))
        assert summary.std_r_finish_position == pytest.approx(float(np.std(fin_row)))
        assert summary.mean_r_total_fastest_laps == pytest.approx(float(np.mean(fl_row)))
        assert summary.mean_r_total_laps_led == pytest.approx(float(np.mean(ll_row)))


def test_dnf_pct_zero_when_no_p_dnf() -> None:
    """Backward-compat: with ``p_dnf=None`` on every driver, dnf_pct is
    exactly 0 and the joint_is_dnf matrix carries only zeros."""
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(6)]
    assert all(d.p_dnf is None for d in drivers)
    result = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=11,
    )
    for summary in result.summaries:
        assert summary.dnf_pct == 0.0
    assert result.joint_is_dnf is not None
    assert int(result.joint_is_dnf.sum()) == 0


def test_dnf_pct_approximates_p_dnf_with_large_n() -> None:
    """With ``p_dnf=0.25``, the empirical Bernoulli mean must approximate 0.25
    within a generous tolerance (large-n CLT)."""
    drivers = [
        MonteCarloDriverInput(
            car_driver_id=cid,
            r_start_position=10.0,
            predicted_targets={
                "r_finish_position": 8.0,
                "r_total_laps_led": 100.0,
                "r_total_fastest_laps": 10.0,
            },
            target_sigmas={
                "r_finish_position": 4.0,
                "r_total_laps_led": 10.0,
                "r_total_fastest_laps": 2.0,
            },
            p_dnf=0.25,
        )
        for cid in (101, 102, 103)
    ]
    result = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=10_000,
        seed=42,
    )
    for summary in result.summaries:
        # σ of Bernoulli(0.25) / sqrt(10000) ≈ 0.0043, so ±0.025 is a
        # very loose 5σ envelope and resistant to seed jitter.
        assert summary.dnf_pct == pytest.approx(0.25, abs=0.025)


def test_dnfed_secondary_targets_scaled_by_completion_fraction() -> None:
    """A DNFed driver's laps_led and fastest_laps samples must be scaled by
    the same per-iteration Uniform(0, 1) completion fraction.

    Both targets share the same ``c`` per (driver, iter), so when σ=0 on both
    secondaries and the predicted means differ, the ratio of the two sampled
    counts must equal the ratio of the predicted means (modulo integer
    rounding, which is bounded by ±1 / scaled_value).
    """
    predicted_ll = 1000.0  # large so the ±1 rounding noise is negligible
    predicted_fl = 200.0
    drivers = [
        MonteCarloDriverInput(
            car_driver_id=777,
            r_start_position=5.0,
            predicted_targets={
                "r_finish_position": 6.0,
                "r_total_laps_led": predicted_ll,
                "r_total_fastest_laps": predicted_fl,
            },
            target_sigmas={
                "r_finish_position": 0.0,
                "r_total_laps_led": 0.0,
                "r_total_fastest_laps": 0.0,
            },
            p_dnf=1.0,  # always DNF
        )
    ]
    result = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=7,
    )
    ll_row = result.joint_total_laps_led[0]
    fl_row = result.joint_total_fastest_laps[0]
    # Always DNF -> dnf_pct exactly 1.0.
    assert result.summaries[0].dnf_pct == pytest.approx(1.0)
    # All samples in [0, predicted_target] post-scaling + rounding.
    assert ll_row.min() >= 0
    assert ll_row.max() <= predicted_ll
    assert fl_row.min() >= 0
    assert fl_row.max() <= predicted_fl
    # The shared completion fraction means laps_led / predicted_ll ≈
    # fastest_laps / predicted_fl per iteration. Compare the empirical
    # completion-fraction distributions; both must approximate Uniform(0, 1)
    # mean 0.5 and look like the same draw.
    c_from_ll = ll_row / predicted_ll
    c_from_fl = fl_row / predicted_fl
    assert float(np.mean(c_from_ll)) == pytest.approx(0.5, abs=0.05)
    assert float(np.mean(c_from_fl)) == pytest.approx(0.5, abs=0.05)
    # Same per-iteration ``c`` ⇒ rounded ratios should agree closely.
    # With predicted_ll=1000 and predicted_fl=200, integer rounding adds at
    # most 1/200 ≈ 0.005 to fl-derived c, so ±0.02 is a comfortable envelope.
    assert float(np.mean(np.abs(c_from_ll - c_from_fl))) == pytest.approx(0.0, abs=0.02)


def test_dnfed_drivers_occupy_back_of_field() -> None:
    """With p_dnf=1.0 forced on a known subset, every iteration must place
    exactly those drivers in positions ``{n - k + 1, ..., n}``."""
    forced_dnf_ids = {1, 2, 3}  # the 3 lowest car_driver_ids always DNF
    drivers: list[MonteCarloDriverInput] = []
    for i in range(10):
        cid = i + 1
        drivers.append(
            MonteCarloDriverInput(
                car_driver_id=cid,
                r_start_position=float(cid),
                predicted_targets={
                    "r_finish_position": float(cid),
                    "r_total_laps_led": 20.0,
                    "r_total_fastest_laps": 3.0,
                },
                target_sigmas={
                    "r_finish_position": 2.0,
                    "r_total_laps_led": 5.0,
                    "r_total_fastest_laps": 1.0,
                },
                p_dnf=1.0 if cid in forced_dnf_ids else 0.0,
            )
        )
    n_drivers = len(drivers)
    k = len(forced_dnf_ids)
    result = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=99,
    )
    finishes = result.joint_finish_position  # (n_drivers, n_sims)
    is_dnf = result.joint_is_dnf
    # DNFed rows must always carry a position in the tail bucket.
    tail_positions = set(range(n_drivers - k + 1, n_drivers + 1))
    nondnf_positions = set(range(1, n_drivers - k + 1))
    for row_idx, driver in enumerate(drivers):
        if driver.car_driver_id in forced_dnf_ids:
            assert (is_dnf[row_idx] == 1).all()
            row_positions = set(int(v) for v in finishes[row_idx])
            assert row_positions.issubset(tail_positions)
        else:
            assert (is_dnf[row_idx] == 0).all()
            row_positions = set(int(v) for v in finishes[row_idx])
            assert row_positions.issubset(nondnf_positions)
    # Every iteration must still be a strict permutation of 1..n_drivers
    # (permutation invariant preserved by the random-tail placement).
    expected_sorted = sorted(range(1, n_drivers + 1))
    for s in range(finishes.shape[1]):
        assert sorted(int(v) for v in finishes[:, s]) == expected_sorted
