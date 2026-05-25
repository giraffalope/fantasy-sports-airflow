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
    summaries = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=42,
    )
    assert [s.car_driver_id for s in summaries] == [d.car_driver_id for d in drivers]
    assert all(s.n_simulations == MIN_SIMULATIONS for s in summaries)


def test_simulate_percentiles_are_ordered_for_each_driver() -> None:
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(5)]
    summaries = simulate_driver_distributions(
        drivers,
        scoring_rules=_DK_RULES_JSON,
        n_simulations=MIN_SIMULATIONS,
        seed=7,
    )
    for s in summaries:
        assert s.min_dfs_points <= s.p05_dfs_points <= s.p50_dfs_points <= s.p95_dfs_points
        assert s.p95_dfs_points <= s.max_dfs_points
        assert s.std_dfs_points >= 0.0


def test_simulate_is_deterministic_given_seed() -> None:
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(8)]
    a = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=123
    )
    b = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=123
    )
    for left, right in zip(a, b, strict=True):
        assert left == right


def test_simulate_changes_with_different_seed() -> None:
    drivers = [_driver(i + 1, finish=float(i + 1)) for i in range(8)]
    a = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=1
    )
    b = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=2
    )
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
    )
    assert summaries[0].std_dfs_points == pytest.approx(0.0)
    assert summaries[0].min_dfs_points == summaries[0].max_dfs_points


def test_simulate_finish_position_stays_inside_field() -> None:
    """The simulator must never re-score with finish positions outside ``[1, field_size]``."""
    drivers = [_driver(1, finish=1.0, sigma_finish=20.0, sigma_laps_led=0.0, sigma_fastest_laps=0.0)]
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
            field_size=40,
            seed=5,
        )

    assert captured, "expected scoring to be called at least once"
    assert all(1.0 <= finish <= 40.0 for finish in captured)
    assert all(float(finish).is_integer() for finish in captured)


def test_simulate_empty_drivers_returns_empty_list() -> None:
    assert (
        simulate_driver_distributions(
            [],
            scoring_rules=_DK_RULES_JSON,
            n_simulations=MIN_SIMULATIONS,
            seed=0,
        )
        == []
    )


def test_simulate_rejects_zero_field_size() -> None:
    with pytest.raises(ValueError):
        simulate_driver_distributions(
            [_driver(1)],
            scoring_rules=_DK_RULES_JSON,
            n_simulations=MIN_SIMULATIONS,
            field_size=0,
            seed=1,
        )


def test_summary_mean_matches_raw_samples() -> None:
    """Cross-check the aggregated mean against a manual numpy reproduction with the same seed."""
    drivers = [_driver(1, finish=10.0)]
    summary = simulate_driver_distributions(
        drivers, scoring_rules=_DK_RULES_JSON, n_simulations=MIN_SIMULATIONS, seed=99
    )[0]
    rng = np.random.default_rng(99)
    samples = np.empty(MIN_SIMULATIONS)
    real_compute = (
        __import__("include.nascar.ml.monte_carlo", fromlist=["compute_dfs_points_from_targets"]).compute_dfs_points_from_targets
    )
    for i in range(MIN_SIMULATIONS):
        perturbed = {}
        for target, mean in drivers[0].predicted_targets.items():
            sigma = drivers[0].target_sigmas[target]
            raw = float(mean) + rng.normal(0.0, sigma)
            if target == "r_finish_position":
                clipped = max(1, min(len(drivers), int(round(raw))))
                perturbed[target] = float(clipped)
            else:
                perturbed[target] = float(max(0, int(round(raw))))
        total, _ = real_compute(drivers[0].r_start_position, perturbed, _DK_RULES_JSON)
        samples[i] = float(total)
    assert summary.mean_dfs_points == pytest.approx(float(samples.mean()), abs=1e-9)
