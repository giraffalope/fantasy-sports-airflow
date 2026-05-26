"""Unit tests for DFS scoring from agg_week_results."""

from __future__ import annotations

import json

import pytest

from include.nascar.ml.scoring import (
    MissingStartPositionError,
    build_scoring_row,
    compute_dfs_points,
    compute_dfs_points_from_targets,
    target_keys_for_rules,
    targets_from_agg_row,
)

_FANDUEL_RULES = {
    "version": 1,
    "components": [
        {
            "name": "finish",
            "source": "r_finish_position",
            "type": "finish_table",
            "table": {"1": 43, "2": 40},
            "formula": {"base": 41, "subtract_position": True, "min": 0},
        },
        {
            "name": "place_diff",
            "source": "r_place_diff",
            "fallback": {
                "from": ["r_start_position", "r_finish_position"],
                "op": "subtract",
            },
            "type": "linear",
            "multiplier": 1.0,
        },
        {
            "name": "laps",
            "source": "r_total_laps_completed",
            "type": "linear",
            "multiplier": 0.1,
        },
        {
            "name": "laps_led",
            "source": "r_total_laps_led",
            "type": "linear",
            "multiplier": 0.1,
        },
    ],
}


def test_fanduel_winner_totals_match_components() -> None:
    """Ross Chastain 2023-53: 43 + 7 + 31.2 + 15.7 = 96.9."""
    row = {
        "r_start_position": 8,
        "r_finish_position": 1,
        "r_place_diff": 7,
        "r_total_laps_completed": 312,
        "r_total_laps_led": 157,
    }
    total, breakdown = compute_dfs_points(row, _FANDUEL_RULES)

    assert breakdown["finish"] == 43.0
    assert breakdown["place_diff"] == 7.0
    assert breakdown["laps"] == 31.2
    assert breakdown["laps_led"] == 15.7
    assert float(total) == 96.9


def test_place_diff_fallback_from_start_finish() -> None:
    row = {
        "r_start_position": 4,
        "r_finish_position": 3,
        "r_total_laps_completed": 312,
        "r_total_laps_led": 0,
    }
    total, breakdown = compute_dfs_points(row, _FANDUEL_RULES)

    assert breakdown["place_diff"] == 1.0
    assert breakdown["finish"] == 38.0
    assert float(total) == 70.2


_DRAFTKINGS_RULES = {
    "version": 1,
    "components": [
        {
            "name": "finish",
            "source": "r_finish_position",
            "type": "finish_table",
            "table": {"1": 45, "2": 42, "3": 41, "4": 40},
            "formula": {"base": 41, "subtract_position": True, "min": 0},
        },
        {
            "name": "place_diff",
            "source": "r_place_diff",
            "fallback": {
                "from": ["r_start_position", "r_finish_position"],
                "op": "subtract",
            },
            "type": "linear",
            "multiplier": 1.0,
        },
        {
            "name": "fastest_laps",
            "source": "r_total_fastest_laps",
            "type": "linear",
            "multiplier": 0.45,
        },
        {
            "name": "laps_led",
            "source": "r_total_laps_led",
            "type": "linear",
            "multiplier": 0.25,
        },
    ],
}


def test_draftkings_winner_totals_match_components() -> None:
    """Ross Chastain 2023-53 DraftKings: 45 + 7 + 23.4 + 39.25 = 114.65."""
    row = {
        "r_start_position": 8,
        "r_finish_position": 1,
        "r_place_diff": 7,
        "r_total_fastest_laps": 52,
        "r_total_laps_led": 157,
    }
    total, breakdown = compute_dfs_points(row, _DRAFTKINGS_RULES)

    assert breakdown["finish"] == 45.0
    assert breakdown["place_diff"] == 7.0
    assert breakdown["fastest_laps"] == 23.4
    assert breakdown["laps_led"] == 39.25
    assert float(total) == 114.65


def test_draftkings_midpack_finish_formula() -> None:
    """Kyle Larson 3rd: finish 41, place_diff 1, fastest_laps 1.8."""
    row = {
        "r_start_position": 4,
        "r_finish_position": 3,
        "r_place_diff": 1,
        "r_total_fastest_laps": 4,
        "r_total_laps_led": 0,
    }
    total, breakdown = compute_dfs_points(row, _DRAFTKINGS_RULES)

    assert breakdown["finish"] == 41.0
    assert breakdown["place_diff"] == 1.0
    assert breakdown["fastest_laps"] == 1.8
    assert float(total) == 43.8


def test_rules_json_string() -> None:
    row = {
        "r_finish_position": 2,
        "r_place_diff": 13,
        "r_total_laps_completed": 312,
        "r_total_laps_led": 2,
    }
    total, _ = compute_dfs_points(row, json.dumps(_FANDUEL_RULES))
    assert float(total) == 84.4


def test_draftkings_target_keys_exclude_place_diff() -> None:
    keys = target_keys_for_rules(_DRAFTKINGS_RULES)
    assert keys == [
        "r_finish_position",
        "r_total_fastest_laps",
        "r_total_laps_led",
    ]


def test_targets_from_agg_row_matches_draftkings_keys() -> None:
    agg = {
        "r_start_position": 8,
        "r_finish_position": 1,
        "r_place_diff": 7,
        "r_total_fastest_laps": 52,
        "r_total_laps_led": 157,
    }
    targets = targets_from_agg_row(agg, _DRAFTKINGS_RULES)
    assert set(targets.keys()) == set(target_keys_for_rules(_DRAFTKINGS_RULES))
    assert targets["r_finish_position"] == 1.0
    assert targets["r_total_fastest_laps"] == 52.0
    assert targets["r_total_laps_led"] == 157.0
    assert "r_place_diff" not in targets


def test_predicted_stats_plus_start_reproduce_draftkings_total() -> None:
    """Model path: predicted targets + known start → same 114.65 as full row."""
    agg = {
        "r_start_position": 8,
        "r_finish_position": 1,
        "r_total_fastest_laps": 52,
        "r_total_laps_led": 157,
    }
    targets = targets_from_agg_row(agg, _DRAFTKINGS_RULES)
    total, breakdown = compute_dfs_points_from_targets(
        agg["r_start_position"],
        targets,
        _DRAFTKINGS_RULES,
    )
    assert float(total) == 114.65
    assert breakdown["place_diff"] == 7.0

    scoring_row = build_scoring_row(
        agg["r_start_position"],
        targets,
        scoring_rules=_DRAFTKINGS_RULES,
    )
    assert "r_place_diff" not in scoring_row
    assert scoring_row["r_start_position"] == 8
    total2, _ = compute_dfs_points(scoring_row, _DRAFTKINGS_RULES)
    assert float(total2) == 114.65


def test_b4_place_diff_requires_start_position() -> None:
    """B4: rules deriving ``r_place_diff`` must raise when start_position is None."""
    targets = {
        "r_finish_position": 1.0,
        "r_total_fastest_laps": 52.0,
        "r_total_laps_led": 157.0,
    }
    with pytest.raises(MissingStartPositionError):
        compute_dfs_points_from_targets(None, targets, _DRAFTKINGS_RULES)


def test_b4_user_supplied_place_diff_does_not_bypass_assertion() -> None:
    """``build_scoring_row`` strips ``r_place_diff`` when rules derive it,
    so passing it directly does NOT let the caller skip ``start_position``."""
    targets = {
        "r_finish_position": 1.0,
        "r_place_diff": 7.0,
        "r_total_fastest_laps": 52.0,
        "r_total_laps_led": 157.0,
    }
    with pytest.raises(MissingStartPositionError):
        compute_dfs_points_from_targets(None, targets, _DRAFTKINGS_RULES)


def test_b4_with_start_position_provides_place_diff_via_fallback() -> None:
    """B4 assertion is a guard, not a behaviour change — happy path still works."""
    targets = {
        "r_finish_position": 1.0,
        "r_total_fastest_laps": 52.0,
        "r_total_laps_led": 157.0,
    }
    total, breakdown = compute_dfs_points_from_targets(
        8, targets, _DRAFTKINGS_RULES
    )
    # finish=45, place_diff=7 (8-1), fastest=23.4, laps_led=39.25 = 114.65
    assert float(total) == 114.65
    assert breakdown["place_diff"] == 7.0


def test_b4_rules_without_place_diff_allow_none_start_position() -> None:
    """When the active rules don't use r_place_diff, the assertion is silent."""
    no_place_diff_rules = {
        "version": 1,
        "components": [
            {
                "name": "finish",
                "source": "r_finish_position",
                "type": "finish_table",
                "table": {"1": 45},
                "formula": {"base": 41, "subtract_position": True, "min": 0},
            },
        ],
    }
    total, breakdown = compute_dfs_points_from_targets(
        None,
        {"r_finish_position": 1.0},
        no_place_diff_rules,
    )
    assert breakdown["finish"] == 45.0
    assert float(total) == 45.0
