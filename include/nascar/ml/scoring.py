"""Compute DFS fantasy points from agg_week_results rows and dfs_scoring_rules JSON."""

from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

Number = int | float | Decimal | None

DERIVED_PLACE_DIFF_SOURCE = "r_place_diff"


def _as_float(value: Number) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _component_has_place_diff_fallback(component: Mapping[str, Any]) -> bool:
    if component.get("source") != DERIVED_PLACE_DIFF_SOURCE:
        return False
    fallback = component.get("fallback") or {}
    return fallback.get("op") == "subtract" and fallback.get("from") == [
        "r_start_position",
        "r_finish_position",
    ]


def target_keys_for_rules(scoring_rules: Mapping[str, Any] | str) -> list[str]:
    """
    Stat columns the model should predict for these scoring rules.

    Excludes ``r_place_diff`` when rules derive it from start/finish at scoring time.
    """
    if isinstance(scoring_rules, str):
        rules = json.loads(scoring_rules)
    else:
        rules = scoring_rules

    keys: list[str] = []
    seen: set[str] = set()
    for component in rules.get("components") or []:
        source = component.get("source")
        if not source or not isinstance(source, str):
            continue
        if source == DERIVED_PLACE_DIFF_SOURCE and _component_has_place_diff_fallback(component):
            continue
        if source not in seen:
            seen.add(source)
            keys.append(source)
    return keys


def targets_from_agg_row(
    agg_row: Mapping[str, Any],
    scoring_rules: Mapping[str, Any] | str,
) -> dict[str, float]:
    """Extract ground-truth label stats for each trainable scoring-rule source field."""
    keys = target_keys_for_rules(scoring_rules)
    targets: dict[str, float] = {}
    for key in keys:
        value = _as_float(agg_row.get(key))
        if value is not None:
            targets[key] = value
    return targets


def build_scoring_row(
    start_position: Number,
    targets: Mapping[str, Number],
    *,
    scoring_rules: Mapping[str, Any] | str | None = None,
) -> dict[str, Any]:
    """
    Build a row dict for ``compute_dfs_points`` from known start + predicted/actual targets.

    When ``scoring_rules`` is provided, any derived-only ``r_place_diff`` is omitted from
    ``targets`` and resolved inside ``compute_dfs_points`` via start/finish fallback.
    """
    row: dict[str, Any] = dict(targets)
    if start_position is not None:
        row["r_start_position"] = start_position

    if scoring_rules is not None:
        if isinstance(scoring_rules, str):
            rules = json.loads(scoring_rules)
        else:
            rules = scoring_rules
        for component in rules.get("components") or []:
            if _component_has_place_diff_fallback(component):
                row.pop(DERIVED_PLACE_DIFF_SOURCE, None)

    return row


def compute_dfs_points_from_targets(
    start_position: Number,
    targets: Mapping[str, Number],
    scoring_rules: Mapping[str, Any] | str,
) -> tuple[Decimal, dict[str, float]]:
    """Apply scoring rules to predicted/actual stat targets plus pre-race start position."""
    row = build_scoring_row(start_position, targets, scoring_rules=scoring_rules)
    return compute_dfs_points(row, scoring_rules)


def _resolve_source(row: Mapping[str, Any], component: Mapping[str, Any]) -> float | None:
    source = component.get("source")
    if source and source in row:
        return _as_float(row[source])

    fallback = component.get("fallback")
    if not fallback or fallback.get("op") != "subtract":
        return None

    fields = fallback.get("from") or []
    if len(fields) != 2:
        return None

    left = _as_float(row.get(fields[0]))
    right = _as_float(row.get(fields[1]))
    if left is None or right is None:
        return None
    return left - right


def _finish_points(finish: float | None, component: Mapping[str, Any]) -> float:
    if finish is None:
        return 0.0

    position = int(finish)
    table = component.get("table") or {}
    if str(position) in table:
        return float(table[str(position)])
    if position in table:
        return float(table[position])

    formula = component.get("formula") or {}
    if formula.get("subtract_position"):
        base = float(formula.get("base", 0))
        points = base - position
        minimum = float(formula.get("min", 0))
        return max(minimum, points)

    return 0.0


def _linear_points(value: float | None, component: Mapping[str, Any]) -> float:
    if value is None:
        return 0.0
    multiplier = float(component.get("multiplier", 1.0))
    return value * multiplier


def _component_points(row: Mapping[str, Any], component: Mapping[str, Any]) -> float:
    component_type = component.get("type")
    if component_type == "finish_table":
        finish = _resolve_source(row, component) or _as_float(row.get("r_finish_position"))
        return _finish_points(finish, component)

    if component_type == "linear":
        value = _resolve_source(row, component)
        return _linear_points(value, component)

    return 0.0


def compute_dfs_points(
    agg_row: Mapping[str, Any],
    scoring_rules: Mapping[str, Any] | str,
) -> tuple[Decimal, dict[str, float]]:
    """
    Return total DFS points and per-component breakdown for one stats row.

    ``agg_row`` may be a full ``agg_week_results`` row or a row built via ``build_scoring_row``.
    ``scoring_rules`` is the ``dfs_scoring_rules.rules`` JSON object (or JSON string).
    """
    if isinstance(scoring_rules, str):
        rules = json.loads(scoring_rules)
    else:
        rules = scoring_rules

    breakdown: dict[str, float] = {}
    total = 0.0

    for component in rules.get("components") or []:
        name = str(component.get("name", "unknown"))
        points = _component_points(agg_row, component)
        breakdown[name] = round(points, 3)
        total += points

    return Decimal(str(round(total, 3))), breakdown
