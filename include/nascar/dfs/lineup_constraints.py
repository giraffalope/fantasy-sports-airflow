"""Shared lineup overlap and driver-diff constraints for DFS portfolio selection."""

from __future__ import annotations

from typing import Sequence


def driver_overlap(indices_a: tuple[int, ...], indices_b: tuple[int, ...]) -> int:
    """Count drivers shared between two lineups."""
    return len(set(indices_a) & set(indices_b))


def min_driver_diff_ok(
    candidate: tuple[int, ...],
    accepted_indices: Sequence[tuple[int, ...]],
    *,
    min_diff: int,
    lineup_size: int,
) -> bool:
    """Return True when ``candidate`` differs from every accepted lineup by at least ``min_diff`` drivers."""
    if not accepted_indices:
        return True
    max_shared = lineup_size - min_diff
    return all(
        driver_overlap(candidate, other) <= max_shared for other in accepted_indices
    )
