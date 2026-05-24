"""Unit tests for DFS lineup optimizer."""

from __future__ import annotations

from include.nascar.dfs.lineup_optimizer import LineupPlayer, generate_top_k_lineups


def _pool() -> list[LineupPlayer]:
    return [
        LineupPlayer(1, 10000, 50.0),
        LineupPlayer(2, 9000, 48.0),
        LineupPlayer(3, 8000, 46.0),
        LineupPlayer(4, 7000, 44.0),
        LineupPlayer(5, 6000, 42.0),
        LineupPlayer(6, 5000, 40.0),
        LineupPlayer(7, 4000, 30.0),
    ]


def test_generate_lineup_respects_cap_and_size() -> None:
    lineups = generate_top_k_lineups(
        _pool(),
        lineup_size=3,
        salary_cap=20000,
        top_k=1,
        min_diff=1,
    )
    assert len(lineups) == 1
    picked = lineups[0]
    assert len(picked) == 3
    salaries = sum(_pool()[i].salary for i in picked)
    assert salaries <= 20000


def test_min_diff_two_forces_more_diversity_than_one() -> None:
    players = _pool()
    one = generate_top_k_lineups(
        players, lineup_size=3, salary_cap=50000, top_k=2, min_diff=1
    )
    two = generate_top_k_lineups(
        players, lineup_size=3, salary_cap=50000, top_k=2, min_diff=2
    )
    assert len(one) >= 2
    assert len(two) >= 2
    overlap_one = len(set(one[0]) & set(one[1]))
    overlap_two = len(set(two[0]) & set(two[1]))
    assert overlap_one >= overlap_two
