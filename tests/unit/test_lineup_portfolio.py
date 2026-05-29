"""Unit tests for the portfolio-aware lineup scoring and diversifier."""

from __future__ import annotations

import numpy as np
import pytest

from include.nascar.dfs.lineup_portfolio import (
    JOINT_METRICS,
    CandidateScore,
    dk_write_max_lineup_correlation,
    dk_write_quality_floor_enabled,
    greedy_diversify,
    merge_scored_candidates,
    objective_value,
    score_candidates_against_joint_samples,
)


# ---------------------------------------------------------------------------
# score_candidates_against_joint_samples
# ---------------------------------------------------------------------------


def test_score_against_joint_samples_recovers_independent_mean() -> None:
    """Independent Gaussian samples: lineup mean equals sum of driver means.

    With independent samples the joint matrix is exchangeable across
    rows so summing the rows reduces to summing the per-driver mean —
    same identity the legacy ``sum(p50)`` objective relied on. This
    test catches regressions in the per-iteration sum / mean math.
    """
    rng = np.random.default_rng(42)
    driver_means = np.array([30.0, 28.0, 26.0, 24.0, 22.0, 20.0])
    n_simulations = 2_000
    joint = rng.normal(driver_means[:, None], 2.0, size=(6, n_simulations))
    scores = score_candidates_against_joint_samples([[0, 1, 2, 3, 4, 5]], joint)

    assert len(scores) == 1
    score = scores[0]
    expected_mean = float(np.sum(driver_means))
    assert score.mean == pytest.approx(expected_mean, abs=0.5)


def test_score_against_joint_samples_captures_negative_correlation() -> None:
    """Anti-correlated samples drop the lineup std below the independent baseline.

    Construct a tiny 2-driver joint matrix where row 1 is the negative of
    row 0 (perfect anti-correlation, as if the two drivers were always
    finishing on opposite ends of the field). The summed lineup should
    have ~zero variance — vastly smaller than sqrt(sigma0^2 + sigma1^2).
    This is the smoking-gun test that proves joint scoring captures
    rank-coupling that scalar summaries throw away.
    """
    n_simulations = 1_000
    rng = np.random.default_rng(7)
    row0 = rng.normal(50.0, 5.0, size=n_simulations)
    row1 = 100.0 - row0  # perfect anti-correlation, mean ~50
    joint = np.stack([row0, row1])

    scores = score_candidates_against_joint_samples([[0, 1]], joint)
    assert len(scores) == 1
    score = scores[0]
    # With perfect anti-correlation, lineup = row0 + (100 - row0) = 100,
    # so std collapses to ~0.
    assert score.std < 1e-9
    # Sum of independent stds would have been sqrt(5^2 + 5^2) ~= 7.07,
    # so the test is meaningful — joint scoring really does shrink the
    # lineup variance dramatically here.
    assert score.mean == pytest.approx(100.0, abs=0.01)


def test_score_against_joint_samples_skips_empty_candidates() -> None:
    """Defensive: empty candidate lists are dropped, not raised on."""
    joint = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    scores = score_candidates_against_joint_samples([[], [0, 1]], joint)
    assert len(scores) == 1


def test_score_against_joint_samples_rejects_non_2d() -> None:
    with pytest.raises(ValueError):
        score_candidates_against_joint_samples([[0]], np.zeros(5))


def test_score_against_joint_samples_returns_empty_on_empty_matrix() -> None:
    assert (
        score_candidates_against_joint_samples([[0]], np.zeros((0, 0))) == []
    )


def test_score_stores_lineup_totals_on_candidate() -> None:
    joint = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    scores = score_candidates_against_joint_samples([[0, 1]], joint)
    assert scores[0].lineup_totals is not None
    np.testing.assert_allclose(scores[0].lineup_totals, [5.0, 7.0, 9.0])


# ---------------------------------------------------------------------------
# objective_value
# ---------------------------------------------------------------------------


def _score(
    *,
    indices: tuple[int, ...] = (0, 1),
    mean: float = 100.0,
    std: float = 10.0,
    p10: float = 80.0,
    p50: float = 100.0,
    p90: float = 120.0,
    sharpe: float = 10.0,
) -> CandidateScore:
    return CandidateScore(
        indices=indices,
        mean=mean,
        std=std,
        p10=p10,
        p50=p50,
        p90=p90,
        sharpe=sharpe,
    )


@pytest.mark.parametrize(
    "metric, expected_field, expected",
    [
        ("joint_p10", "p10", 80.0),
        ("joint_p50", "p50", 100.0),
        ("joint_p90", "p90", 120.0),
        ("joint_mean", "mean", 100.0),
        ("joint_sharpe", "sharpe", 10.0),
    ],
)
def test_objective_value_routes_metric_to_field(
    metric: str, expected_field: str, expected: float
) -> None:
    """Every joint metric token must read the right :class:`CandidateScore` field."""
    score = _score()
    assert objective_value(score, metric) == expected
    assert metric in JOINT_METRICS  # token surface stays in sync


def test_dk_write_env_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FS_DK_WRITE_QUALITY_FLOOR", "0")
    assert dk_write_quality_floor_enabled() is False
    monkeypatch.setenv("FS_DK_WRITE_MAX_LINEUP_CORR", "0.5")
    assert dk_write_max_lineup_correlation() == pytest.approx(0.5)


def test_objective_value_falls_back_to_median_for_unknown_metric() -> None:
    """Unknown metric token doesn't crash; falls back to ``p50`` (audit log warns)."""
    score = _score()
    assert objective_value(score, "unrecognized_token") == score.p50


# ---------------------------------------------------------------------------
# greedy_diversify
# ---------------------------------------------------------------------------


def test_greedy_diversify_respects_exposure_cap() -> None:
    """Cap holds when the pool offers admissible alternatives.

    Half the pool contains driver 0 (with high objective values so it
    sorts to the top), half contains a fully disjoint driver set. Cap
    at 0.40 with target_count = 10 admits at most ceil(10 * 0.40) = 4
    appearances of driver 0; the remaining slots come from the disjoint
    half of the pool, so the cap does NOT need to relax.
    """
    high_value_driver0 = [
        _score(indices=(0, 100 + i), p10=200.0 - i, p90=300.0 - i)
        for i in range(8)
    ]
    disjoint_alternatives = [
        _score(indices=(50 + i, 60 + i), p10=180.0 - i, p90=280.0 - i)
        for i in range(8)
    ]
    candidates = high_value_driver0 + disjoint_alternatives
    selected = greedy_diversify(
        candidates,
        target_count=10,
        exposure_cap=0.40,
        objective_metric="joint_p10",
        use_quality_floor=False,
    )
    assert len(selected) == 10
    appearances = sum(1 for s in selected if 0 in s.indices)
    assert appearances <= 4


def test_greedy_diversify_orders_by_objective_metric() -> None:
    """Output preserves objective-descending order from the first pass."""
    pool = [
        _score(indices=(0, i + 10), p10=float(i), p90=200.0 - float(i))
        for i in range(5)
    ]
    selected_floor = greedy_diversify(
        pool,
        target_count=3,
        exposure_cap=1.0,  # no diversification pressure
        objective_metric="joint_p10",
        use_quality_floor=False,
    )
    # Pool entry with the largest p10 should win the top slot.
    assert selected_floor[0].p10 == pytest.approx(4.0)
    assert selected_floor[1].p10 == pytest.approx(3.0)
    assert selected_floor[2].p10 == pytest.approx(2.0)

    selected_ceiling = greedy_diversify(
        pool,
        target_count=3,
        exposure_cap=1.0,
        objective_metric="joint_p90",
        use_quality_floor=False,
    )
    # Highest p90 corresponds to i=0 (because p90 = 200 - i).
    assert selected_ceiling[0].p90 == pytest.approx(200.0)


def test_greedy_diversify_relaxes_cap_when_pool_starved(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Tight cap that can't fill: relax automatically with a warning."""
    candidates = [
        _score(indices=(0, 1, 2, 3, 4, 5 + i), p10=float(100 - i), p90=float(150 - i))
        for i in range(4)
    ]
    with caplog.at_level("WARNING"):
        selected = greedy_diversify(
            candidates,
            target_count=4,
            exposure_cap=0.20,  # ceil(4 * 0.20) = 1, but driver 0 is in every candidate
            objective_metric="joint_p10",
            use_quality_floor=False,
        )
    assert len(selected) == 4
    relax_logs = [r for r in caplog.records if "relaxing exposure cap" in r.message]
    assert relax_logs, "expected at least one relaxation warning"


def test_greedy_diversify_returns_empty_when_target_is_zero() -> None:
    candidates = [_score(indices=(0, 1))]
    assert greedy_diversify(
        candidates,
        target_count=0,
        exposure_cap=1.0,
        objective_metric="joint_p10",
    ) == []


def test_greedy_diversify_returns_empty_on_empty_pool() -> None:
    assert greedy_diversify(
        [],
        target_count=5,
        exposure_cap=0.5,
        objective_metric="joint_p10",
    ) == []


def test_greedy_diversify_handles_duplicate_candidates() -> None:
    """Two identical candidates may both come back from CP-SAT under min_diff=1.

    The selector must not double-count one through the exposure budget;
    the second copy should be skipped because the drivers have already
    consumed budget from the first copy.
    """
    duplicate = _score(indices=(0, 1, 2), p10=100.0, p90=200.0)
    candidates = [
        duplicate,
        duplicate,
        _score(indices=(3, 4, 5), p10=90.0, p90=190.0),
    ]
    selected = greedy_diversify(
        candidates,
        target_count=3,
        exposure_cap=0.40,  # ceil(3 * 0.4) = 2 — duplicate would otherwise burn both budget slots
        objective_metric="joint_p10",
        use_quality_floor=False,
    )
    # Both unique lineups should fit under the cap; the duplicate should
    # be deduplicated by the seen-indices ledger.
    selected_index_sets = [s.indices for s in selected]
    assert duplicate.indices in selected_index_sets
    assert (3, 4, 5) in selected_index_sets


def test_greedy_diversify_enforces_min_driver_diff() -> None:
    """Lineups sharing too many drivers are skipped when min_driver_diff is set."""
    similar = [
        _score(indices=(0, 1, 2, 3, 4, 5), p10=200.0),
        _score(indices=(0, 1, 2, 3, 4, 6), p10=199.0),
        _score(indices=(10, 11, 12, 13, 14, 15), p10=150.0),
        _score(indices=(20, 21, 22, 23, 24, 25), p10=140.0),
    ]
    selected = greedy_diversify(
        similar,
        target_count=2,
        exposure_cap=1.0,
        objective_metric="joint_p10",
        min_driver_diff=3,
        lineup_size=6,
        relax_min_driver_diff=False,
        use_quality_floor=False,
    )
    assert len(selected) == 2
    assert selected[0].indices == (0, 1, 2, 3, 4, 5)
    assert selected[1].indices == (10, 11, 12, 13, 14, 15)


def test_merge_scored_candidates_keeps_higher_p50() -> None:
    low = _score(indices=(0, 1), p50=90.0)
    high = _score(indices=(0, 1), p50=110.0)
    merged = merge_scored_candidates([low], [high])
    assert len(merged) == 1
    assert merged[0].p50 == pytest.approx(110.0)


def test_greedy_diversify_quality_floor_skips_tail(
    caplog: pytest.LogCaptureFixture,
) -> None:
    pool = [
        _score(indices=(0, 1, 2, 3, 4, 5), p10=200.0),
        _score(indices=(10, 11, 12, 13, 14, 15), p10=160.0),
        _score(indices=(20, 21, 22, 23, 24, 25), p10=150.0),
        _score(indices=(30, 31, 32, 33, 34, 35), p10=50.0),
    ]
    selected = greedy_diversify(
        pool,
        target_count=2,
        exposure_cap=1.0,
        objective_metric="joint_p10",
        min_driver_diff=1,
        lineup_size=6,
        use_quality_floor=True,
        max_lineup_correlation=1.0,
        relax_min_driver_diff=False,
    )
    assert len(selected) == 2
    assert all(s.p10 >= 155.0 for s in selected)
    assert all(s.p10 >= 150.0 for s in selected)
    assert not any(s.p10 < 100.0 for s in selected)


def test_greedy_diversify_rejects_high_correlation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rng = np.random.default_rng(0)
    totals_a = rng.normal(100.0, 5.0, size=500)
    totals_b = totals_a + rng.normal(0.0, 0.01, size=500)
    pool = [
        CandidateScore(
            indices=(0, 1, 2, 3, 4, 5),
            mean=100.0,
            std=5.0,
            p10=90.0,
            p50=100.0,
            p90=110.0,
            sharpe=20.0,
            lineup_totals=totals_a,
        ),
        CandidateScore(
            indices=(0, 1, 2, 3, 4, 6),
            mean=99.0,
            std=5.0,
            p10=89.0,
            p50=99.0,
            p90=109.0,
            sharpe=19.0,
            lineup_totals=totals_b,
        ),
        CandidateScore(
            indices=(10, 11, 12, 13, 14, 15),
            mean=80.0,
            std=8.0,
            p10=70.0,
            p50=80.0,
            p90=90.0,
            sharpe=10.0,
            lineup_totals=rng.normal(80.0, 8.0, size=500),
        ),
    ]
    selected = greedy_diversify(
        pool,
        target_count=2,
        exposure_cap=1.0,
        objective_metric="joint_p50",
        min_driver_diff=1,
        lineup_size=6,
        use_quality_floor=False,
        max_lineup_correlation=0.85,
        relax_min_driver_diff=False,
    )
    assert len(selected) == 2
    assert selected[0].indices == (0, 1, 2, 3, 4, 5)
    assert selected[1].indices == (10, 11, 12, 13, 14, 15)


def test_greedy_diversify_relaxes_min_driver_diff_when_starved(
    caplog: pytest.LogCaptureFixture,
) -> None:
    candidates = [
        _score(indices=(0, 1, 2, 3, 4, 5), p10=200.0),
        _score(indices=(0, 1, 2, 3, 4, 6), p10=199.0),
        _score(indices=(0, 1, 2, 3, 4, 7), p10=198.0),
    ]
    with caplog.at_level("WARNING"):
        selected = greedy_diversify(
            candidates,
            target_count=3,
            exposure_cap=1.0,
            objective_metric="joint_p10",
            min_driver_diff=3,
            lineup_size=6,
            use_quality_floor=False,
        )
    assert len(selected) == 3
    relax_logs = [
        r for r in caplog.records if "relaxing min_driver_diff" in r.message
    ]
    assert relax_logs
