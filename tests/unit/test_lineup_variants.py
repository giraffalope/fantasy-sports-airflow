"""Unit tests for salary-feasible lineup variant generation."""

from __future__ import annotations

import numpy as np
import pytest

from include.nascar.dfs.lineup_variants import (
    dk_variant_pool_cap,
    dk_variant_seed_count,
    generate_lineup_variants,
)


def _salaries_for_six() -> np.ndarray:
    return np.array([10_000, 9000, 8000, 7000, 6000, 5000, 4000, 3000], dtype=np.int64)


def _salaries_for_ten() -> np.ndarray:
    return np.array(
        [10_000, 9000, 8000, 7000, 6000, 5000, 4500, 4000, 3500, 3000],
        dtype=np.int64,
    )


def test_single_swap_respects_salary_cap() -> None:
    salaries = _salaries_for_six()
    seed = [0, 1, 2, 3, 4, 5]
    variants = generate_lineup_variants(
        salaries,
        [seed],
        lineup_size=6,
        salary_cap=50_000,
        max_variants=50,
        max_swaps=1,
    )
    assert variants
    for lineup in variants:
        assert len(lineup) == 6
        assert int(np.sum(salaries[lineup])) <= 50_000
        assert tuple(sorted(lineup)) != tuple(sorted(seed))


def test_three_swap_variant_supports_min_driver_diff_three() -> None:
    salaries = _salaries_for_ten()
    seed = [0, 1, 2, 3, 4, 5]
    variants = generate_lineup_variants(
        salaries,
        [seed],
        lineup_size=6,
        salary_cap=50_000,
        max_variants=200,
        max_swaps=3,
    )
    assert variants
    seed_set = set(seed)
    assert any(len(seed_set & set(lineup)) <= 3 for lineup in variants)


def test_duplicate_seed_skipped() -> None:
    salaries = _salaries_for_six()
    seed = [0, 1, 2, 3, 4, 5]
    variants = generate_lineup_variants(
        salaries,
        [seed, seed],
        lineup_size=6,
        salary_cap=50_000,
        max_variants=20,
        max_swaps=1,
    )
    assert variants


def test_too_few_outside_drivers_for_swap_count() -> None:
    """Seven drivers: ``max_swaps=2`` needs two outsiders; only one exists."""
    salaries = np.array([9000, 8000, 7000, 6000, 5000, 4000, 3000], dtype=np.int64)
    seed = [0, 1, 2, 3, 4, 5]
    assert (
        generate_lineup_variants(
            salaries,
            [seed],
            lineup_size=6,
            salary_cap=50_000,
            max_variants=50,
            max_swaps=2,
        )
        == []
    )
    one_swap = generate_lineup_variants(
        salaries,
        [seed],
        lineup_size=6,
        salary_cap=50_000,
        max_variants=50,
        max_swaps=1,
    )
    assert one_swap
    assert all(len(v) == 6 for v in one_swap)


def test_invalid_seed_size_skipped() -> None:
    salaries = _salaries_for_six()
    assert (
        generate_lineup_variants(
            salaries,
            [[0, 1, 2]],
            lineup_size=6,
            salary_cap=50_000,
            max_variants=10,
        )
        == []
    )


def test_max_variants_zero_returns_empty() -> None:
    salaries = _salaries_for_six()
    assert (
        generate_lineup_variants(
            salaries,
            [[0, 1, 2, 3, 4, 5]],
            max_variants=0,
        )
        == []
    )


def test_lineup_size_exceeds_pool_returns_empty() -> None:
    salaries = _salaries_for_six()
    assert (
        generate_lineup_variants(
            salaries,
            [[0, 1, 2, 3, 4, 5]],
            lineup_size=10,
        )
        == []
    )


def test_dk_variant_env_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FS_DK_VARIANT_SEED_COUNT", raising=False)
    monkeypatch.delenv("FS_DK_VARIANT_POOL_CAP", raising=False)
    assert dk_variant_seed_count() == 25
    assert dk_variant_pool_cap() >= 120
