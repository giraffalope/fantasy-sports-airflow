"""Unit tests for ``split_holdout_mask`` / ``split_train_dev_holdout_masks``.

Covers A2: the three-way temporal split, env-var-driven dev_weeks defaults
(via :mod:`include.nascar.ml.model_scope`), and small-cohort fallbacks.
"""

from __future__ import annotations

import numpy as np
import pytest

from include.nascar.ml.model_scope import dev_weeks, holdout_weeks
from include.nascar.ml.xgb_train_pipeline import (
    split_holdout_mask,
    split_train_dev_holdout_masks,
)


def _frame(week_ids: list[int]) -> list[dict[str, object]]:
    return [{"week_id": w} for w in week_ids]


class TestSplitTrainDevHoldoutMasks:
    """A2: three-way temporal split."""

    def test_three_way_split_with_six_weeks(self) -> None:
        """Default 4-train / 1-dev / 1-holdout on a six-week frame."""
        frame = _frame([1, 1, 2, 3, 4, 5, 6, 6])
        train, dev, holdout = split_train_dev_holdout_masks(
            frame, dev_weeks=1, holdout_weeks=1
        )

        np.testing.assert_array_equal(
            train, [True, True, True, True, True, False, False, False]
        )
        np.testing.assert_array_equal(
            dev, [False, False, False, False, False, True, False, False]
        )
        np.testing.assert_array_equal(
            holdout, [False, False, False, False, False, False, True, True]
        )

    def test_masks_are_disjoint_and_total(self) -> None:
        frame = _frame([1, 2, 3, 4, 5, 6, 7, 8])
        train, dev, holdout = split_train_dev_holdout_masks(
            frame, dev_weeks=2, holdout_weeks=2
        )

        np.testing.assert_array_equal(train | dev | holdout, np.ones(len(frame), bool))
        assert int((train & dev).sum()) == 0
        assert int((train & holdout).sum()) == 0
        assert int((dev & holdout).sum()) == 0

    def test_three_way_orders_dev_before_holdout(self) -> None:
        """Dev must always precede holdout in week order (no temporal leakage)."""
        frame = _frame([10, 20, 30, 40, 50])
        train, dev, holdout = split_train_dev_holdout_masks(
            frame, dev_weeks=1, holdout_weeks=1
        )

        train_weeks = {int(r["week_id"]) for r, m in zip(frame, train) if m}
        dev_weeks_set = {int(r["week_id"]) for r, m in zip(frame, dev) if m}
        holdout_weeks_set = {int(r["week_id"]) for r, m in zip(frame, holdout) if m}

        assert max(train_weeks) < min(dev_weeks_set)
        assert max(dev_weeks_set) < min(holdout_weeks_set)

    def test_small_cohort_collapses_to_two_way(self) -> None:
        """When weeks <= holdout_weeks + dev_weeks, dev is empty (legacy fallback)."""
        frame = _frame([1, 2])  # only 2 distinct weeks
        train, dev, holdout = split_train_dev_holdout_masks(
            frame, dev_weeks=1, holdout_weeks=1
        )

        np.testing.assert_array_equal(train, [True, False])
        np.testing.assert_array_equal(dev, [False, False])
        np.testing.assert_array_equal(holdout, [False, True])

    def test_only_one_distinct_week_returns_all_train(self) -> None:
        """When weeks <= holdout_weeks, holdout is empty too (bootstrap)."""
        frame = _frame([1, 1, 1])
        train, dev, holdout = split_train_dev_holdout_masks(
            frame, dev_weeks=1, holdout_weeks=1
        )

        np.testing.assert_array_equal(train, [True, True, True])
        np.testing.assert_array_equal(dev, [False, False, False])
        np.testing.assert_array_equal(holdout, [False, False, False])

    def test_empty_frame_returns_three_empty_masks(self) -> None:
        train, dev, holdout = split_train_dev_holdout_masks([], dev_weeks=1, holdout_weeks=1)
        assert train.dtype == bool and dev.dtype == bool and holdout.dtype == bool
        assert train.size == 0 and dev.size == 0 and holdout.size == 0

    def test_dev_weeks_zero_equals_two_way(self) -> None:
        """``dev_weeks=0`` matches the legacy ``split_holdout_mask`` contract."""
        frame = _frame([1, 2, 3, 4, 5])
        train_3, dev_3, holdout_3 = split_train_dev_holdout_masks(
            frame, dev_weeks=0, holdout_weeks=2
        )
        train_2, holdout_2 = split_holdout_mask(frame, holdout_weeks=2)

        np.testing.assert_array_equal(dev_3, np.zeros(len(frame), bool))
        np.testing.assert_array_equal(train_3, train_2)
        np.testing.assert_array_equal(holdout_3, holdout_2)


class TestSplitHoldoutMaskBackwardsCompat:
    """A2: legacy two-way wrapper still returns ``(train, holdout)``."""

    def test_returns_two_tuple(self) -> None:
        frame = _frame([1, 2, 3, 4])
        result = split_holdout_mask(frame, holdout_weeks=1)
        assert isinstance(result, tuple) and len(result) == 2

    def test_dev_folded_into_train(self) -> None:
        """The wrapper folds any dev slice into the train mask."""
        frame = _frame([1, 2, 3, 4, 5, 6])
        train, holdout = split_holdout_mask(frame, holdout_weeks=2)

        # All non-holdout rows must be in train (no dev slice externally visible).
        assert int(train.sum()) + int(holdout.sum()) == len(frame)


class TestEnvVarDefaults:
    """A2: env vars resolve to integer defaults."""

    def test_holdout_weeks_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FS_ML_HOLDOUT_WEEKS", raising=False)
        assert holdout_weeks() == 1

    def test_holdout_weeks_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FS_ML_HOLDOUT_WEEKS", "3")
        assert holdout_weeks() == 3

    def test_dev_weeks_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("FS_ML_DEV_WEEKS", raising=False)
        assert dev_weeks() == 1

    def test_dev_weeks_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FS_ML_DEV_WEEKS", "2")
        assert dev_weeks() == 2

    def test_dev_weeks_zero_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``FS_ML_DEV_WEEKS=0`` is supported as the explicit disable knob."""
        monkeypatch.setenv("FS_ML_DEV_WEEKS", "0")
        assert dev_weeks() == 0
