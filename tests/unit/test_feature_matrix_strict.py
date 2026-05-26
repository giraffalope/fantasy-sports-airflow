"""Tests for B2: strict-mode NaN/inf handling in ``build_feature_matrix``.

The legacy behavior silently wrapped every residual non-finite cell in
:func:`numpy.nan_to_num`, hiding upstream snapshot-build / aggregate
regressions from train time. After B2:

- Default (predict) path zero-fills + logs per-feature counts.
- Strict path (``strict=True`` or ``FS_ML_STRICT_FEATURES=1``) raises
  :class:`FeatureValueError` with the per-feature counts attached.
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pytest

from include.nascar.ml.feature_matrix import (
    FeatureValueError,
    build_feature_matrix,
)


def _frame_with_nan() -> list[dict[str, object]]:
    """Two-row frame: row 0 clean, row 1 carries a NaN literal in ``features``."""
    return [
        {"features": {"x": 1.0, "y": 2.0}},
        {"features": {"x": math.nan, "y": 4.0}},
    ]


def _frame_with_inf() -> list[dict[str, object]]:
    return [
        {"features": {"x": 1.0, "y": 2.0}},
        {"features": {"x": math.inf, "y": -math.inf}},
    ]


class TestStrictMode:
    """B2: ``strict=True`` and ``FS_ML_STRICT_FEATURES=1`` raise on residual NaN/inf."""

    def test_strict_raises_with_per_feature_counts(self) -> None:
        with pytest.raises(FeatureValueError) as exc:
            build_feature_matrix(_frame_with_nan(), ["x", "y"], strict=True)

        # Only column ``x`` carries the NaN — counts must reflect that, not the
        # total cell count.
        assert exc.value.counts == {"x": 1}
        assert "non-finite" in str(exc.value)

    def test_strict_raises_on_residual_inf(self) -> None:
        with pytest.raises(FeatureValueError) as exc:
            build_feature_matrix(_frame_with_inf(), ["x", "y"], strict=True)

        assert exc.value.counts == {"x": 1, "y": 1}

    def test_env_var_promotes_default_callers_to_strict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``FS_ML_STRICT_FEATURES=1`` flips the default to strict (train path)."""
        monkeypatch.setenv("FS_ML_STRICT_FEATURES", "1")
        with pytest.raises(FeatureValueError):
            build_feature_matrix(_frame_with_nan(), ["x", "y"])

    @pytest.mark.parametrize("value", ["0", "false", "no", ""])
    def test_env_var_disabled_keeps_default_zero_fill(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("FS_ML_STRICT_FEATURES", value)
        # No exception — falls through to zero-fill.
        matrix = build_feature_matrix(_frame_with_nan(), ["x", "y"])
        assert matrix.shape == (2, 2)
        assert matrix[1, 0] == 0.0  # NaN was zero-filled.

    def test_explicit_strict_false_overrides_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Predict-time callers pass ``strict=False`` to opt out of the env-wide strict mode."""
        monkeypatch.setenv("FS_ML_STRICT_FEATURES", "1")
        # Must not raise even when env requests strict.
        build_feature_matrix(_frame_with_nan(), ["x", "y"], strict=False)


class TestZeroFillFallback:
    """B2: default predict path zero-fills and logs per-feature counts."""

    def test_logs_per_feature_counts(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            matrix = build_feature_matrix(
                _frame_with_nan(), ["x", "y"], strict=False
            )

        assert matrix[1, 0] == 0.0
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warning_records, "expected a WARNING log on residual NaN zero-fill"
        # The structured counts dict must surface the offending feature name.
        joined = " ".join(r.message for r in warning_records)
        assert "'x'" in joined or "{'x': 1}" in joined

    def test_clean_frame_no_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        frame = [{"features": {"x": 1.0, "y": 2.0}}]
        with caplog.at_level(logging.WARNING):
            build_feature_matrix(frame, ["x", "y"], strict=False)
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert not warnings, (
            "build_feature_matrix must not warn on a clean frame; got: "
            f"{[r.message for r in warnings]}"
        )

    def test_zero_fills_inf_and_neg_inf(self) -> None:
        matrix = build_feature_matrix(
            _frame_with_inf(), ["x", "y"], strict=False
        )
        np.testing.assert_array_equal(matrix[1], [0.0, 0.0])
