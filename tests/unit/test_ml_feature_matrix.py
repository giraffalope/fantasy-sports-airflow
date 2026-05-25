"""Unit tests for feature matrix imputation."""

from __future__ import annotations

from include.nascar.ml.feature_matrix import build_feature_matrix, feature_fill_values


def test_feature_fill_values_uses_median() -> None:
    frame = [
        {"features": {"a": 10.0, "b": None}},
        {"features": {"a": 20.0, "b": 4.0}},
        {"features": {"a": None, "b": 6.0}},
    ]
    fills = feature_fill_values(frame, ["a", "b"])
    assert fills["a"] == 15.0
    assert fills["b"] == 5.0


def test_build_feature_matrix_fills_missing() -> None:
    frame = [{"features": {"x": None}}]
    matrix = build_feature_matrix(frame, ["x"], fill_values={"x": 7.0})
    assert matrix[0, 0] == 7.0
