"""Unit tests for ``include.nascar.enums``."""

from __future__ import annotations

from include.nascar.enums import NascarSeries


def test_nascar_series_cup_matches_db_conventions() -> None:
    cup = NascarSeries.CUP
    assert cup.series_id == 1
    assert cup.db_name == "Cup"
