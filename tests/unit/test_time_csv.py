"""Tests for ``include.nascar.time_csv`` lap-time normalization."""

from __future__ import annotations

from include.nascar.time_csv import normalize_time_to_hhmmss_mmm


def test_normalize_seconds_decimal() -> None:
    assert normalize_time_to_hhmmss_mmm("47.074") == "00:00:47.074"


def test_normalize_decimal_comma_in_plausible_range() -> None:
    assert normalize_time_to_hhmmss_mmm("47,074") == "00:00:47.074"


def test_normalize_nbsp_wrapped() -> None:
    assert normalize_time_to_hhmmss_mmm("\xa047.074\xa0") == "00:00:47.074"


def test_normalize_first_token_with_trailing_note() -> None:
    assert normalize_time_to_hhmmss_mmm("48.025 (W)") == "00:00:48.025"


def test_normalize_minute_second_colon() -> None:
    assert normalize_time_to_hhmmss_mmm("1:32.776") == "00:01:32.776"


def test_normalize_passthrough_non_numeric() -> None:
    assert normalize_time_to_hhmmss_mmm("NT") == "NT"
