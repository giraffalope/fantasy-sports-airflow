"""Tests for ``include.nascar.schedule_week_date_csv``."""

from __future__ import annotations

from include.nascar.schedule_week_date_csv import week_date_for_schedule_csv


def test_week_date_naive_space_separated_datetime() -> None:
    assert week_date_for_schedule_csv("2026-03-01 00:00:00", "") == "2026-03-01"


def test_week_date_utc_z_becomes_utc_calendar_day() -> None:
    # 2026-03-10 05:00 UTC is still March 10 in UTC.
    assert week_date_for_schedule_csv("2026-03-10T05:00:00.000Z", "") == "2026-03-10"


def test_week_date_est_evening_maps_to_next_utc_day() -> None:
    # 2026-03-09 22:00 US Eastern = 2026-03-10 03:00 UTC → UTC calendar date March 10.
    assert week_date_for_schedule_csv("2026-03-09T22:00:00-05:00", "") == "2026-03-10"


def test_week_date_naive_iso_datetime_with_t() -> None:
    assert week_date_for_schedule_csv("2025-02-16T12:00:00", "") == "2025-02-16"


def test_week_date_empty_falls_back_to_cell() -> None:
    assert week_date_for_schedule_csv(None, "  fallback  ") == "fallback"


def test_week_date_unparsable_sort_returns_empty() -> None:
    assert week_date_for_schedule_csv("not-a-date", "") == ""
