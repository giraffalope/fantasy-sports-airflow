"""Tests for season-year resolution (cron vs asset-triggered Airflow context)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from include.nascar.data_sources.racing_reference_race_week import (
    RACE_WEEK_SCRAPE_HORIZON_DAYS,
    _cup_series_schedule_year_from_context,
    _year_from_airflow_run_id,
    _year_from_airflow_scheduled_run_id,
    parse_schedule_week_date,
    race_week_run_anchor_from_context,
    week_date_within_race_week_scrape_horizon,
)


def test_year_from_scheduled_run_id() -> None:
    assert _year_from_airflow_scheduled_run_id("scheduled__2026-03-29T21:00:00+00:00") == 2026
    assert _year_from_airflow_scheduled_run_id(None) is None
    assert _year_from_airflow_scheduled_run_id("manual__x") is None


def test_year_from_asset_triggered_run_id() -> None:
    rid = "asset_triggered__2026-03-29T21:53:18.448021+00:00_oaEL2K8L"
    assert _year_from_airflow_run_id(rid) == 2026
    assert _year_from_airflow_run_id("asset_triggered__nounderscore") is None


def test_year_from_dag_run_asset_triggered_context() -> None:
    dr = type("DR", (), {"run_id": "asset_triggered__2025-07-11T12:00:00+00:00_xyz"})()
    assert _cup_series_schedule_year_from_context({"dag_run": dr}) == 2025


def test_year_from_data_interval_start() -> None:
    ctx = {"data_interval_start": datetime(2025, 7, 15, tzinfo=UTC)}
    assert _cup_series_schedule_year_from_context(ctx) == 2025


def test_year_from_logical_date() -> None:
    ctx = {"logical_date": datetime(2024, 1, 2, tzinfo=UTC)}
    assert _cup_series_schedule_year_from_context(ctx) == 2024


def test_year_from_triggering_asset_event_source_run_id() -> None:
    ev = type(
        "E",
        (),
        {
            "source_run_id": "scheduled__2026-01-15T12:00:00+00:00",
            "timestamp": datetime(2020, 1, 1, tzinfo=UTC),
        },
    )()
    ctx = {"triggering_asset_events": {"k": [ev]}}
    assert _cup_series_schedule_year_from_context(ctx) == 2026


def test_year_from_triggering_asset_event_timestamp() -> None:
    ev = type(
        "E",
        (),
        {
            "source_run_id": None,
            "timestamp": datetime(2027, 6, 1, tzinfo=UTC),
        },
    )()
    ctx = {"triggering_asset_events": {object(): [ev]}}
    assert _cup_series_schedule_year_from_context(ctx) == 2027


def test_raises_when_unresolvable() -> None:
    with pytest.raises(ValueError):
        _cup_series_schedule_year_from_context({})


def test_race_week_run_anchor_matches_data_interval_start() -> None:
    ctx = {"data_interval_start": datetime(2025, 7, 15, 14, 30, tzinfo=UTC)}
    assert race_week_run_anchor_from_context(ctx) == datetime(2025, 7, 15, 14, 30, tzinfo=UTC)


def test_parse_schedule_week_date() -> None:
    assert parse_schedule_week_date("2025-03-09") == date(2025, 3, 9)
    assert parse_schedule_week_date("  2025-03-09T00:00:00  ") == date(2025, 3, 9)
    assert parse_schedule_week_date("") is None
    assert parse_schedule_week_date("nope") is None


def test_week_date_within_horizon_past_and_near_future() -> None:
    run = datetime(2025, 7, 15, 12, 0, tzinfo=UTC)
    assert week_date_within_race_week_scrape_horizon(date(2025, 2, 16), run)
    assert week_date_within_race_week_scrape_horizon(date(2025, 7, 28), run)


def test_week_date_within_horizon_excludes_far_future() -> None:
    run = datetime(2025, 7, 15, 12, 0, tzinfo=UTC)
    cutoff_day = (run + timedelta(days=RACE_WEEK_SCRAPE_HORIZON_DAYS)).date()
    far = cutoff_day + timedelta(days=30)
    assert not week_date_within_race_week_scrape_horizon(far, run)


def test_race_week_run_anchor_raises_without_fallbacks() -> None:
    with pytest.raises(ValueError, match="race-week run anchor"):
        race_week_run_anchor_from_context({})
