"""Tests for season-year resolution (cron vs asset-triggered Airflow context)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from include.nascar.data_sources.racing_reference_race_week import (
    _cup_series_schedule_year_from_context,
    _year_from_airflow_run_id,
    _year_from_airflow_scheduled_run_id,
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
