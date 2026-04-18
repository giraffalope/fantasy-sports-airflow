"""Tests for ``include.nascar.cup_rr_week_folder_map``."""

from __future__ import annotations

from datetime import date

from include.nascar.cup_rr_week_folder_map import (
    RrScheduleRow,
    load_racing_reference_schedule_rows,
    match_rr_week_number,
    normalize_race_name,
)


def test_normalize_race_name_presented_by_stripped() -> None:
    assert normalize_race_name("Daytona 500 Presented By Something") == "daytona 500"


def test_match_rr_week_number_within_one_day() -> None:
    rows = [
        RrScheduleRow(6, "2020-03-15", "Atlanta 400", "http://y"),
    ]
    assert (
        match_rr_week_number(rows, scheduled_start="2020-03-16", race_name="Atlanta 400") == 6
    )


def test_match_rr_week_number_exact_date_and_title() -> None:
    rows = [
        RrScheduleRow(5, "2020-03-08", "FanShield 500", "http://x"),
        RrScheduleRow(6, "2020-03-15", "Atlanta 400", "http://y"),
    ]
    assert match_rr_week_number(rows, scheduled_start="2020-03-15", race_name="Atlanta 400") == 6


def test_match_rr_week_number_fuzzy_title() -> None:
    rows = [
        RrScheduleRow(6, "2020-03-15", "Atlanta 400", "http://y"),
    ]
    assert (
        match_rr_week_number(
            rows,
            scheduled_start="2020-03-15",
            race_name="Atlanta 400 presented by Acme",
        )
        == 6
    )


def test_match_rr_week_number_same_date_ambiguous_returns_none() -> None:
    rows = [
        RrScheduleRow(5, "2020-03-15", "Race A", "http://a"),
        RrScheduleRow(6, "2020-03-15", "Race B", "http://b"),
    ]
    assert (
        match_rr_week_number(rows, scheduled_start="2020-03-15", race_name="Unrelated Title")
        is None
    )


def test_match_rr_week_number_accepts_date_object() -> None:
    rows = [RrScheduleRow(7, "2021-02-21", "Daytona 500", "http://d")]
    assert match_rr_week_number(rows, scheduled_start=date(2021, 2, 21), race_name="Daytona 500") == 7


def test_load_racing_reference_schedule_rows_skips_empty_week_url(tmp_path) -> None:
    p = tmp_path / "schedule.csv"
    p.write_text(
        "week_number,week_url,week_date,week_track,week_track_url,week_race_name\n"
        "1,,2020-01-01,T,t,R\n"
        "2,http://w,2020-01-08,T,t,Race Two\n",
        encoding="utf-8",
    )
    rows = load_racing_reference_schedule_rows(str(p))
    assert len(rows) == 1
    assert rows[0].week_number == 2
