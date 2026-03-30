"""
Integration: race-week ``cup_series_sync_weeks`` with stubbed download and a minimal
``schedule.csv`` (same path layout as production).
"""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

import include.nascar.data_sources as nascar_data_sources
from include.nascar.data_sources.racing_reference import RacingReferenceSource
from include.nascar.data_sources.racing_reference_race_week import (
    cup_series_sync_weeks,
)
from include.nascar.enums import NascarSeries
from include.utils.file import File
from include.utils.scraper import Scraper

FIXTURES = Path(__file__).resolve().parent / "fixtures"
RACE_FIXTURE = FIXTURES / "race_page_with_links.html"


@pytest.fixture
def airflow_race_week_context() -> dict:
    return {"data_interval_start": datetime(2025, 7, 15, 12, 0, tzinfo=UTC)}


def test_cup_sync_weeks_skips_schedule_rows_beyond_14_day_horizon(
    tmp_path, monkeypatch: pytest.MonkeyPatch, airflow_race_week_context: dict
) -> None:
    """Only weeks with ``week_date`` before run anchor + 14 days are downloaded."""
    scraper_root = tmp_path / "nascar"
    source = RacingReferenceSource(scraper_root=str(scraper_root))
    monkeypatch.setattr(nascar_data_sources, "DEFAULT_NASCAR_DATA_SOURCE", source)

    year = airflow_race_week_context["data_interval_start"].year
    series_year = source.storage_path_series_year(NascarSeries.CUP.db_name, year)
    schedule_csv = Path(series_year) / "schedule.csv"
    schedule_csv.parent.mkdir(parents=True, exist_ok=True)
    # Run anchor 2025-07-15 12:00 UTC → cutoff 2025-07-29 12:00 UTC
    File.write_csv(
        str(schedule_csv),
        [
            ["1", "/2025_W01/daytona", "2025-02-16", "Daytona", "/t/", "Daytona 500"],
            ["2", "/2025_W99/far", "2025-12-01", "Far", "/t/", "Future 500"],
        ],
        header=[
            "week_number",
            "week_url",
            "week_date",
            "week_track",
            "week_track_url",
            "week_race_name",
        ],
    )

    downloads: list[tuple[str, str]] = []

    def fake_download(self, url, filename, user_agent, max_attempts=3):
        downloads.append((url, filename))
        out_dir = Path(self.output_folder)
        out_dir.mkdir(parents=True, exist_ok=True)
        if filename == "race-results.html":
            shutil.copy(RACE_FIXTURE, out_dir / filename)

    monkeypatch.setattr(Scraper, "download", fake_download)

    cup_series_sync_weeks(**airflow_race_week_context)

    assert len(downloads) == 1
    assert downloads[0][1] == "race-results.html"
    week1 = Path(source.storage_path_week(NascarSeries.CUP.db_name, year, 1))
    week2 = Path(source.storage_path_week(NascarSeries.CUP.db_name, year, 2))
    assert (week1 / "race-results.html").is_file()
    assert not (week2 / "race-results.html").exists()


def test_cup_sync_weeks_downloads_when_missing(
    tmp_path, monkeypatch: pytest.MonkeyPatch, airflow_race_week_context: dict
) -> None:
    scraper_root = tmp_path / "nascar"
    source = RacingReferenceSource(scraper_root=str(scraper_root))
    monkeypatch.setattr(nascar_data_sources, "DEFAULT_NASCAR_DATA_SOURCE", source)

    year = airflow_race_week_context["data_interval_start"].year
    series_year = source.storage_path_series_year(NascarSeries.CUP.db_name, year)
    schedule_csv = Path(series_year) / "schedule.csv"
    schedule_csv.parent.mkdir(parents=True, exist_ok=True)
    File.write_csv(
        str(schedule_csv),
        [["1", "/2025_W01/daytona", "2025-02-16", "Daytona", "/t/", "Daytona 500"]],
        header=[
            "week_number",
            "week_url",
            "week_date",
            "week_track",
            "week_track_url",
            "week_race_name",
        ],
    )

    def fake_download(self, url, filename, user_agent, max_attempts=3):
        out_dir = Path(self.output_folder)
        out_dir.mkdir(parents=True, exist_ok=True)
        if filename == "race-results.html":
            shutil.copy(RACE_FIXTURE, out_dir / filename)

    monkeypatch.setattr(Scraper, "download", fake_download)

    cup_series_sync_weeks(**airflow_race_week_context)

    week_dir = Path(source.storage_path_week(NascarSeries.CUP.db_name, year, 1))
    assert (week_dir / "race-results.html").is_file()
