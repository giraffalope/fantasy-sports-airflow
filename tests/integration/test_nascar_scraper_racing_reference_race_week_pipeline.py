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
from include.utils.file import File
from include.utils.scraper import Scraper

FIXTURES = Path(__file__).resolve().parent / "fixtures"
RACE_FIXTURE = FIXTURES / "race_page_with_links.html"


@pytest.fixture
def airflow_race_week_context() -> dict:
    return {"data_interval_start": datetime(2025, 7, 15, 12, 0, tzinfo=UTC)}


def test_cup_sync_weeks_downloads_when_missing(
    tmp_path, monkeypatch: pytest.MonkeyPatch, airflow_race_week_context: dict
) -> None:
    scraper_root = tmp_path / "nascar"
    source = RacingReferenceSource(scraper_root=str(scraper_root))
    monkeypatch.setattr(nascar_data_sources, "DEFAULT_NASCAR_DATA_SOURCE", source)

    year = airflow_race_week_context["data_interval_start"].year
    series_year = source.storage_path_series_year("Cup", year)
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

    week_dir = Path(source.storage_path_week("Cup", year, 1))
    assert (week_dir / "race-results.html").is_file()
