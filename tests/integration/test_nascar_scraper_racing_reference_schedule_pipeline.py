"""
Integration: Racing Reference schedule DAG callables (download + convert) with a
stubbed browser download, using the same paths the DAG uses at runtime.
"""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

import include.nascar.data_sources as nascar_data_sources
from include.nascar.data_sources.racing_reference import (
    RacingReferenceSource,
    cup_series_convert_year_schedule_html_to_csv,
    cup_series_download_year_schedule_html,
)
from include.nascar.enums import NascarSeries
from include.utils.file import File
from include.utils.scraper import Scraper

FIXTURES = Path(__file__).resolve().parent / "fixtures"
SCHEDULE_FIXTURE = FIXTURES / "schedule_two_races.html"


@pytest.fixture
def airflow_schedule_context() -> dict:
    return {"data_interval_start": datetime(2025, 7, 15, 12, 0, tzinfo=UTC)}


def test_cup_schedule_download_then_convert_integration(
    tmp_path, monkeypatch: pytest.MonkeyPatch, airflow_schedule_context: dict
) -> None:
    """Mirror DAG tasks 2–3: download writes HTML, convert produces CSV."""
    scraper_root = tmp_path / "nascar"
    source = RacingReferenceSource(scraper_root=str(scraper_root))
    monkeypatch.setattr(nascar_data_sources, "DEFAULT_NASCAR_DATA_SOURCE", source)

    def fake_download(self, url, filename, user_agent, max_attempts=3):
        out_dir = Path(self.output_folder)
        out_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(SCHEDULE_FIXTURE, out_dir / filename)

    monkeypatch.setattr(Scraper, "download", fake_download)

    cup_series_download_year_schedule_html(**airflow_schedule_context)
    cup_series_convert_year_schedule_html_to_csv(**airflow_schedule_context)

    year = airflow_schedule_context["data_interval_start"].year
    base = source.storage_path_series_year(NascarSeries.CUP.db_name, year)
    html_path = Path(base) / "schedule.html"
    csv_path = Path(base) / "schedule.csv"

    assert html_path.is_file()
    assert csv_path.is_file()

    rows = File.read_csv(str(csv_path), as_dict=True)
    assert len(rows) == 2
    assert rows[0]["week_track"] == "Daytona"
