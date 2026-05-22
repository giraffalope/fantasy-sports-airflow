"""Backfill schedule DAGs skip HTML download and HTML→CSV when ``schedule.csv`` already exists."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from pathlib import Path

import pytest

import include.nascar.data_sources as nascar_data_sources
from include.nascar.data_sources.frcs import (
    FrcsSource,
    download_year_schedule_html as frcs_download_year_schedule_html,
    convert_year_schedule_html_to_csv as frcs_convert_year_schedule_html_to_csv,
)
from include.nascar.data_sources.racing_reference import (
    RacingReferenceSource,
    download_year_schedule_html as rr_download_year_schedule_html,
    convert_year_schedule_html_to_csv as rr_convert_year_schedule_html_to_csv,
)
from include.nascar.enums import NascarSeries
from include.utils.scraper import Scraper


@pytest.fixture
def backfill_context() -> dict:
    return {
        "data_interval_start": datetime(2015, 6, 1, 0, 0, tzinfo=UTC),
        "dag": SimpleNamespace(dag_id="backfill.nascar.cup.scraper.frcs.schedule"),
    }


@pytest.fixture
def prod_context() -> dict:
    return {
        "data_interval_start": datetime(2015, 6, 1, 0, 0, tzinfo=UTC),
        "dag": SimpleNamespace(dag_id="nascar.cup.scraper.frcs.schedule"),
    }


def test_frcs_backfill_skips_download_and_convert_when_schedule_csv_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backfill_context: dict
) -> None:
    source = FrcsSource(scraper_root=str(tmp_path))
    monkeypatch.setattr(nascar_data_sources, "DEFAULT_NASCAR_DATA_SOURCE", source)
    year = backfill_context["data_interval_start"].year
    base = Path(source.storage_path_series_year(NascarSeries.CUP.db_name, year))
    base.mkdir(parents=True, exist_ok=True)
    (base / "schedule.csv").write_text("event_number,week_url\n1,http://x\n", encoding="utf-8")

    calls: list[tuple[str, str]] = []

    def fake_download(self, url, filename, user_agent, max_attempts=3):
        calls.append((url, filename))

    monkeypatch.setattr(Scraper, "download", fake_download)

    frcs_download_year_schedule_html(backfill_context, NascarSeries.CUP.db_name, source=source)
    assert calls == []

    frcs_convert_year_schedule_html_to_csv(backfill_context, NascarSeries.CUP.db_name, source=source)
    assert (base / "schedule.csv").read_text(encoding="utf-8").startswith("event_number")


def test_frcs_prod_still_downloads_when_schedule_csv_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prod_context: dict
) -> None:
    source = FrcsSource(scraper_root=str(tmp_path))
    monkeypatch.setattr(nascar_data_sources, "DEFAULT_NASCAR_DATA_SOURCE", source)
    year = prod_context["data_interval_start"].year
    base = Path(source.storage_path_series_year(NascarSeries.CUP.db_name, year))
    base.mkdir(parents=True, exist_ok=True)
    (base / "schedule.csv").write_text("event_number,week_url\n1,http://x\n", encoding="utf-8")

    calls: list[str] = []

    def fake_download(self, url, filename, user_agent, max_attempts=3):
        calls.append(filename)
        out_dir = Path(self.output_folder)
        (out_dir / filename).write_text("<html><body></body></html>", encoding="utf-8")

    monkeypatch.setattr(Scraper, "download", fake_download)

    frcs_download_year_schedule_html(prod_context, NascarSeries.CUP.db_name, source=source)
    assert "schedule.html" in calls


def test_rr_backfill_skips_download_when_schedule_csv_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backfill_context: dict
) -> None:
    source = RacingReferenceSource(scraper_root=str(tmp_path))
    monkeypatch.setattr(nascar_data_sources, "DEFAULT_NASCAR_DATA_SOURCE", source)
    ctx = {
        **backfill_context,
        "dag": SimpleNamespace(dag_id="backfill.nascar.cup.scraper.racing_reference.schedule"),
    }
    year = ctx["data_interval_start"].year
    base = Path(source.storage_path_series_year(NascarSeries.CUP.db_name, year))
    base.mkdir(parents=True, exist_ok=True)
    (base / "schedule.csv").write_text("week_number,week_url\n1,http://x\n", encoding="utf-8")

    calls: list[str] = []

    def fake_download(self, url, filename, user_agent, max_attempts=3):
        calls.append(filename)

    monkeypatch.setattr(Scraper, "download", fake_download)

    rr_download_year_schedule_html(ctx, NascarSeries.CUP.db_name, source=source)
    assert calls == []

    before = (base / "schedule.csv").read_text(encoding="utf-8")
    rr_convert_year_schedule_html_to_csv(ctx, NascarSeries.CUP.db_name, source=source)
    assert (base / "schedule.csv").read_text(encoding="utf-8") == before
