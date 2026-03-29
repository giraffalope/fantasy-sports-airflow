"""
Integration tests: recorded-style HTML fixtures exercised through the same
components the DAG uses (converter + data source), without Airflow or network.
"""

from __future__ import annotations

from pathlib import Path

from bs4 import BeautifulSoup

from include.nascar.data_sources.racing_reference import RacingReferenceSource
from include.nascar.html_to_csv_converter import HtmlToCsvConverter
from include.utils.file import File

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_schedule_fixture_through_converter_and_file_read_csv(tmp_path) -> None:
    src = FIXTURES / "schedule_two_races.html"
    html_copy = tmp_path / "schedule.html"
    csv_out = tmp_path / "schedule.csv"
    html_copy.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    HtmlToCsvConverter.convert_schedule(str(html_copy), str(csv_out))
    rows = File.read_csv(str(csv_out), as_dict=True)

    assert len(rows) == 2
    assert rows[0]["week_number"] == "1"
    assert rows[0]["week_track"] == "Daytona"
    assert rows[0]["week_date"] == "2025-02-16"
    assert "/2025-01/daytona" in rows[0]["week_url"] or "2025-01/daytona" in rows[0]["week_url"]

    assert rows[1]["week_number"] == "2"
    assert rows[1]["week_track"] == "Atlanta"
    assert rows[1]["week_date"] == "2025-02-23"


def test_race_page_fixture_results_csv_and_related_urls(tmp_path) -> None:
    src = FIXTURES / "race_page_with_links.html"
    html_path = tmp_path / "race-results.html"
    csv_path = tmp_path / "race-results.csv"
    html_path.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    HtmlToCsvConverter.convert_race_results(str(html_path), str(csv_path))
    result_rows = File.read_csv(str(csv_path), as_dict=False)
    assert len(result_rows) >= 2
    header, data = result_rows[0], result_rows[1]
    assert "position" in header
    assert "Driver X" in data

    soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")
    source = RacingReferenceSource(scraper_root=str(tmp_path / "scraper"))

    assert source.extract_entrylist_url(soup) == (
        "https://www.racing-reference.info/race/2025_week01/entrylist"
    )
    assert source.extract_practice_results_url(soup).endswith("/practice-results")
    assert source.extract_qualifying_url(soup).endswith("/qual-results")
    assert source.extract_loopdata_url(soup).endswith("/loopdata")
    assert source.extract_penalties_url(soup).endswith("/penalties")


def test_resolved_week_urls_from_schedule_match_source_base(tmp_path) -> None:
    """Mirrors DAG behavior: schedule CSV week_url values resolve against the scrape source."""
    src = FIXTURES / "schedule_two_races.html"
    html_copy = tmp_path / "schedule.html"
    csv_out = tmp_path / "schedule.csv"
    html_copy.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
    HtmlToCsvConverter.convert_schedule(str(html_copy), str(csv_out))

    source = RacingReferenceSource()
    for row in File.read_csv(str(csv_out), as_dict=True):
        resolved = source.resolve_page_url(row.get("week_url"))
        assert resolved is not None
        assert resolved.startswith(source.base_url)
