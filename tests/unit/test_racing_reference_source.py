"""Unit tests for RacingReferenceSource URL and HTML link extraction."""

from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from include.nascar.data_sources.racing_reference import RacingReferenceSource


@pytest.fixture
def source() -> RacingReferenceSource:
    return RacingReferenceSource(scraper_root="/tmp/scraper/nascar")


def test_id_and_base_url(source: RacingReferenceSource) -> None:
    assert source.id == "racing_reference"
    assert source.base_url == "https://www.racing-reference.info"


def test_schedule_url(source: RacingReferenceSource) -> None:
    assert source.schedule_url(2025) == "https://www.racing-reference.info/raceyear/2025/W"


def test_storage_paths(source: RacingReferenceSource) -> None:
    assert source.storage_path_series_year("Cup", 2025) == (
        "/tmp/scraper/nascar/www.racing-reference.info/cup/2025"
    )
    assert source.storage_path_week("Cup", 2025, 3) == (
        "/tmp/scraper/nascar/www.racing-reference.info/cup/2025/03"
    )


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        ("https://example.com/x", "https://example.com/x"),
        ("http://example.com/x", "http://example.com/x"),
        ("//cdn.example.com/x", "https://cdn.example.com/x"),
        ("/race/foo", "https://www.racing-reference.info/race/foo"),
        ("race/foo", "https://www.racing-reference.info/race/foo"),
        ("  /trimmed/  ", "https://www.racing-reference.info/trimmed/"),
    ],
)
def test_resolve_href(source: RacingReferenceSource, href: str, expected: str) -> None:
    assert source.resolve_href(href) == expected


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("/x", "https://www.racing-reference.info/x"),
    ],
)
def test_resolve_page_url(
    source: RacingReferenceSource, href: str | None, expected: str | None
) -> None:
    assert source.resolve_page_url(href) == expected


def _results_table_with_links(links: list[tuple[str, str]]) -> BeautifulSoup:
    """Build minimal `table.tb` markup with one row of anchor cells."""
    parts = ["<table class='tb'><tr>"]
    for href, text in links:
        parts.append(f"<td><a href='{href}'>{text}</a></td>")
    parts.append("</tr></table>")
    return BeautifulSoup("".join(parts), "html.parser")


def test_extract_entrylist_url(source: RacingReferenceSource) -> None:
    soup = _results_table_with_links(
        [
            ("/other", "x"),
            ("/2024_W01/entrylist", "entry"),
        ]
    )
    assert source.extract_entrylist_url(soup) == (
        "https://www.racing-reference.info/2024_W01/entrylist"
    )


def test_extract_practice_qualifying_loop_penalties(source: RacingReferenceSource) -> None:
    soup = _results_table_with_links(
        [
            ("https://www.racing-reference.info/p1/practice-results", "p"),
            ("/q/qual-results", "q"),
            ("/l/loopdata", "l"),
            ("/pen/penalties", "pen"),
        ]
    )
    assert source.extract_practice_results_url(soup).endswith("/p1/practice-results")
    assert source.extract_qualifying_url(soup) == "https://www.racing-reference.info/q/qual-results"
    assert source.extract_loopdata_url(soup) == "https://www.racing-reference.info/l/loopdata"
    assert source.extract_penalties_url(soup) == "https://www.racing-reference.info/pen/penalties"


def test_extract_returns_none_without_tb_table(source: RacingReferenceSource) -> None:
    soup = BeautifulSoup("<div><a href='/entrylist'>x</a></div>", "html.parser")
    assert source.extract_entrylist_url(soup) is None


def test_iter_practice_session_urls(source: RacingReferenceSource) -> None:
    html = """
    <select name="qnum">
      <option value="">All</option>
      <option value="/sess/1">1</option>
      <option value="/sess/2">2</option>
      <option value="">Footer</option>
    </select>
    """
    soup = BeautifulSoup(html, "html.parser")
    pairs = source.iter_practice_session_urls(soup)
    assert pairs == [
        (1, "https://www.racing-reference.info/sess/1"),
        (2, "https://www.racing-reference.info/sess/2"),
    ]


def test_iter_practice_session_urls_empty_without_select(source: RacingReferenceSource) -> None:
    soup = BeautifulSoup("<div></div>", "html.parser")
    assert source.iter_practice_session_urls(soup) == []


def test_racing_reference_converters_delegates_to_html_to_csv(
    tmp_path, source: RacingReferenceSource
) -> None:
    """Smoke test: adapter calls through to HtmlToCsvConverter.convert_schedule."""
    html = tmp_path / "sched.html"
    csv_out = tmp_path / "sched.csv"
    html.write_text(
        """
        <div class="table-row">
          <div class="race-number"><a href="/race/1" title="Test 400">1</a></div>
          <div class="date">03/15/25</div>
          <div class="track"><a href="/t">Daytona</a></div>
        </div>
        """,
        encoding="utf-8",
    )
    source.converters.convert_schedule(str(html), str(csv_out))
    text = csv_out.read_text(encoding="utf-8")
    assert "Daytona" in text
    assert "2025-03-15" in text
