"""Unit tests for FrcsSource URL and HTML link extraction."""

from __future__ import annotations

import csv
import io

import pytest
from bs4 import BeautifulSoup

from include.nascar.data_sources.frcs import FrcsSource
from include.nascar.data_sources.frcs_html_to_csv import FrcsHtmlToCsv
from include.nascar.enums import NascarSeries


@pytest.fixture
def source() -> FrcsSource:
    return FrcsSource(scraper_root="/tmp/scraper/nascar")


def test_id_and_base_url(source: FrcsSource) -> None:
    assert source.id == "frcs"
    assert source.base_url == "https://frcs.pro"


def test_schedule_url(source: FrcsSource) -> None:
    assert source.schedule_url(2026) == "https://frcs.pro/nascar/cup/races/schedule/2026/"


def test_storage_paths(source: FrcsSource) -> None:
    cup = NascarSeries.CUP.db_name
    assert source.storage_path_series_year(cup, 2026) == (
        "/tmp/scraper/nascar/frcs.pro/cup/2026"
    )
    assert source.storage_path_week(cup, 2026, 3) == "/tmp/scraper/nascar/frcs.pro/cup/2026/03"


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        ("https://example.com/x", "https://example.com/x"),
        ("//cdn.example.com/x", "https://cdn.example.com/x"),
        ("/nascar/foo", "https://frcs.pro/nascar/foo"),
        ("nascar/foo", "https://frcs.pro/nascar/foo"),
    ],
)
def test_resolve_href(source: FrcsSource, href: str, expected: str) -> None:
    assert source.resolve_href(href) == expected


def test_extract_urls_from_race_page_snippet(source: FrcsSource) -> None:
    soup = BeautifulSoup(
        """
        <a href="https://frcs.pro/nascar/cup/races/entrylist/2026/track/race">Entry</a>
        <a href="/nascar/cup/races/practice/1/2026/track/race">P1</a>
        <a href="/nascar/cup/races/practice/2/2026/track/race">P2</a>
        <a href="/nascar/cup/races/qualifying/2026/track/race">Q</a>
        <a href="/nascar/cup/races/loopdata/2026/track/race">Loop</a>
        """,
        "html.parser",
    )
    assert source.extract_entrylist_url(soup) is not None
    assert "/entrylist/" in (source.extract_entrylist_url(soup) or "")
    assert "/practice/1/" in (source.extract_practice_results_url(soup) or "")
    assert "/qualifying/" in (source.extract_qualifying_url(soup) or "")
    assert "/loopdata/" in (source.extract_loopdata_url(soup) or "")


def test_iter_practice_session_urls(source: FrcsSource) -> None:
    soup = BeautifulSoup(
        """
        <nav>
          <a href="/nascar/cup/races/practice/2/2026/foo/bar">P2</a>
          <a href="/nascar/cup/races/practice/1/2026/foo/bar">P1</a>
        </nav>
        """,
        "html.parser",
    )
    pairs = source.iter_practice_session_urls(soup)
    assert pairs == [
        (1, "https://frcs.pro/nascar/cup/races/practice/1/2026/foo/bar"),
        (2, "https://frcs.pro/nascar/cup/races/practice/2/2026/foo/bar"),
    ]


def test_frcs_convert_schedule_row_order(tmp_path, source: FrcsSource) -> None:
    html = tmp_path / "sched.html"
    csv_out = tmp_path / "sched.csv"
    html.write_text(
        """
        <table><tbody>
        <tr>
          <td data-column='number'>0</td>
          <td data-column='date' data-sort='2026-03-01 00:00:00'>Mar 01</td>
          <td data-column='track'><a href='https://frcs.pro/nascar/tracks/foo'>Foo Track</a></td>
          <td data-column='race'><a href='https://frcs.pro/nascar/cup/races/results/2026/foo/race-a'>Race A</a></td>
        </tr>
        <tr>
          <td data-column='number'>0</td>
          <td data-column='date' data-sort='2026-03-08 00:00:00'>Mar 08</td>
          <td data-column='track'><a href='https://frcs.pro/nascar/tracks/bar'>Bar Track</a></td>
          <td data-column='race'><a href='https://frcs.pro/nascar/cup/races/results/2026/bar/race-b'>Race B</a></td>
        </tr>
        </tbody></table>
        """,
        encoding="utf-8",
    )
    source.converters.convert_schedule(str(html), str(csv_out))
    text = csv_out.read_text(encoding="utf-8")
    assert "week_number" in text
    assert "2026-03-01" in text
    assert "Race A" in text
    assert "1" in text.splitlines()[1]


def test_parse_js_const_entry_list(tmp_path) -> None:
    html = tmp_path / "e.html"
    csv_out = tmp_path / "e.csv"
    html.write_text(
        """
        <script>
        const entryListData = [{"car_no":"16","driver_name":"<a href='x'>A J</a>",
          "team":"<a href='t'>Team</a>","make":"Chevrolet","crew_chief":"<a href='c'>CC</a>",
          "sponsor":"Co"}];
        </script>
        """,
        encoding="utf-8",
    )
    FrcsHtmlToCsv.convert_entrylist(str(html), str(csv_out))
    body = csv_out.read_text(encoding="utf-8")
    assert "A J" in body
    assert "Chevrolet" in body


def test_frcs_convert_draftkings_snake_case_headers_from_data_field(tmp_path) -> None:
    html = tmp_path / "dk.html"
    csv_out = tmp_path / "dk.csv"
    html.write_text(
        """
        <table id="draftkingsTable">
          <thead><tr>
            <th data-field="rank"><div class="tabulator-col-title">Rk</div></th>
            <th data-field="driverName"></th>
            <th data-field="fantasyPoints"></th>
          </tr></thead>
          <tbody><tr>
            <td>1</td><td><a href="/d">Jane</a></td><td>52.5</td>
          </tr></tbody>
        </table>
        """,
        encoding="utf-8",
    )
    FrcsHtmlToCsv.convert_draftkings_points(str(html))
    body = csv_out.read_text(encoding="utf-8")
    lines = [ln for ln in body.splitlines() if ln.strip()]
    assert "rank" in lines[0].lower()
    assert "driver_name" in lines[0].lower()
    assert "fantasy_points" in lines[0].lower()
    assert "Jane" in body
    assert "52.5" in body


def test_frcs_convert_fanduel_snake_case_headers_from_titles(tmp_path) -> None:
    html = tmp_path / "fd.html"
    csv_out = tmp_path / "fd.csv"
    html.write_text(
        """
        <table id="fanduelTable">
          <thead><tr>
            <th><div class="tabulator-col-title">#</div></th>
            <th><div class="tabulator-col-title">Driver</div></th>
            <th><div class="tabulator-col-title">Fantasy Pts</div></th>
          </tr></thead>
          <tbody><tr>
            <td>12</td><td>Bob</td><td>40</td>
          </tr></tbody>
        </table>
        """,
        encoding="utf-8",
    )
    FrcsHtmlToCsv.convert_fanduel_points(str(html))
    body = csv_out.read_text(encoding="utf-8")
    header = body.splitlines()[0].lower()
    assert "number" in header
    assert "driver_name" in header
    assert "fantasy_pts" in header
    assert "Bob" in body


def test_frcs_convert_fanduel_production_thead_abbr_labels(tmp_path) -> None:
    """FRCS fanduelTable thead: icons + RNK/SLRY and tooltipped <abbr> (production order)."""
    html = tmp_path / "fd.html"
    csv_out = tmp_path / "fd.csv"
    html.write_text(
        """
        <table id="fanduelTable">
          <thead>
            <tr class="text-nowrap">
              <th><i class="fa-solid fa-shield"></i>RNK</th>
              <th>SLRY</th>
              <th><abbr class="info">DRIVER</abbr></th>
              <th><abbr class="info">TEAM</abbr></th>
              <th><abbr class="info">#</abbr></th>
              <th><abbr class="info">MAKE</abbr></th>
              <th><abbr class="info">DR</abbr></th>
              <th><abbr class="info">ST</abbr></th>
              <th><abbr class="info">FIN</abbr></th>
              <th><abbr class="info">FIN PTS</abbr></th>
              <th><abbr class="info">PD PTS</abbr></th>
              <th><abbr class="info">LAPS</abbr></th>
              <th><abbr class="info">LAP PTS</abbr></th>
              <th><abbr class="info">LED</abbr></th>
              <th><abbr class="info">% LED</abbr></th>
              <th><abbr class="info">LED PTS</abbr></th>
              <th><abbr class="info">TOT PTS</abbr></th>
            </tr>
          </thead>
          <tbody><tr>
            <td>1</td><td>9200</td><td>Jane</td><td>T</td><td>16</td><td>Chevy</td>
            <td>92</td><td>4</td><td>2</td><td>50</td><td>4</td><td>267</td><td>13.4</td>
            <td>45</td><td>16.9</td><td>11.25</td><td>78.65</td>
          </tr></tbody>
        </table>
        """,
        encoding="utf-8",
    )
    FrcsHtmlToCsv.convert_fanduel_points(str(html))
    header_line = csv_out.read_text(encoding="utf-8").splitlines()[0]
    cols = next(csv.reader(io.StringIO(header_line)))
    assert cols == [
        "rank",
        "salary",
        "driver_name",
        "team",
        "number",
        "manufacturer",
        "driver_rating",
        "start",
        "finish",
        "finish_points",
        "place_differential_points",
        "laps",
        "lap_points",
        "laps_led",
        "pct_laps_led",
        "laps_led_points",
        "total_points",
    ]
    assert "9200" in csv_out.read_text(encoding="utf-8")


def test_frcs_convert_draftkings_prefers_tabulator_data_field_on_tds(tmp_path) -> None:
    """Saved HTML often lists columns on ``td[data-field]``; thead may be incomplete."""
    html = tmp_path / "dk.html"
    csv_out = tmp_path / "dk.csv"
    html.write_text(
        """
        <table id="draftkingsTable">
          <thead><tr><th></th><th></th><th></th></tr></thead>
          <tbody><tr>
            <td data-field="rank">1</td>
            <td data-field="salary">9400</td>
            <td data-field="finishPlaceDifferentialPoints">2</td>
            <td data-field="lapsPoints">5</td>
            <td data-field="lapsLedPoints">3</td>
            <td data-field="totalPoints">80</td>
          </tr></tbody>
        </table>
        """,
        encoding="utf-8",
    )
    FrcsHtmlToCsv.convert_draftkings_points(str(html))
    header_line = csv_out.read_text(encoding="utf-8").splitlines()[0].lower()
    assert "rank" in header_line
    assert "salary" in header_line
    assert "place_differential_points" in header_line
    assert "fastest_laps_points" in header_line
    assert "laps_led_points" in header_line
    assert "total_points" in header_line
    assert "column_5" not in header_line


def test_frcs_convert_draftkings_production_thead_abbr_labels(tmp_path) -> None:
    """FRCS draftkingsTable thead: icons + RNK/SLRY and tooltipped <abbr> text (no data-field)."""
    html = tmp_path / "dk.html"
    csv_out = tmp_path / "dk.csv"
    html.write_text(
        """
        <table id="draftkingsTable">
          <thead>
            <tr class="text-nowrap">
              <th><i class="fa-solid fa-crown"></i>RNK</th>
              <th>SLRY</th>
              <th><abbr class="info">DRIVER</abbr></th>
              <th><abbr class="info">TEAM</abbr></th>
              <th><abbr class="info">#</abbr></th>
              <th><abbr class="info">MAKE</abbr></th>
              <th><abbr class="info">LAPS</abbr></th>
              <th><abbr class="info">DR</abbr></th>
              <th><abbr class="info">ST</abbr></th>
              <th><abbr class="info">FIN</abbr></th>
              <th><abbr class="info">FIN PTS</abbr></th>
              <th><abbr class="info">PD PTS</abbr></th>
              <th><abbr class="info">FST</abbr></th>
              <th><abbr class="info">% FST</abbr></th>
              <th><abbr class="info">FST PTS</abbr></th>
              <th><abbr class="info">LED</abbr></th>
              <th><abbr class="info">% LED</abbr></th>
              <th><abbr class="info">LED PTS</abbr></th>
              <th><abbr class="info">TOT PTS</abbr></th>
            </tr>
          </thead>
          <tbody><tr>
            <td>1</td><td>9400</td><td>Jane</td><td>T</td><td>16</td><td>Chevy</td>
            <td>267</td><td>95</td><td>3</td><td>5</td><td>40</td><td>2</td>
            <td>12</td><td>4.5</td><td>5</td><td>20</td><td>7.5</td><td>5</td><td>52</td>
          </tr></tbody>
        </table>
        """,
        encoding="utf-8",
    )
    FrcsHtmlToCsv.convert_draftkings_points(str(html))
    header_line = csv_out.read_text(encoding="utf-8").splitlines()[0]
    cols = next(csv.reader(io.StringIO(header_line)))
    assert cols == [
        "rank",
        "salary",
        "driver_name",
        "team",
        "number",
        "manufacturer",
        "laps",
        "driver_rating",
        "start",
        "finish",
        "finish_points",
        "place_differential_points",
        "fastest_laps",
        "pct_fastest_laps",
        "fastest_laps_points",
        "laps_led",
        "pct_laps_led",
        "laps_led_points",
        "total_points",
    ]
    assert "9400" in csv_out.read_text(encoding="utf-8")


def test_frcs_convert_draftkings_pads_headers_when_tbody_has_more_columns(tmp_path) -> None:
    html = tmp_path / "dk.html"
    csv_out = tmp_path / "dk.csv"
    html.write_text(
        """
        <table id="draftkingsTable">
          <thead><tr><th data-field="a">A</th></tr></thead>
          <tbody><tr><td>1</td><td>2</td><td>3</td></tr></tbody>
        </table>
        """,
        encoding="utf-8",
    )
    FrcsHtmlToCsv.convert_draftkings_points(str(html))
    cols = next(csv.reader(io.StringIO(csv_out.read_text(encoding="utf-8").splitlines()[0])))
    assert cols == ["a", "column", "column_2"]
