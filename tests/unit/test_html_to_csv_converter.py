"""Unit tests for HtmlToCsvConverter helpers and parsers."""

from __future__ import annotations

import pytest

from include.nascar.html_to_csv_converter import HtmlToCsvConverter


@pytest.mark.parametrize(
    ("time_str", "expected"),
    [
        ("", 0.0),
        ("30.5", 30.5),
        ("1:30.5", 90.5),
        ("0:45", 45.0),
    ],
)
def test_convert_to_seconds(time_str: str, expected: float) -> None:
    assert HtmlToCsvConverter.convert_to_seconds(time_str) == pytest.approx(expected)


def test_convert_schedule_writes_expected_columns(tmp_path) -> None:
    html = tmp_path / "in.html"
    csv_path = tmp_path / "out.csv"
    html.write_text(
        """
        <div class="table-row">
          <div class="race-number"><a href="/2025_week01" title="Some 500">1</a></div>
          <div class="date">01/05/25</div>
          <div class="track"><a href="/tracks/foo">Atlanta</a></div>
        </div>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_schedule(str(html), str(csv_path))
    body = csv_path.read_text(encoding="utf-8")
    assert "Atlanta" in body
    assert "2025-01-05" in body
    assert "/2025_week01" in body or "2025_week01" in body


def test_convert_race_results_minimal_table(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "race.csv"
    html.write_text(
        """
        <table class="race-results-tbl">
          <tr><th>h</th></tr>
          <tr>
            <td class="col">1</td>
            <td class="col">2</td>
            <td class="col">11</td>
            <td class="col">Driver A</td>
            <td class="col"><b>Sponsor</b> <a>Owner O</a></td>
            <td class="col">Ford</td>
            <td class="col">200</td>
            <td class="col">Running</td>
            <td class="col">10</td>
            <td class="col">40</td>
            <td class="col">0</td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_race_results(str(html), str(csv_path))
    out = csv_path.read_text(encoding="utf-8")
    assert "Driver A" in out
    assert "Sponsor" in out


def test_convert_race_results_no_results_table_no_crash(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "race.csv"
    html.write_text("<html><body><p>no table</p></body></html>", encoding="utf-8")
    HtmlToCsvConverter.convert_race_results(str(html), str(csv_path))
    text = csv_path.read_text(encoding="utf-8")
    assert "position" in text


def test_convert_race_results_skips_short_rows(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "race.csv"
    html.write_text(
        """
        <table class="race-results-tbl">
          <tr><th>h</th></tr>
          <tr><td class="col">1</td><td class="col">2</td></tr>
          <tr>
            <td class="col">1</td><td class="col">2</td><td class="col">11</td>
            <td class="col">Driver A</td>
            <td class="col"><b>S</b> <a>O</a></td>
            <td class="col">Ford</td><td class="col">200</td><td class="col">Running</td>
            <td class="col">10</td><td class="col">40</td><td class="col">0</td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_race_results(str(html), str(csv_path))
    body = csv_path.read_text(encoding="utf-8")
    assert "Driver A" in body
    assert body.count("Driver A") == 1


def test_convert_prerace_penalties_no_other_results_cell_no_crash(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "pre.csv"
    html.write_text("<html><body><p>minimal</p></body></html>", encoding="utf-8")
    HtmlToCsvConverter.convert_prerace_penalties(str(html), str(csv_path))
    text = csv_path.read_text(encoding="utf-8")
    assert "name" in text


def test_convert_prerace_penalties_tb_without_newhead_no_crash(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "pre.csv"
    html.write_text(
        """
        <html><body>
        <td class="otherResultsTbl">
          <table class="tb"><tr><td class="col">no newhead here</td></tr></table>
        </td>
        </body></html>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_prerace_penalties(str(html), str(csv_path))
    assert csv_path.is_file()


def test_convert_caution_flags_no_other_results_no_crash(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "caution.csv"
    html.write_text("<html><body></body></html>", encoding="utf-8")
    HtmlToCsvConverter.convert_caution_flags(str(html), str(csv_path))
    assert "condition" in csv_path.read_text(encoding="utf-8")


def test_convert_caution_flags_row_without_img_uses_first_cell_text(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "caution.csv"
    html.write_text(
        """
        <html><body>
        <td class="otherResultsTbl">
          <table class="tb">
            <tr><td class="newhead">Caution flag breakdown:</td></tr>
            <tr><td class="col">h</td><td class="col">h</td><td class="col">h</td>
                <td class="col">h</td><td class="col">h</td><td class="col">h</td></tr>
            <tr>
              <td class="col">green</td>
              <td class="col">1</td><td class="col">2</td><td class="col">3</td>
              <td class="col">rain</td><td class="col">#99</td>
            </tr>
          </table>
        </td>
        </body></html>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_caution_flags(str(html), str(csv_path))
    body = csv_path.read_text(encoding="utf-8")
    assert "green" in body
    assert "rain" in body


def test_convert_caution_flags_skips_short_rows(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "caution.csv"
    html.write_text(
        """
        <html><body>
        <td class="otherResultsTbl">
          <table class="tb">
            <tr><td class="newhead">Caution flag breakdown:</td></tr>
            <tr><td class="col">a</td><td class="col">b</td><td class="col">c</td>
                <td class="col">d</td><td class="col">e</td><td class="col">f</td></tr>
            <tr><td class="col">short</td></tr>
            <tr>
              <td class="col"><img src="/icons/yellow_flag.gif"/></td>
              <td class="col">10</td><td class="col">12</td><td class="col">2</td>
              <td class="col">debris</td><td class="col"></td>
            </tr>
          </table>
        </td>
        </body></html>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_caution_flags(str(html), str(csv_path))
    body = csv_path.read_text(encoding="utf-8")
    assert "yellow" in body
    assert "debris" in body
    assert "short" not in body


def test_convert_prerace_penalties_to_the_back_skips_short_rows(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "pre.csv"
    html.write_text(
        """
        <html><body>
        <td class="otherResultsTbl">
          <table class="tb">
            <tr><td class="newhead">To the back:</td></tr>
            <tr><td class="col">h1</td><td class="col">h2</td><td class="col">h3</td></tr>
            <tr><td class="col">only one</td></tr>
            <tr>
              <td class="col">A</td><td class="col">1</td><td class="col">reason</td>
            </tr>
          </table>
        </td>
        </body></html>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_prerace_penalties(str(html), str(csv_path))
    body = csv_path.read_text(encoding="utf-8")
    assert "A" in body
    assert "only one" not in body
