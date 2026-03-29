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
