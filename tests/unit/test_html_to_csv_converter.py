"""Unit tests for HtmlToCsvConverter helpers and parsers."""

from __future__ import annotations

import csv
import io

from include.nascar.html_to_csv_converter import HtmlToCsvConverter


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
    assert not csv_path.is_file()


def test_convert_race_results_plain_td_without_col_class(tmp_path) -> None:
    """Saved RR pages sometimes omit ``class=\"col\"`` on ``td`` cells."""
    html = tmp_path / "race.html"
    csv_path = tmp_path / "race.csv"
    html.write_text(
        """
        <table class="race-results-tbl">
          <tbody>
            <tr><td>Pos</td><td>St</td><td>#</td><td>Driver</td><td>Sp</td><td>Car</td>
                <td>Laps</td><td>Stat</td><td>Led</td><td>Pts</td><td>PP</td></tr>
            <tr>
              <td>1</td><td>3</td><td>11</td><td>Jane</td>
              <td><b>Co</b> <a>Own</a></td><td>Ford</td><td>200</td><td>Running</td>
              <td>5</td><td>40</td><td>5</td>
            </tr>
          </tbody>
        </table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_race_results(str(html), str(csv_path))
    body = csv_path.read_text(encoding="utf-8")
    assert "Jane" in body
    assert "Co" in body


def test_convert_race_results_twelve_columns_skips_money_column(tmp_path) -> None:
    """Money sits between Laps and Status on many RR result tables."""
    html = tmp_path / "race.html"
    csv_path = tmp_path / "race.csv"
    html.write_text(
        """
        <table class="race-results-tbl"><tbody>
          <tr><td>Pos</td><td>St</td><td>#</td><td>D</td><td>S</td><td>C</td>
              <td>L</td><td>$</td><td>Stt</td><td>Ld</td><td>Pts</td><td>PP</td></tr>
          <tr>
            <td>1</td><td>1</td><td>2</td><td>Bob</td>
            <td><b>B</b> <a>O</a></td><td>Ch</td><td>100</td><td>999</td>
            <td>Run</td><td>10</td><td>50</td><td>7</td>
          </tr>
        </tbody></table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_race_results(str(html), str(csv_path))
    body = csv_path.read_text(encoding="utf-8")
    assert "Bob" in body
    assert "Run" in body
    assert "999" not in body


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


def test_convert_race_results_chasecol_driver_column(tmp_path) -> None:
    """Playoff rows use ``td.chasecol`` for the driver; keep driver in ``name`` column."""
    html = tmp_path / "race.html"
    csv_path = tmp_path / "race.csv"
    html.write_text(
        """
        <table class="race-results-tbl"><tbody>
          <tr><td class="col">Pos</td><td class="col">St</td><td class="col">#</td>
              <td class="col">Driver</td><td class="col">Sponsor / Owner</td><td class="col">Car</td>
              <td class="col">Laps</td><td class="col">Status</td><td class="col">Led</td><td class="col">Pts</td><td class="col">PP</td></tr>
          <tr>
            <td class="col">2</td><td class="col">4</td><td class="col">24</td>
            <td class="chasecol"><a>Chase Elliott</a></td>
            <td class="col">Hooters (Rick Hendrick)</td>
            <td class="col">Chevrolet</td><td class="col">312</td><td class="col">running</td>
            <td class="col">34</td><td class="col">45</td><td class="col">0</td>
          </tr>
        </tbody></table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_race_results(str(html), str(csv_path))
    rows = list(csv.reader(io.StringIO(csv_path.read_text(encoding="utf-8"))))
    assert rows[1][3] == "Chase Elliott"
    assert rows[1][4] == ""
    assert rows[1][5] == ""


def test_convert_prerace_penalties_no_other_results_cell_no_crash(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "pre.csv"
    html.write_text("<html><body><p>minimal</p></body></html>", encoding="utf-8")
    HtmlToCsvConverter.convert_prerace_penalties(str(html), str(csv_path))
    assert not csv_path.is_file()


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
    assert not csv_path.is_file()


def test_convert_caution_flags_no_other_results_no_crash(tmp_path) -> None:
    html = tmp_path / "race.html"
    csv_path = tmp_path / "caution.csv"
    html.write_text("<html><body></body></html>", encoding="utf-8")
    HtmlToCsvConverter.convert_caution_flags(str(html), str(csv_path))
    assert not csv_path.is_file()


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


def test_convert_practices_nine_columns_omits_behind_leader(tmp_path) -> None:
    """Matches RR Cup markup: col 5 = sec behind (e.g. 0.121), col 6 = MPH (~175+)."""
    html = tmp_path / "practice-01.html"
    html.write_text(
        """
        <table class="pracResultsTbl">
          <tr><td class="newhead">Practice 1 results for this race:</td></tr>
          <tr class="odd">
            <td class="col">1</td>
            <td class="col"><a>Leader</a></td>
            <td class="col">2</td>
            <td class="col">Ford</td>
            <td class="col">48.025</td>
            <td class="col"></td>
            <td class="col">187.402</td>
            <td class="col">13</td>
            <td class="col">3</td>
          </tr>
          <tr class="even">
            <td class="col">2</td>
            <td class="col"><a>Chaser</a></td>
            <td class="col">1</td>
            <td class="col">Chevrolet</td>
            <td class="col">48.146</td>
            <td class="col">0.121</td>
            <td class="col">186.931</td>
            <td class="col">6</td>
            <td class="col">4</td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_practices([str(html)])
    csv_path = tmp_path / "practice-01.csv"
    body = csv_path.read_text(encoding="utf-8")
    header_cols = next(csv.reader(io.StringIO(body.splitlines()[0])))
    assert header_cols == [
        "rank",
        "driver_name",
        "number",
        "manufacturer",
        "lap_time",
        "lap_speed",
        "total_lap",
        "best_lap",
    ]
    assert "00:00:48.025" in body
    assert "00:00:48.146" in body
    assert "187.402" in body
    assert "186.931" in body
    assert "0.121" not in body
    assert "Leader" in body


def test_convert_practices_chasecol_driver_column_nine_cells(tmp_path) -> None:
    """Playoff practice grids use ``td.chasecol`` for the driver (same as qualifying)."""
    html = tmp_path / "practice-01.html"
    html.write_text(
        """
        <table class="pracResultsTbl">
          <tr><td class="newhead">Practice 1 results for this race:</td></tr>
          <tr class="odd">
            <td class="col">3</td>
            <td class="chasecol"> &nbsp; <a href="/driver/Ryan_Blaney/">Ryan Blaney</a></td>
            <td class="col">12</td>
            <td class="col">Ford</td>
            <td class="col">32.188</td>
            <td class="col">0.252</td>
            <td class="col">167.764</td>
            <td class="col">32</td>
            <td class="col">2</td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_practices([str(html)])
    body = (tmp_path / "practice-01.csv").read_text(encoding="utf-8")
    assert "Ryan Blaney" in body
    assert "167.764" in body
    assert '"12"' in body or ",12," in body
    assert "0.252" not in body


def test_convert_practices_eight_columns(tmp_path) -> None:
    html = tmp_path / "practice-01.html"
    html.write_text(
        """
        <table class="pracResultsTbl">
          <tr><td class="newhead">Practice Results</td></tr>
          <tr class="even">
            <td class="col">2</td>
            <td class="col"><a>Other</a></td>
            <td class="col">3</td>
            <td class="col">Ford</td>
            <td class="col">31.0</td>
            <td class="col">175.0</td>
            <td class="col">12</td>
            <td class="col">30.5</td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_practices([str(html)])
    body = (tmp_path / "practice-01.csv").read_text(encoding="utf-8")
    header_cols = next(csv.reader(io.StringIO(body.splitlines()[0])))
    assert header_cols == [
        "rank",
        "driver_name",
        "number",
        "manufacturer",
        "lap_time",
        "lap_speed",
        "total_lap",
        "best_lap",
    ]
    assert "00:00:31.000" in body
    assert "175.0" in body
    assert "Other" in body


def test_convert_qualifying_uses_normalized_headers(tmp_path) -> None:
    html = tmp_path / "qualifying.html"
    html.write_text(
        """
        <table class="qualResultTbl">
          <tr><td class="newhead">Qualifying results for this race:</td></tr>
          <tr class="odd">
            <td class="col">1</td>
            <td class="col"><a>Leader</a></td>
            <td class="col">2</td>
            <td class="col">Ford</td>
            <td class="col">48.025</td>
            <td class="col">187.402</td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_qualifying(str(html))
    body = (tmp_path / "qualifying.csv").read_text(encoding="utf-8")
    header_cols = next(csv.reader(io.StringIO(body.splitlines()[0])))
    assert header_cols == [
        "rank",
        "driver_name",
        "number",
        "manufacturer",
        "lap_time",
        "lap_speed",
    ]
    assert "00:00:48.025" in body


def test_convert_practices_normalizes_minute_second_lap_time(tmp_path) -> None:
    html = tmp_path / "practice-01.html"
    html.write_text(
        """
        <table class="pracResultsTbl">
          <tr><td class="newhead">Practice Results</td></tr>
          <tr class="odd">
            <td class="col">1</td>
            <td class="col"><a>Driver</a></td>
            <td class="col">3</td>
            <td class="col">Ford</td>
            <td class="col">1:32.776</td>
            <td class="col">175.0</td>
            <td class="col">12</td>
            <td class="col">30.5</td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_practices([str(html)])
    body = (tmp_path / "practice-01.csv").read_text(encoding="utf-8")
    assert "00:01:32.776" in body


def test_convert_qualifying_pads_short_rows_with_empty_strings(tmp_path) -> None:
    """Rows with fewer than 6 ``td.col`` cells get empty strings for missing columns."""
    html = tmp_path / "qualifying.html"
    html.write_text(
        """
        <table class="qualResultTbl">
          <tr><td class="newhead">Qualifying results for this race:</td></tr>
          <tr class="odd">
            <td class="col">incomplete</td>
          </tr>
          <tr class="even">
            <td class="col">1</td>
            <td class="col"><a>Leader</a></td>
            <td class="col">2</td>
            <td class="col">Ford</td>
            <td class="col">48.025</td>
            <td class="col">187.402</td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    HtmlToCsvConverter.convert_qualifying(str(html))
    body = (tmp_path / "qualifying.csv").read_text(encoding="utf-8")
    assert "incomplete" in body
    assert "Leader" in body
    assert body.count("187.402") == 1
    lines = [ln for ln in body.strip().splitlines() if ln.strip()]
    assert len(lines) == 3


def test_rr_convert_entrylist_missing_eltbl_no_crash(tmp_path) -> None:
    from include.nascar.data_sources.racing_reference_html_to_csv import RacingReferenceHtmlToCsv

    html = tmp_path / "entrylist.html"
    csv_path = tmp_path / "entrylist.csv"
    html.write_text("<html><body><p>no entry table</p></body></html>", encoding="utf-8")
    RacingReferenceHtmlToCsv.convert_entrylist(str(html), str(csv_path))
    assert not csv_path.is_file()


def test_rr_convert_entrylist_compound_eltbl_and_tb_classes(tmp_path) -> None:
    from include.nascar.data_sources.racing_reference_html_to_csv import RacingReferenceHtmlToCsv

    html = tmp_path / "entrylist.html"
    csv_path = tmp_path / "entrylist.csv"
    html.write_text(
        """
        <table class="wrap eLTbl">
          <table class="tb other">
            <tr><td class="newhead">Entry list for this race:</td></tr>
            <tr class="odd">
              <td class="col">x</td>
              <td class="col">11</td>
              <td class="col"><a>Jane Racer</a></td>
              <td class="col">Owner O</td>
              <td class="col">Ford</td>
              <td class="col">Chief C</td>
              <td class="col">Sponsor S</td>
            </tr>
          </table>
        </table>
        """,
        encoding="utf-8",
    )
    RacingReferenceHtmlToCsv.convert_entrylist(str(html), str(csv_path))
    body = csv_path.read_text(encoding="utf-8")
    assert "Jane Racer" in body
    assert "11" in body
