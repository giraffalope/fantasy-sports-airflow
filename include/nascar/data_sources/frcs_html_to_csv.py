"""HTML / embedded JSON → CSV parsers for frcs.pro (NASCAR Cup schedule, race weekend pages)."""

from __future__ import annotations

import json
import logging
import re
from html import unescape
from typing import Literal

from bs4 import BeautifulSoup, Tag

from include.utils.file import File

_FRCS_SCHEDULE_RACE_COL = "race"
_FRCS_SCHEDULE_TRACK_COL = "track"
_FRCS_SCHEDULE_DATE_COL = "date"
# Tabulator column for the schedule table header shown as ``#`` (race index in season).
_FRCS_SCHEDULE_NUMBER_COL = "number"


def _frcs_schedule_cell_sort_or_text(td: Tag) -> str:
    ds = td.get("data-sort")
    if ds is not None and str(ds).strip():
        return str(ds).strip()
    return td.get_text(strip=True)


def _strip_html(fragment: str) -> str:
    return BeautifulSoup(unescape(fragment), "html.parser").get_text(strip=True)


def _parse_js_const_json_array(html: str, const_name: str) -> list[dict] | None:
    needle = f"const {const_name} = "
    start = html.find(needle)
    if start == -1:
        return None
    bracket = html.find("[", start + len(needle))
    if bracket == -1:
        return None
    depth = 0
    for i in range(bracket, len(html)):
        c = html[i]
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[bracket : i + 1])
                except json.JSONDecodeError:
                    logging.warning("Could not JSON-decode %s from FRCS page", const_name)
                    return None
    return None


def _frcs_table_column_labels(table: Tag) -> list[str]:
    thead = table.find("thead")
    if thead is None:
        return []
    row = thead.find("tr")
    if row is None:
        return []
    labels: list[str] = []
    for th in row.find_all("th"):
        abbr = th.find("abbr")
        raw = abbr.get_text(strip=True) if abbr else th.get_text(strip=True)
        labels.append(re.sub(r"\s+", " ", raw))
    return labels


def _raw_header_text_from_th(th: Tag) -> str:
    for attr in ("data-field", "tabulator-field", "field"):
        v = th.get(attr)
        if v and str(v).strip():
            return str(v).strip()
    dc = th.get("data-column")
    if dc is not None and str(dc).strip() and not str(dc).strip().isdigit():
        return str(dc).strip()
    col_title = th.select_one(".tabulator-col-title")
    if col_title is not None:
        t = col_title.get_text(strip=True)
        if t:
            return t
    abbr = th.find("abbr")
    if abbr is not None:
        t = abbr.get_text(strip=True)
        if t:
            return t
    for attr in ("aria-label", "title"):
        v = th.get(attr)
        if v and str(v).strip():
            return str(v).strip()
    return re.sub(r"\s+", " ", th.get_text(strip=True))


def _frcs_dfs_header_synonyms(slug: str) -> str:
    if slug == "driver":
        return "driver_name"
    if slug in {"rk", "pos", "position"}:
        return "rank"
    return slug


def _frcs_header_label_to_snake_case(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    if re.fullmatch(r"[a-z][a-z0-9_]*", raw):
        return _frcs_dfs_header_synonyms(raw) or "column"
    if re.search(r"[a-z][A-Z]|[A-Z][a-z]", raw) and " " not in raw:
        s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw)
        s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
        s = re.sub(r"_+", "_", s).lower().strip("_")
        return _frcs_dfs_header_synonyms(s) or "column"
    t = raw.lower()
    if t in {"#", "no.", "no", "no #"}:
        return "number"
    t = t.replace("#", "number")
    t = t.replace("%", "pct")
    t = re.sub(r"[^a-z0-9]+", "_", t)
    t = re.sub(r"_+", "_", t).strip("_")
    return _frcs_dfs_header_synonyms(t) or "column"


# FRCS ``draftkingsTable`` column order (production layout). Used when tbody width matches.
_FRCS_DRAFTKINGS_FIXED_HEADERS: tuple[str, ...] = (
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
)

_FRCS_FANDUEL_FIXED_HEADERS: tuple[str, ...] = (
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
)

_FRCS_DRAFTKINGS_HEADER_MAP: dict[str, str] = {
    "rnk": "rank",
    "slry": "salary",
    "driver": "driver_name",
    "team": "team",
    "#": "number",
    "make": "manufacturer",
    "laps": "laps",
    "dr": "driver_rating",
    "st": "start",
    "fin": "finish",
    "fin pts": "finish_points",
    "pd pts": "place_differential_points",
    "fst": "fastest_laps",
    "% fst": "pct_fastest_laps",
    "fst pts": "fastest_laps_points",
    "led": "laps_led",
    "% led": "pct_laps_led",
    "led pts": "laps_led_points",
    "tot pts": "total_points",
}


def _frcs_compact_field_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


_FRCS_DRAFTKINGS_COMPACT_FIELD_MAP: dict[str, str] = {
    "rank": "rank",
    "rnk": "rank",
    "salary": "salary",
    "slry": "salary",
    "driver": "driver_name",
    "drivername": "driver_name",
    "team": "team",
    "number": "number",
    "carnumber": "number",
    "make": "manufacturer",
    "manufacturer": "manufacturer",
    "laps": "laps",
    "driverrating": "driver_rating",
    "start": "start",
    "startposition": "start",
    "finish": "finish",
    "finishposition": "finish",
    "finishpoints": "finish_points",
    "placedifferentialpoints": "place_differential_points",
    "finishplacedifferentialpoints": "place_differential_points",
    "fastestlaps": "fastest_laps",
    "pctfastestlaps": "pct_fastest_laps",
    "percentfastestlaps": "pct_fastest_laps",
    "fastestlapspoints": "fastest_laps_points",
    "lapspoints": "fastest_laps_points",
    "lapsled": "laps_led",
    "pctlapsled": "pct_laps_led",
    "percentlapsled": "pct_laps_led",
    "lapsledpoints": "laps_led_points",
    "totalpoints": "total_points",
}

_FRCS_DRAFTKINGS_SNAKE_OVERRIDES: dict[str, str] = {
    "finish_place_differential_points": "place_differential_points",
    "laps_points": "fastest_laps_points",
    "fast_lap_points": "fastest_laps_points",
    "fast_laps_points": "fastest_laps_points",
}


def _frcs_draftkings_header_to_snake(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    key = re.sub(r"\s+", " ", raw).lower()
    if key in _FRCS_DRAFTKINGS_HEADER_MAP:
        return _FRCS_DRAFTKINGS_HEADER_MAP[key]
    ck = _frcs_compact_field_key(raw)
    if ck in _FRCS_DRAFTKINGS_COMPACT_FIELD_MAP:
        return _FRCS_DRAFTKINGS_COMPACT_FIELD_MAP[ck]
    slug = _frcs_header_label_to_snake_case(raw)
    if slug in _FRCS_DRAFTKINGS_SNAKE_OVERRIDES:
        return _FRCS_DRAFTKINGS_SNAKE_OVERRIDES[slug]
    cslug = slug.replace("_", "")
    if cslug in _FRCS_DRAFTKINGS_COMPACT_FIELD_MAP:
        return _FRCS_DRAFTKINGS_COMPACT_FIELD_MAP[cslug]
    return slug


_FRCS_FANDUEL_HEADER_MAP: dict[str, str] = {
    "rnk": "rank",
    "slry": "salary",
    "driver": "driver_name",
    "team": "team",
    "#": "number",
    "make": "manufacturer",
    "dr": "driver_rating",
    "st": "start",
    "fin": "finish",
    "fin pts": "finish_points",
    "pd pts": "place_differential_points",
    "laps": "laps",
    "lap pts": "lap_points",
    "led": "laps_led",
    "% led": "pct_laps_led",
    "led pts": "laps_led_points",
    "tot pts": "total_points",
}

_FRCS_FANDUEL_COMPACT_FIELD_MAP: dict[str, str] = {
    "rank": "rank",
    "rnk": "rank",
    "salary": "salary",
    "slry": "salary",
    "driver": "driver_name",
    "drivername": "driver_name",
    "team": "team",
    "number": "number",
    "carnumber": "number",
    "make": "manufacturer",
    "manufacturer": "manufacturer",
    "driverrating": "driver_rating",
    "start": "start",
    "startposition": "start",
    "finish": "finish",
    "finishposition": "finish",
    "finishpoints": "finish_points",
    "placedifferentialpoints": "place_differential_points",
    "finishplacedifferentialpoints": "place_differential_points",
    "laps": "laps",
    "lapspoints": "lap_points",
    "lappts": "lap_points",
    "lapsled": "laps_led",
    "pctlapsled": "pct_laps_led",
    "percentlapsled": "pct_laps_led",
    "lapsledpoints": "laps_led_points",
    "totalpoints": "total_points",
}

_FRCS_FANDUEL_SNAKE_OVERRIDES: dict[str, str] = {
    "finish_place_differential_points": "place_differential_points",
    "laps_points": "lap_points",
}


def _frcs_fanduel_header_to_snake(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    key = re.sub(r"\s+", " ", raw).lower()
    if key in _FRCS_FANDUEL_HEADER_MAP:
        return _FRCS_FANDUEL_HEADER_MAP[key]
    ck = _frcs_compact_field_key(raw)
    if ck in _FRCS_FANDUEL_COMPACT_FIELD_MAP:
        return _FRCS_FANDUEL_COMPACT_FIELD_MAP[ck]
    slug = _frcs_header_label_to_snake_case(raw)
    if slug in _FRCS_FANDUEL_SNAKE_OVERRIDES:
        return _FRCS_FANDUEL_SNAKE_OVERRIDES[slug]
    cslug = slug.replace("_", "")
    if cslug in _FRCS_FANDUEL_COMPACT_FIELD_MAP:
        return _FRCS_FANDUEL_COMPACT_FIELD_MAP[cslug]
    return slug


_FRCS_RACE_RESULTS_HEADER_MAP: dict[str, str] = {
    "fin": "finish",
    "st": "start",
    "#": "number",
    "driver": "driver_name",
    "make": "manufacturer",
    "tot pts": "total_points",
    "fin pts": "finish_points",
    "stg pts": "stage_points",
    "bon pts": "bonus_points",
    "po pts": "playoff_points",
    "laps": "laps",
    "led": "laps_led",
    "x led": "times_led",
    "s1": "stage_1",
    "s2": "stage_2",
    "s3": "stage_3",
    "status": "status",
}


def _frcs_race_results_header_to_snake(raw: str) -> str:
    key = re.sub(r"\s+", " ", raw.strip().lower())
    if key in _FRCS_RACE_RESULTS_HEADER_MAP:
        return _FRCS_RACE_RESULTS_HEADER_MAP[key]
    return _frcs_header_label_to_snake_case(raw)


def _frcs_unique_snake_headers(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in names:
        base = name if name else "column"
        n = seen.get(base, 0)
        seen[base] = n + 1
        out.append(base if n == 0 else f"{base}_{n + 1}")
    return out


def _frcs_thead_labels_expanded_for_colspan(table: Tag) -> list[str]:
    thead = table.find("thead")
    if thead is None:
        return []
    row = thead.find("tr")
    if row is None:
        return []
    labels: list[str] = []
    for th in row.find_all("th"):
        raw = _raw_header_text_from_th(th)
        try:
            n = int(th.get("colspan") or 1)
        except (TypeError, ValueError):
            n = 1
        for _ in range(max(1, n)):
            labels.append(raw)
    return labels


def _frcs_first_body_row_data_fields(table: Tag) -> list[str]:
    tbody = table.find("tbody")
    if tbody is None:
        return []
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        fields: list[str] = []
        for td in cells:
            f = td.get("data-field") or td.get("tabulator-field")
            fields.append(str(f).strip() if f else "")
        return fields
    return []


def _frcs_dfs_merged_raw_labels(table: Tag, num_data_columns: int) -> list[str]:
    thead_labels = _frcs_thead_labels_expanded_for_colspan(table)
    field_labels = _frcs_first_body_row_data_fields(table)
    if num_data_columns <= 0:
        return thead_labels or field_labels
    merged: list[str] = []
    for i in range(num_data_columns):
        f = field_labels[i] if i < len(field_labels) else ""
        t = thead_labels[i] if i < len(thead_labels) else ""
        merged.append(f if f else t)
    return merged


def _frcs_table_headers_snake_case(
    table: Tag,
    num_data_columns: int,
    *,
    dfs_site: Literal["draftkings", "fanduel"] | None = None,
) -> list[str]:
    if dfs_site == "draftkings" and num_data_columns == len(_FRCS_DRAFTKINGS_FIXED_HEADERS):
        return list(_FRCS_DRAFTKINGS_FIXED_HEADERS)
    if dfs_site == "fanduel" and num_data_columns == len(_FRCS_FANDUEL_FIXED_HEADERS):
        return list(_FRCS_FANDUEL_FIXED_HEADERS)
    raw_labels = _frcs_dfs_merged_raw_labels(table, num_data_columns)
    if not raw_labels:
        for tr in table.find_all("tr"):
            ths = tr.find_all("th")
            if ths and not tr.find_all("td"):
                raw_labels = [_raw_header_text_from_th(th) for th in ths]
                break
    if dfs_site == "draftkings":
        snake = [_frcs_draftkings_header_to_snake(x) for x in raw_labels]
    elif dfs_site == "fanduel":
        snake = [_frcs_fanduel_header_to_snake(x) for x in raw_labels]
    else:
        snake = [_frcs_header_label_to_snake_case(x) for x in raw_labels]
    need = max(num_data_columns, len(snake))
    while len(snake) < need:
        snake.append(f"column_{len(snake)}")
    if num_data_columns and len(snake) > num_data_columns:
        snake = snake[:num_data_columns]
    return _frcs_unique_snake_headers(snake)


def _frcs_cell_text(cell: Tag) -> str:
    link = cell.find("a", href=True)
    if link is not None:
        return link.get_text(strip=True)
    return cell.get_text(strip=True)


def _convert_table_by_id_to_csv(
    html_file_path: str,
    csv_file_path: str,
    table_id: str,
    *,
    dfs_site: Literal["draftkings", "fanduel"] | None = None,
) -> None:
    with open(html_file_path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")
    table = soup.find("table", id=table_id)
    if table is None:
        logging.warning("FRCS table '%s' not found in %s", table_id, html_file_path)
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, [], [])
        return
    tbody = table.find("tbody")
    rows_src = tbody if tbody is not None else table
    rows: list[tuple[str, ...]] = []
    for tr in rows_src.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        vals = tuple(_frcs_cell_text(c) for c in cells)
        if vals:
            rows.append(vals)
    num_cols = max((len(r) for r in rows), default=0)
    headers = _frcs_table_headers_snake_case(table, num_cols, dfs_site=dfs_site)
    File.silent_remove(csv_file_path)
    File.write_csv(csv_file_path, rows, headers)


class FrcsHtmlToCsv:
    @staticmethod
    def convert_schedule(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
        tbody = soup.find("tbody")
        keys = [
            "event_number",
            "week_number",
            "week_url",
            "week_date",
            "week_track",
            "week_track_url",
            "week_race_name",
        ]
        if tbody is None:
            logging.warning("FRCS schedule: no tbody in %s", html_file_path)
            File.silent_remove(csv_file_path)
            File.write_csv(csv_file_path, [], keys)
            return
        csv_list: list = []
        event_number = 0
        for tr in tbody.find_all("tr"):
            tds = {
                td.get("data-column"): td
                for td in tr.find_all("td", attrs={"data-column": True})
            }
            race_td = tds.get(_FRCS_SCHEDULE_RACE_COL)
            if race_td is None:
                continue
            race_a = race_td.find("a", href=True)
            if race_a is None:
                continue
            num_td = tds.get(_FRCS_SCHEDULE_NUMBER_COL)
            if num_td is None:
                logging.warning("FRCS schedule: row missing # column (%r); skipped", _FRCS_SCHEDULE_NUMBER_COL)
                continue
            week_number_raw = _frcs_schedule_cell_sort_or_text(num_td)
            if not week_number_raw:
                logging.warning("FRCS schedule: empty # cell; skipped row with race %s", race_a.get_text(strip=True))
                continue
            event_number += 1
            week_url = race_a["href"].strip()
            week_race_name = race_a.get_text(strip=True)
            track_td = tds.get(_FRCS_SCHEDULE_TRACK_COL)
            track_a = track_td.find("a", href=True) if track_td else None
            week_track = track_a.get_text(strip=True) if track_a else ""
            week_track_url = track_a["href"].strip() if track_a and track_a.get("href") else ""
            date_td = tds.get(_FRCS_SCHEDULE_DATE_COL)
            week_date = ""
            if date_td is not None:
                ds = date_td.get("data-sort")
                if ds:
                    week_date = str(ds).strip()[:10]
                else:
                    week_date = date_td.get_text(strip=True)
            csv_list.append(
                (
                    str(event_number),
                    week_number_raw,
                    week_url,
                    week_date,
                    week_track,
                    week_track_url,
                    week_race_name,
                )
            )
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, keys)
        logging.info("FRCS: schedule %d rows -> %s", len(csv_list), csv_file_path)

    @staticmethod
    def convert_race_results(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
        table = soup.find("table", id="resultsTable")
        if table is None:
            # Some saved pages omit ``id=resultsTable``; pick table nearest a "Race Results"
            # heading and avoid unrelated sections like "Race Infractions".
            heading = soup.find(
                re.compile(r"^h[1-6]$"), string=re.compile(r"Race Results", re.I)
            )
            if heading is not None:
                table = heading.find_next("table")
            if table is None:
                for candidate in soup.find_all("table", class_=lambda c: c and "cupTable" in str(c)):
                    labels = _frcs_table_column_labels(candidate)
                    labels_norm = {re.sub(r"[^a-z0-9]+", "", x.lower()) for x in labels}
                    # Require race-results-like labels, not infractions columns.
                    if {"fin", "st", "driver"} <= labels_norm:
                        table = candidate
                        break
        headers: list[str] = []
        csv_list: list[tuple[str, ...]] = []
        if table is not None:
            raw_labels = _frcs_table_column_labels(table)
            headers = _frcs_unique_snake_headers(
                [_frcs_race_results_header_to_snake(x) for x in raw_labels]
            )
            tbody = table.find("tbody")
            rows_src = tbody if tbody is not None else table
            for tr in rows_src.find_all("tr"):
                cells = tr.find_all("td")
                if not cells:
                    continue
                row = tuple(_frcs_cell_text(cells[i]) if i < len(cells) else "" for i in range(len(headers)))
                if any(row):
                    csv_list.append(row)
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)

    @staticmethod
    def convert_entrylist(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as f:
            html = f.read()
        data = _parse_js_const_json_array(html, "entryListData")
        keys = ["number", "driver_name", "owner", "manufacturer", "crew_chief", "sponsor"]
        csv_list: list = []
        if data:
            for row in data:
                csv_list.append(
                    (
                        str(row.get("car_no", "")).strip(),
                        _strip_html(str(row.get("driver_name", ""))),
                        _strip_html(str(row.get("team", ""))),
                        str(row.get("make", "")).strip(),
                        _strip_html(str(row.get("crew_chief", ""))),
                        str(row.get("sponsor", "")).strip(),
                    )
                )
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, keys)

    @staticmethod
    def convert_practices(paths: list[str]) -> None:
        headers = [
            "rank",
            "driver_name",
            "number",
            "manufacturer",
            "lap_time",
            "lap_speed",
            "total_lap",
            "best_lap",
        ]
        for path in paths:
            with open(path, encoding="utf-8") as f:
                html = f.read()
            data = _parse_js_const_json_array(html, "practiceData")
            csv_list: list = []
            if data:
                for row in data:
                    csv_list.append(
                        (
                            str(row.get("session_rank", "")).strip(),
                            str(row.get("full_name", "")).strip(),
                            str(row.get("car_no", "")).strip(),
                            str(row.get("make", "")).strip(),
                            str(row.get("lap_time", "")).strip(),
                            str(row.get("lap_speed", "")).strip(),
                            str(row.get("total_lap", "")).strip(),
                            str(row.get("best_lap", "")).strip(),
                        )
                    )
            csv_file_path = path.replace(".html", ".csv")
            File.silent_remove(csv_file_path)
            File.write_csv(csv_file_path, csv_list, headers)

    @staticmethod
    def convert_qualifying(path: str | None) -> None:
        if path is None:
            return
        with open(path, encoding="utf-8") as f:
            html = f.read()
        r2 = _parse_js_const_json_array(html, "roundTwo")
        r1 = _parse_js_const_json_array(html, "roundOne")
        data = r2 if r2 else r1
        headers = ["rank", "driver_name", "number", "manufacturer", "lap_time", "lap_speed"]
        csv_list: list = []
        if data:
            for row in data:
                csv_list.append(
                    (
                        str(row.get("lap_rank", "")).strip(),
                        str(row.get("full_name", "")).strip(),
                        str(row.get("car_no", "")).strip(),
                        str(row.get("make", "")).strip(),
                        str(row.get("lap_time", "")).strip(),
                        str(row.get("lap_speed", "")).strip(),
                    )
                )
        csv_file_path = path.replace(".html", ".csv")
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)

    @staticmethod
    def convert_loop_data(path: str | None) -> None:
        if path is None:
            return
        with open(path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
        table = soup.find("table", id="loopDataTable")
        headers = [
            "driver_name",
            "start",
            "mid_race",
            "finish",
            "high_pos",
            "low_pos",
            "avg_pos",
            "pass_diff",
            "green_flag_passes",
            "green_flag_times_passed",
            "quality_passes",
            "pct_quality_passes",
            "fastest_lap",
            "top_15_laps",
            "pct_top_15_laps",
            "laps_led",
            "pct_laps_led",
            "total_laps",
            "driver_rating",
        ]
        if table is None:
            logging.warning("FRCS loop data: no #loopDataTable in %s", path)
            csv_file_path = path.replace(".html", ".csv")
            File.silent_remove(csv_file_path)
            File.write_csv(csv_file_path, [], headers)
            return
        labels = _frcs_table_column_labels(table)
        idx = {lab: i for i, lab in enumerate(labels)}

        def ix(key: str) -> int | None:
            return idx.get(key)

        tbody = table.find("tbody")
        rows_src = tbody if tbody is not None else table
        csv_list: list = []
        if ix("DRIVER") is None:
            logging.warning("FRCS loop data: missing DRIVER column %s", labels)
            csv_file_path = path.replace(".html", ".csv")
            File.silent_remove(csv_file_path)
            File.write_csv(csv_file_path, [], headers)
            return
        mapping = [
            ("DRIVER", "driver_name", True),
            ("ST", "start", False),
            ("MID", "mid_race", False),
            ("FIN", "finish", False),
            ("HI", "high_pos", False),
            ("LOW", "low_pos", False),
            ("ARP", "avg_pos", False),
            ("PL DIF", "pass_diff", False),
            ("GFP", "green_flag_passes", False),
            ("GFxP", "green_flag_times_passed", False),
            ("QPS", "quality_passes", False),
            ("% QPS", "pct_quality_passes", False),
            ("FST", "fastest_lap", False),
            ("% T15", "pct_top_15_laps", False),
            ("LED", "laps_led", False),
            ("% LED", "pct_laps_led", False),
            ("LAPS", "total_laps", False),
            ("DR", "driver_rating", False),
        ]
        for tr in rows_src.find_all("tr"):
            cells = tr.find_all("td")
            if not cells:
                continue
            out: dict[str, str] = {h: "" for h in headers}
            out["top_15_laps"] = ""
            for label, out_key, is_driver in mapping:
                i = ix(label)
                if i is None or i >= len(cells):
                    continue
                if is_driver:
                    c = cells[i]
                    a = c.find("a", href=True)
                    out[out_key] = a.get_text(strip=True) if a else _frcs_cell_text(c)
                else:
                    out[out_key] = _frcs_cell_text(cells[i])
            if out["driver_name"]:
                csv_list.append(tuple(out[k] for k in headers))
        csv_file_path = path.replace(".html", ".csv")
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)

    @staticmethod
    def convert_prerace_penalties(html_file_path: str, csv_file_path: str) -> None:
        with open(html_file_path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
        h4 = soup.find("h4", string=re.compile(r"Race Infractions", re.I))
        table = h4.find_next("table", class_=lambda c: c and "cupTable" in str(c)) if h4 else None
        headers_csv = ["name", "number", "reason"]
        csv_list: list = []
        if table is not None:
            tbody = table.find("tbody")
            for tr in (tbody.find_all("tr") if tbody else table.find_all("tr")):
                cells = tr.find_all("td")
                if len(cells) < 3:
                    continue
                driver = cells[0].get_text(strip=True)
                infraction = cells[1].get_text(strip=True)
                if "pre-race" not in infraction.lower():
                    continue
                reason = infraction
                csv_list.append((driver, "", reason))
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers_csv)

    @staticmethod
    def convert_caution_flags(html_file_path: str, csv_file_path: str) -> None:
        headers_csv = [
            "condition",
            "from_lap",
            "to_lap",
            "number_of_laps",
            "reason",
            "free_pass_cars",
        ]
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, [], headers_csv)

    @staticmethod
    def convert_lap_leaders(html_file_path: str, csv_file_path: str) -> None:
        headers_csv = ["name", "from_lap", "to_lap", "number_of_laps"]
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, [], headers_csv)

    @staticmethod
    def convert_penalties(path: str | None) -> None:
        if path is None:
            return

        def _norm(label: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", label.lower())

        def _first_idx(labels: list[str], choices: set[str]) -> int | None:
            for i, label in enumerate(labels):
                if _norm(label) in choices:
                    return i
            return None

        def _val(cells: list, idx: int | None) -> str:
            if idx is None or idx >= len(cells):
                return ""
            return cells[idx].get_text(strip=True)

        with open(path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")

        heading = soup.find(re.compile(r"^h[1-6]$"), string=re.compile(r"Race Infractions", re.I))
        table = heading.find_next("table") if heading else None
        headers_csv = ["number", "driver_name", "lap", "flag", "infraction", "penalty"]
        csv_list: list = []
        if table is not None:
            header_cells = table.find_all("th")
            header_labels = [h.get_text(strip=True) for h in header_cells]
            idx_number = _first_idx(header_labels, {"number", "carno", "carnumber", "#", "no"})
            idx_driver = _first_idx(header_labels, {"driver", "drivername", "name"})
            idx_lap = _first_idx(header_labels, {"lap", "laps"})
            idx_flag = _first_idx(header_labels, {"flag"})
            idx_infraction = _first_idx(header_labels, {"infraction", "violation", "reason"})
            idx_penalty = _first_idx(header_labels, {"penalty", "action"})
            tbody = table.find("tbody")
            for tr in (tbody.find_all("tr") if tbody else table.find_all("tr")):
                cells = tr.find_all("td")
                if len(cells) < 3:
                    continue
                number = _val(cells, idx_number)
                driver_name = _val(cells, idx_driver)
                lap = _val(cells, idx_lap)
                flag = _val(cells, idx_flag)
                infraction = _val(cells, idx_infraction)
                penalty = _val(cells, idx_penalty)

                # Fallbacks for FRCS variants with no/partial headers.
                if not driver_name:
                    driver_name = cells[0].get_text(strip=True)
                if not infraction and len(cells) > 1:
                    infraction = cells[1].get_text(strip=True)
                if not penalty and len(cells) > 2:
                    penalty = cells[2].get_text(strip=True)
                if not lap and len(cells) > 3:
                    lap = cells[3].get_text(strip=True)
                if not flag and len(cells) > 5:
                    flag = cells[5].get_text(strip=True)

                if any((number, driver_name, lap, flag, infraction, penalty)):
                    csv_list.append((number, driver_name, lap, flag, infraction, penalty))
        csv_file_path = path.replace(".html", ".csv")
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers_csv)

    @staticmethod
    def convert_lineup(path: str | None) -> None:
        if path is None:
            return
        with open(path, encoding="utf-8") as f:
            html = f.read()
        data = _parse_js_const_json_array(html, "lineUpData")
        headers = [
            "start",
            "number",
            "driver_name",
            "manufacturer",
            "sponsor",
            "team",
            "crew_chief",
        ]
        rows: list[tuple[str, ...]] = []
        if data:
            for row in data:
                rows.append(
                    (
                        str(row.get("start", "")).strip(),
                        str(row.get("car_no", "")).strip(),
                        str(row.get("driver_name", "")).strip(),
                        str(row.get("make", "")).strip(),
                        str(row.get("sponsor", "")).strip(),
                        str(row.get("team", "")).strip(),
                        str(row.get("crew_chief", "")).strip(),
                    )
                )
        csv_file_path = path.replace(".html", ".csv")
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, rows, headers)

    @staticmethod
    def convert_pit_stops(path: str | None) -> None:
        if path is None:
            return
        with open(path, encoding="utf-8") as f:
            html = f.read()
        summary = _parse_js_const_json_array(html, "pitStopSummary")
        detail = _parse_js_const_json_array(html, "pitStopDetail")
        summary_headers = [
            "rank",
            "number",
            "driver_name",
            "stops",
            "start",
            "finish",
            "status",
            "avg_driver",
            "avg_crew",
            "avg_stop",
            "fastest_stop",
            "slowest_stop",
        ]
        summary_rows: list[tuple[str, ...]] = []
        if summary:
            for row in summary:
                summary_rows.append(
                    (
                        str(row.get("rank", "")).strip(),
                        str(row.get("vehicle_number", "")).strip(),
                        str(row.get("full_name", "")).strip(),
                        str(row.get("stops", "")).strip(),
                        str(row.get("start", "")).strip(),
                        str(row.get("finish", "")).strip(),
                        str(row.get("status", "")).strip(),
                        str(row.get("avg_driver", "")).strip(),
                        str(row.get("avg_crew", "")).strip(),
                        str(row.get("avg_stop", "")).strip(),
                        str(row.get("fastest_stop", "")).strip(),
                        str(row.get("slowest_stop", "")).strip(),
                    )
                )
        detail_headers = [
            "number",
            "driver_name",
            "leader_lap",
            "pitter_lap",
            "type",
            "stop_duration",
            "driver_time",
            "total_duration",
        ]
        detail_rows: list[tuple[str, ...]] = []
        if detail:
            for row in detail:
                detail_rows.append(
                    (
                        str(row.get("vehicle_number", "")).strip(),
                        str(row.get("full_name", "")).strip(),
                        str(row.get("leader_lap", "")).strip(),
                        str(row.get("pitter_lap", "")).strip(),
                        str(row.get("type", "")).strip(),
                        str(row.get("stop_duration", "")).strip(),
                        str(row.get("driver_time", "")).strip(),
                        str(row.get("total_duration", "")).strip(),
                    )
                )
        base = path.replace(".html", "")
        File.silent_remove(f"{base}-summary.csv")
        File.write_csv(f"{base}-summary.csv", summary_rows, summary_headers)
        File.silent_remove(f"{base}-detail.csv")
        File.write_csv(f"{base}-detail.csv", detail_rows, detail_headers)

    @staticmethod
    def convert_draftkings_points(path: str | None) -> None:
        if path is None:
            return
        _convert_table_by_id_to_csv(
            path, path.replace(".html", ".csv"), "draftkingsTable", dfs_site="draftkings"
        )

    @staticmethod
    def convert_fanduel_points(path: str | None) -> None:
        if path is None:
            return
        _convert_table_by_id_to_csv(
            path, path.replace(".html", ".csv"), "fanduelTable", dfs_site="fanduel"
        )


class FrcsHtmlConverters:
    def convert_schedule(self, html_file_path: str, csv_file_path: str) -> None:
        FrcsHtmlToCsv.convert_schedule(html_file_path, csv_file_path)

    def convert_race_results(self, html_file_path: str, csv_file_path: str) -> None:
        FrcsHtmlToCsv.convert_race_results(html_file_path, csv_file_path)

    def convert_prerace_penalties(self, html_file_path: str, csv_file_path: str) -> None:
        FrcsHtmlToCsv.convert_prerace_penalties(html_file_path, csv_file_path)

    def convert_caution_flags(self, html_file_path: str, csv_file_path: str) -> None:
        FrcsHtmlToCsv.convert_caution_flags(html_file_path, csv_file_path)

    def convert_lap_leaders(self, html_file_path: str, csv_file_path: str) -> None:
        FrcsHtmlToCsv.convert_lap_leaders(html_file_path, csv_file_path)

    def convert_entrylist(self, html_file_path: str, csv_file_path: str) -> None:
        FrcsHtmlToCsv.convert_entrylist(html_file_path, csv_file_path)

    def convert_practices(self, paths: list[str]) -> None:
        FrcsHtmlToCsv.convert_practices(paths)

    def convert_qualifying(self, path: str | None) -> None:
        FrcsHtmlToCsv.convert_qualifying(path)

    def convert_loop_data(self, path: str | None) -> None:
        FrcsHtmlToCsv.convert_loop_data(path)

    def convert_penalties(self, path: str | None) -> None:
        FrcsHtmlToCsv.convert_penalties(path)

    def convert_lineup(self, path: str | None) -> None:
        FrcsHtmlToCsv.convert_lineup(path)

    def convert_pit_stops(self, path: str | None) -> None:
        FrcsHtmlToCsv.convert_pit_stops(path)

    def convert_draftkings_points(self, path: str | None) -> None:
        FrcsHtmlToCsv.convert_draftkings_points(path)

    def convert_fanduel_points(self, path: str | None) -> None:
        FrcsHtmlToCsv.convert_fanduel_points(path)
