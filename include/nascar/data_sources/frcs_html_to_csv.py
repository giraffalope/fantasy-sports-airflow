"""HTML / embedded JSON → CSV parsers for frcs.pro (NASCAR Cup schedule, race weekend pages)."""

from __future__ import annotations

import json
import logging
import os
import re
from html import unescape
from typing import Any, Literal

from bs4 import BeautifulSoup, Tag

from include.utils.file import File

_FRCS_SCHEDULE_RACE_COL = "race"
_FRCS_SCHEDULE_TRACK_COL = "track"
_FRCS_SCHEDULE_DATE_COL = "date"
# Tabulator column for the schedule table header shown as ``#`` (race index in season).
_FRCS_SCHEDULE_NUMBER_COL = "number"
# ``td`` cells may be ``class="col"`` or ``class="col odd"`` (Racing Reference / saved FRCS HTML).
_FRCS_TD_COL_CLASS = re.compile(r"(^|\s)col(\s|$)")

# Try-order for embedded qualifying JSON (naming varies by era / bundle).
_FRCS_QUALIFYING_JSON_CONST_NAMES: tuple[str, ...] = (
    "roundTwo",
    "roundOne",
    "qualifyingData",
    "round_two",
    "round_one",
    "qualifying_data",
    "round2",
    "round1",
)


def _frcs_class_has(class_attr: str | list[str] | None, token: str) -> bool:
    if not class_attr:
        return False
    if isinstance(class_attr, str):
        return token in class_attr.split()
    return token in class_attr


def _frcs_schedule_cell_sort_or_text(td: Tag) -> str:
    ds = td.get("data-sort")
    if ds is not None and str(ds).strip():
        return str(ds).strip()
    return td.get_text(strip=True)


def _strip_html(fragment: str) -> str:
    return BeautifulSoup(unescape(fragment), "html.parser").get_text(strip=True)


def _parse_js_const_json_array(html: str, const_name: str) -> list[dict] | None:
    """Parse ``const``/``let``/``var`` *name* = … from an FRCS inline script blob.

    Uses :class:`json.JSONDecoder` ``raw_decode`` so brackets inside JSON strings do not
    break extraction, and ``const roundTwo = null`` does not steal the next ``[`` from
    ``roundOne``. JavaScript ``null`` is treated like an empty array for round-style data.
    """
    pattern = re.compile(
        rf"(?:const|let|var)\s+{re.escape(const_name)}\s*=\s*",
        re.IGNORECASE | re.MULTILINE,
    )
    decoder = json.JSONDecoder()
    for m in pattern.finditer(html):
        pos = m.end()
        while pos < len(html) and html[pos] in " \t\n\r":
            pos += 1
        if pos >= len(html):
            continue
        try:
            obj, _end = decoder.raw_decode(html, pos)
        except json.JSONDecodeError:
            continue
        if obj is None:
            return []
        if isinstance(obj, list):
            return obj
    return None


def _frcs_tabulator_qual_rows_from_root(
    root: Tag, *, require_time_or_rank: bool = False
) -> list[dict[str, str]]:
    rows_out: list[dict[str, str]] = []
    for row in root.select("div.tabulator-row"):
        classes = row.get("class") or []
        if "tabulator-calcs-holder" in classes:
            continue
        row_dict: dict[str, str] = {}
        for cell in row.select("div.tabulator-cell"):
            field = cell.get("tabulator-field") or cell.get("data-field")
            if not field:
                continue
            row_dict[str(field)] = _frcs_cell_text(cell)
        has_driverish = any(
            row_dict.get(k)
            for k in (
                "lap_rank",
                "full_name",
                "rank",
                "pos",
                "position",
                "driver_name",
                "driver",
                "name",
            )
        )
        if not has_driverish:
            continue
        if require_time_or_rank:
            has_time_or_speed = any(
                row_dict.get(k) for k in ("lap_time", "time", "lap_speed", "speed", "best_lap", "best_speed")
            )
            has_rankish = any(row_dict.get(k) for k in ("lap_rank", "rank", "pos", "position"))
            if not (has_time_or_speed or has_rankish):
                continue
        rows_out.append(row_dict)
    return rows_out


def _frcs_qualifying_rows_from_tabulator_div(soup: BeautifulSoup) -> list[dict[str, str]] | None:
    """Fallback when embedded ``roundOne``/``roundTwo`` JSON is missing (e.g. saved DOM)."""
    for div_id in ("roundTwoTable", "roundOneTable"):
        root = soup.find(id=div_id)
        if root is None:
            continue
        rows_out = _frcs_tabulator_qual_rows_from_root(root)
        if rows_out:
            return rows_out
    return None


def _frcs_qualifying_rows_from_generic_tabulator(soup: BeautifulSoup) -> list[dict[str, str]] | None:
    """Scan any ``div.tabulator`` with qualifying-like cells (ids other than round one/two tables)."""
    best: list[dict[str, str]] | None = None
    best_n = 0
    for root in soup.find_all("div", class_=lambda c: _frcs_class_has(c, "tabulator")):
        if not root.select_one("div.tabulator-row"):
            continue
        rid = root.get("id")
        if rid in ("roundTwoTable", "roundOneTable"):
            continue
        rows_out = _frcs_tabulator_qual_rows_from_root(root, require_time_or_rank=True)
        if len(rows_out) > best_n:
            best_n = len(rows_out)
            best = rows_out
    return best if best_n else None


def _frcs_qualifying_row_to_tuple(row: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    """Map FRCS / RR-style qualifying row keys onto the six CSV columns."""

    def first(*keys: str) -> str:
        for k in keys:
            v = row.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
        return ""

    rank = first("lap_rank", "rank", "pos", "position", "rk", "qual_rank")
    driver_raw = first("full_name", "driver_name", "driver", "name")
    driver = _strip_html(driver_raw) if driver_raw else driver_raw
    num = first("car_no", "number", "car_number", "no")
    make = first("make", "manufacturer", "manuf")
    lt = first("lap_time", "time", "best_lap")
    ls = first("lap_speed", "speed", "best_speed")
    return (rank, driver, num, make, lt, ls)


def _frcs_qual_dict_from_col_td_sequence(cols: list[Tag]) -> dict[str, str] | None:
    """Map a 5–8 column RR-style qualifying row (``td`` list) to internal keys."""
    if len(cols) < 5 or len(cols) > 14:
        return None
    rank = cols[0].get_text(strip=True)
    if not rank:
        return None
    driver_name = ""
    if len(cols) > 1:
        a1 = cols[1].find("a")
        driver_name = a1.get_text(strip=True) if a1 is not None else cols[1].get_text(strip=True)
    if not driver_name.strip():
        return None
    number = cols[2].get_text(strip=True) if len(cols) > 2 else ""
    manufacturer = cols[3].get_text(strip=True) if len(cols) > 3 else ""
    lap_time = cols[4].get_text(strip=True) if len(cols) > 4 else ""
    lap_speed = cols[5].get_text(strip=True) if len(cols) > 5 else ""
    return {
        "lap_rank": rank,
        "full_name": driver_name,
        "car_no": number,
        "make": manufacturer,
        "lap_time": lap_time,
        "lap_speed": lap_speed,
    }


def _frcs_qual_result_tbl_data_rows(table: Tag) -> list[dict[str, str]]:
    rows_out: list[dict[str, str]] = []
    for tr in table.find_all("tr", class_=["odd", "even"]):
        cols = tr.find_all("td", class_=_FRCS_TD_COL_CLASS)
        if not cols:
            continue
        rowd = _frcs_qual_dict_from_col_td_sequence(cols)
        if rowd:
            rows_out.append(rowd)
    return rows_out


def _frcs_qualifying_rows_from_rr_like_tables(soup: BeautifulSoup) -> list[dict[str, str]] | None:
    """Odd/even rows with ``col`` ``td`` cells but no ``qualResultTbl`` class (legacy FRCS saves)."""
    best: list[dict[str, str]] | None = None
    best_n = 0
    for table in soup.find_all("table"):
        if _frcs_class_has(table.get("class"), "qualResultTbl"):
            continue
        rows_out: list[dict[str, str]] = []
        for tr in table.find_all("tr"):
            if tr.find("th"):
                continue
            cols = tr.find_all("td", class_=_FRCS_TD_COL_CLASS)
            if len(cols) < 5:
                continue
            cl = tr.get("class") or []
            if isinstance(cl, str):
                cl = cl.split()
            is_oe = "odd" in cl or "even" in cl
            rowd = _frcs_qual_dict_from_col_td_sequence(cols)
            if rowd is None:
                continue
            if is_oe or cols[1].find("a") is not None:
                rows_out.append(rowd)
        if len(rows_out) > best_n:
            best_n = len(rows_out)
            best = rows_out
    return best if best_n >= 1 else None


def _frcs_qualifying_rows_from_thead_table(soup: BeautifulSoup) -> list[dict[str, str]] | None:
    """Bootstrap / static pages: ``thead`` with Driver + Time or Speed columns."""
    best: list[dict[str, str]] | None = None
    best_n = 0
    for table in soup.find_all("table"):
        thead = table.find("thead")
        if thead is None:
            continue
        hr = thead.find("tr")
        if hr is None:
            continue
        ths = hr.find_all("th")
        if len(ths) < 4:
            continue
        labels = [re.sub(r"\s+", " ", th.get_text(strip=True)).lower() for th in ths]

        def col_idx(*needles: str) -> int | None:
            for i, lab in enumerate(labels):
                for n in needles:
                    if n in lab:
                        return i
            return None

        i_drv = col_idx("driver", "name")
        if i_drv is None:
            continue
        i_time = col_idx("time", "sec", "lap")
        i_spd = col_idx("speed", "mph")
        if i_time is None and i_spd is None:
            continue
        i_rk = col_idx("pos", "rk", "qual")
        i_no: int | None = None
        for i, lab in enumerate(labels):
            s = lab.strip()
            if s in {"#", "no.", "no", "car #", "car no", "number"}:
                i_no = i
                break
        i_mk = col_idx("car", "mfr", "make", "manuf")

        idxs = [i_drv] + [x for x in (i_time, i_spd, i_rk, i_no, i_mk) if x is not None]
        need_max = max(idxs)
        tbody = table.find("tbody") or table
        rows_out: list[dict[str, str]] = []
        for tr in tbody.find_all("tr"):
            if tr.find("th"):
                continue
            tds = tr.find_all("td")
            if len(tds) <= need_max:
                continue

            def cell(i: int | None) -> str:
                if i is None or i >= len(tds):
                    return ""
                return tds[i].get_text(strip=True)

            rank = cell(i_rk) if i_rk is not None else ""
            dc = tds[i_drv]
            a = dc.find("a")
            driver_name = a.get_text(strip=True) if a else dc.get_text(strip=True)
            if not driver_name.strip():
                continue
            num = cell(i_no) if i_no is not None else ""
            make = cell(i_mk) if i_mk is not None else ""
            lt = cell(i_time) if i_time is not None else ""
            ls = cell(i_spd) if i_spd is not None else ""
            rows_out.append(
                {
                    "lap_rank": rank,
                    "full_name": driver_name,
                    "car_no": num,
                    "make": make,
                    "lap_time": lt,
                    "lap_speed": ls,
                }
            )
        if len(rows_out) > best_n:
            best_n = len(rows_out)
            best = rows_out
    return best if best_n >= 1 else None


def _frcs_qualifying_rows_from_qual_result_tbl(soup: BeautifulSoup) -> list[dict[str, str]] | None:
    """Legacy ``table.qualResultTbl`` layout (Racing Reference; sometimes saved as FRCS ``qualifying.html``)."""
    tables = soup.find_all("table", class_=lambda c: _frcs_class_has(c, "qualResultTbl"))
    if not tables:
        return None
    for strict in (True, False):
        for table in tables:
            newhead = table.find("td", class_=lambda c: _frcs_class_has(c, "newhead"))
            h = newhead.get_text().strip().lower() if newhead else ""
            if "qualifying" not in h:
                continue
            if strict and "results" not in h:
                continue
            rows = _frcs_qual_result_tbl_data_rows(table)
            if rows:
                return rows
    best: list[dict[str, str]] | None = None
    best_n = 0
    for table in tables:
        rows = _frcs_qual_result_tbl_data_rows(table)
        if len(rows) > best_n:
            best_n = len(rows)
            best = rows
    return best if best_n else None


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


def _sort_csv_rows_by_column_numeric(
    rows: list[tuple[str, ...]],
    headers: list[str],
    col: str,
    *,
    reverse: bool = False,
) -> None:
    if not rows or col not in headers:
        return
    idx = headers.index(col)

    def sort_key(row: tuple[str, ...]) -> float:
        if idx >= len(row):
            return float("-inf") if reverse else float("inf")
        s = row[idx].strip().replace(",", "")
        if not s or s in {"-", "—"}:
            return float("-inf") if reverse else float("inf")
        try:
            return float(s)
        except ValueError:
            return float("-inf") if reverse else float("inf")

    rows.sort(key=sort_key, reverse=reverse)


def _frcs_dfs_strip_salary_commas_and_sort_desc(
    rows: list[tuple[str, ...]],
    headers: list[str],
) -> None:
    if not rows or "salary" not in headers:
        return
    si = headers.index("salary")
    for i, row in enumerate(rows):
        if si >= len(row):
            continue
        cells = list(row)
        cells[si] = cells[si].replace(",", "")
        rows[i] = tuple(cells)
    _sort_csv_rows_by_column_numeric(rows, headers, "salary", reverse=True)


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
    if dfs_site is not None:
        _frcs_dfs_strip_salary_commas_and_sort_desc(rows, headers)
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
        pos_col = "position" if "position" in headers else "finish"
        _sort_csv_rows_by_column_numeric(csv_list, headers, pos_col, reverse=False)
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
        def _practice_cell(row: dict, key: str) -> str:
            v = row.get(key)
            if v is None:
                return ""
            return str(v).strip()

        # ``practiceData`` tabulator fields: time5..time30 (multi-lap averages), lap10_speed / lap10_from /
        # lap10_to (10-lap avg group — distinct from single-best-lap lap_speed).
        headers = [
            "rank",
            "driver_name",
            "number",
            "manufacturer",
            "lap_time",
            "lap_speed",
            "total_lap",
            "best_lap",
            "5_lap_average_time",
            "10_lap_average_time",
            "20_lap_average_time",
            "25_lap_average_time",
            "30_lap_average_time",
            "10_lap_average_speed",
            "10_lap_start",
            "10_lap_end",
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
                            _practice_cell(row, "session_rank"),
                            _practice_cell(row, "full_name"),
                            _practice_cell(row, "car_no"),
                            _practice_cell(row, "make"),
                            _practice_cell(row, "lap_time"),
                            _practice_cell(row, "lap_speed"),
                            _practice_cell(row, "total_lap"),
                            _practice_cell(row, "best_lap"),
                            _practice_cell(row, "time5"),
                            _practice_cell(row, "time10"),
                            _practice_cell(row, "time20"),
                            _practice_cell(row, "time25"),
                            _practice_cell(row, "time30"),
                            _practice_cell(row, "lap10_speed"),
                            _practice_cell(row, "lap10_from"),
                            _practice_cell(row, "lap10_to"),
                        )
                    )
            _sort_csv_rows_by_column_numeric(csv_list, headers, "rank", reverse=False)
            csv_file_path = path.replace(".html", ".csv")
            File.silent_remove(csv_file_path)
            File.write_csv(csv_file_path, csv_list, headers)

    @staticmethod
    def convert_qualifying(path: str | None) -> None:
        if path is None:
            return
        with open(path, encoding="utf-8") as f:
            html = f.read()
        data: list[Any] | None = None
        for const_name in _FRCS_QUALIFYING_JSON_CONST_NAMES:
            arr = _parse_js_const_json_array(html, const_name)
            if arr:
                data = arr
                break
        soup: BeautifulSoup | None = None
        if not data:
            soup = BeautifulSoup(html, "html.parser")
            dom_rows = _frcs_qualifying_rows_from_tabulator_div(soup)
            if dom_rows:
                data = dom_rows
        if not data:
            if soup is None:
                soup = BeautifulSoup(html, "html.parser")
            gen_rows = _frcs_qualifying_rows_from_generic_tabulator(soup)
            if gen_rows:
                data = gen_rows
        if not data:
            if soup is None:
                soup = BeautifulSoup(html, "html.parser")
            tbl_rows = _frcs_qualifying_rows_from_qual_result_tbl(soup)
            if tbl_rows:
                data = tbl_rows
        if not data:
            if soup is None:
                soup = BeautifulSoup(html, "html.parser")
            rr_like = _frcs_qualifying_rows_from_rr_like_tables(soup)
            if rr_like:
                data = rr_like
        if not data:
            if soup is None:
                soup = BeautifulSoup(html, "html.parser")
            thead_rows = _frcs_qualifying_rows_from_thead_table(soup)
            if thead_rows:
                data = thead_rows
        headers = ["rank", "driver_name", "number", "manufacturer", "lap_time", "lap_speed"]
        csv_list: list = []
        if data:
            for row in data:
                if isinstance(row, dict):
                    csv_list.append(_frcs_qualifying_row_to_tuple(row))
        csv_file_path = path.replace(".html", ".csv")
        if not csv_list:
            logging.warning(
                "FRCS qualifying: no extractable qualifying rows in %s; trying Racing Reference parser",
                path,
            )
            from include.nascar.data_sources.racing_reference_html_to_csv import RacingReferenceHtmlToCsv

            RacingReferenceHtmlToCsv.convert_qualifying(path)
            return
        _sort_csv_rows_by_column_numeric(csv_list, headers, "rank", reverse=False)
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
        _sort_csv_rows_by_column_numeric(csv_list, headers, "finish", reverse=False)
        csv_file_path = path.replace(".html", ".csv")
        File.silent_remove(csv_file_path)
        File.write_csv(csv_file_path, csv_list, headers)

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

        def _frcs_race_infractions_heading(soup: BeautifulSoup) -> Tag | None:
            for tag in soup.find_all(re.compile(r"^h[1-6]$")):
                if "race infractions" in tag.get_text(strip=True).lower():
                    return tag
            return None

        with open(path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")

        heading = _frcs_race_infractions_heading(soup)
        table = None
        if heading is not None:
            cand = heading.find_next("table", class_=lambda c: c and "cupTable" in str(c))
            table = cand if cand is not None else heading.find_next("table")
        headers_csv = ["driver_name", "lap", "flag", "infraction", "penalty"]
        csv_list: list = []
        if table is not None:
            thead = table.find("thead")
            header_row = thead.find("tr") if thead is not None else None
            header_cells = header_row.find_all("th") if header_row else table.find_all("th")
            header_labels = [h.get_text(strip=True) for h in header_cells]
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

                if any((driver_name, lap, flag, infraction, penalty)):
                    csv_list.append((driver_name, lap, flag, infraction, penalty))
        if os.path.basename(path).lower() == "race-results.html":
            csv_file_path = os.path.join(os.path.dirname(path), "penalties.csv")
        else:
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
        """FRCS does not surface pre-race infractions; satisfy ``HtmlConverters`` only."""
        del html_file_path
        File.silent_remove(csv_file_path)

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
