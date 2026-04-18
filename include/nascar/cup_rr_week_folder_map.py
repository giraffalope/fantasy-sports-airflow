"""
Map Cup ``nascar.weeks`` rows (FRCS ``event_number`` / folder index) to Racing Reference disk
folders (RR ``schedule.csv`` ``week_number``).

Racing Reference week artifacts live under ``…/cup/<year>/<week_number:02d>/`` where
``week_number`` comes from the RR schedule page, which may differ from FRCS sequential
``event_number`` used for ``weeks.number`` and FRCS paths.
"""

from __future__ import annotations

import difflib
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from include.nascar.data_sources.racing_reference_race_week import parse_schedule_week_date
from include.utils.file import File

_LOG = logging.getLogger(__name__)

# Strip trailing sponsor / presentation clauses for looser title matching.
_PRESENTED_BY_RE = re.compile(
    r"\s+(presented\s+by|sponsored\s+by)\b.*$",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class RrScheduleRow:
    """One Racing Reference ``schedule.csv`` data row (folder index + identity)."""

    week_number: int
    week_date_raw: str
    week_race_name: str
    week_url: str


def normalize_race_name(name: str | None) -> str:
    """Lowercase, collapse whitespace, trim common suffix noise."""
    if not name:
        return ""
    s = str(name).strip().lower()
    s = _PRESENTED_BY_RE.sub("", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _scheduled_start_to_date(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    return parse_schedule_week_date(str(raw))


def load_racing_reference_schedule_rows(schedule_csv_path: str) -> list[RrScheduleRow]:
    """
    Parse RR ``schedule.csv`` into rows with a non-empty ``week_url`` (race week exists on site).

    Does not apply scrape-horizon filtering — importer / backfill needs the full season.
    """
    if not os.path.isfile(schedule_csv_path):
        return []
    try:
        dict_rows = File.read_csv(filename=schedule_csv_path, as_dict=True)
    except Exception as exc:
        _LOG.warning("Cup RR week map: could not read %s: %s", schedule_csv_path, exc)
        return []
    out: list[RrScheduleRow] = []
    for row in dict_rows:
        week_url = (row.get("week_url") or "").strip()
        if not week_url:
            continue
        try:
            week_number = int(str(row.get("week_number", "")).strip())
        except (TypeError, ValueError):
            _LOG.warning("Cup RR week map: bad week_number in row %s", row)
            continue
        week_date_raw = (row.get("week_date") or "").strip()
        week_race_name = (row.get("week_race_name") or "").strip()
        out.append(
            RrScheduleRow(
                week_number=week_number,
                week_date_raw=week_date_raw,
                week_race_name=week_race_name,
                week_url=week_url,
            )
        )
    return out


def _name_similarity(a_norm: str, b_norm: str) -> float:
    if not a_norm or not b_norm:
        return 0.0
    if a_norm == b_norm:
        return 1.0
    return difflib.SequenceMatcher(None, a_norm, b_norm).ratio()


def match_rr_week_number(
    rows: list[RrScheduleRow],
    *,
    scheduled_start: Any,
    race_name: str | None,
) -> int | None:
    """
    Return RR ``week_number`` (disk folder index) for the DB week, or ``None`` if no confident match.

    Matches when RR ``week_date`` is the same calendar day as DB ``scheduled_start``, or within
    **±1 day** (sources often differ by one day). Then race title similarity disambiguates.
    """
    target_date = _scheduled_start_to_date(scheduled_start)
    if target_date is None:
        return None
    target_name = normalize_race_name(race_name)
    date_window: list[RrScheduleRow] = []
    for r in rows:
        wd = parse_schedule_week_date(r.week_date_raw)
        if wd is None:
            continue
        if abs((wd - target_date).days) <= 1:
            date_window.append(r)
    if not date_window:
        return None
    if len(date_window) == 1:
        return date_window[0].week_number

    scored: list[tuple[float, int, str]] = []
    for r in date_window:
        rr_norm = normalize_race_name(r.week_race_name)
        score = _name_similarity(target_name, rr_norm)
        scored.append((score, r.week_number, r.week_race_name))
    scored.sort(key=lambda t: (-t[0], t[1]))
    best_score, best_wn, best_title = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else -1.0

    if best_score >= 0.92:
        return best_wn
    if best_score >= 0.75 and (best_score - second_score) >= 0.08:
        return best_wn

    _LOG.warning(
        "Cup RR week map: ambiguous RR folder for date=%s db_name=%r (best_rr=%r score=%.2f second=%.2f); "
        "falling back to FRCS event index for RR paths",
        target_date.isoformat(),
        race_name,
        best_title,
        best_score,
        second_score,
    )
    return None
