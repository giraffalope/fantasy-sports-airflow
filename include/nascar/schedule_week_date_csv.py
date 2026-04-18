"""
Normalize ``week_date`` strings written to ``schedule.csv`` from FRCS / Racing Reference HTML.

When the source provides a **timezone-aware** instant (ISO with ``Z`` / ``±HH:MM``), convert to
**UTC** and emit ``YYYY-MM-DD`` for that instant's calendar day in UTC. Naive datetimes use the
embedded calendar date (no invented timezone). Plain ``YYYY-MM-DD`` uses the first 10 characters.
"""

from __future__ import annotations

from datetime import UTC, datetime


def week_date_for_schedule_csv(data_sort: str | None, cell_text_fallback: str = "") -> str:
    """
    Return ``week_date`` cell value for schedule CSV output.

    Prefer ``data_sort`` when present; otherwise returns ``cell_text_fallback`` stripped (caller
    may still parse ``mm/dd/yy``).
    """
    raw = (data_sort or "").strip()
    if not raw:
        return (cell_text_fallback or "").strip()
    parsed = _parse_sort_value_to_utc_calendar_date(raw)
    if parsed is not None:
        return parsed
    if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
        return raw[:10]
    return ""


def _parse_sort_value_to_utc_calendar_date(raw: str) -> str | None:
    s = raw.strip()
    if not s:
        return None

    # Tabulator-style millisecond epoch (typical length 13).
    if s.isdigit() and len(s) >= 13:
        try:
            dt = datetime.fromtimestamp(int(s) / 1000.0, tz=UTC)
            return dt.date().isoformat()
        except (OSError, OverflowError, ValueError):
            return None

    candidates: list[str] = [s, s.replace("Z", "+00:00")]
    if "T" not in s and " " in s and len(s) > 10:
        t = s.replace(" ", "T", 1)
        candidates.extend((t, t.replace("Z", "+00:00")))

    for cand in candidates:
        try:
            dt = datetime.fromisoformat(cand)
        except ValueError:
            continue
        if dt.tzinfo is not None:
            return dt.astimezone(UTC).date().isoformat()
        # Naive: only treat as datetime (not bare date) if time component present.
        if "T" in cand or (" " in cand and len(cand) > 10):
            return dt.date().isoformat()

    return None
