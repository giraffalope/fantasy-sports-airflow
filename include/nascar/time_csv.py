"""Lap-time strings for CSV: normalize to ``HH:MM:SS.mmm`` where parseable."""

from __future__ import annotations

import json
import os
import re
import time

_DEBUG_LOG = "/home/jackalope/bin/.cursor/debug-ae744d.log"
_SESSION_ID = "ae744d"


def _agent_log(hypothesis_id: str, message: str, data: dict) -> None:
    if os.environ.get("CURSOR_DEBUG_TIME") != "1":
        return
    try:
        payload = {
            "sessionId": _SESSION_ID,
            "hypothesisId": hypothesis_id,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with open(_DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError:
        pass


def normalize_time_to_hhmmss_mmm(value: str) -> str:
    """Parse common lap-time tokens; return ``HH:MM:SS.mmm`` or the original string if unknown."""
    # #region agent log
    raw_in = value
    # #endregion
    s = value.strip().replace("\xa0", "").replace("\u2009", "").replace("\u202f", "").strip()
    if not s:
        return ""

    # Strip trailing notes when the cell is "48.025 (W)" or "47.074 *".
    if " " in s and s[:1].isdigit():
        s = s.split()[0]

    # Hypothesis H_comma: European-style decimal comma (no ASCII dot), e.g. "47,074" seconds.
    if "." not in s and re.fullmatch(r"[0-9]+,[0-9]+", s):
        s_dot = s.replace(",", ".")
        try:
            v = float(s_dot)
            if 15.0 < v < 200.0:
                s = s_dot
        except ValueError:
            pass

    total_seconds: float | None = None
    if re.fullmatch(r"^[0-9]+(?:\.[0-9]*)?$", s):
        total_seconds = float(s)
    elif re.fullmatch(r"^[0-9]+:[0-9]{1,2}(?:\.[0-9]*)?$", s):
        mins_raw, secs_raw = s.split(":", 1)
        total_seconds = (int(mins_raw) * 60) + float(secs_raw)
    elif re.fullmatch(r"^[0-9]+:[0-9]{1,2}:[0-9]{1,2}(?:\.[0-9]*)?$", s):
        hrs_raw, mins_raw, secs_raw = s.split(":", 2)
        total_seconds = (int(hrs_raw) * 3600) + (int(mins_raw) * 60) + float(secs_raw)
    else:
        # #region agent log
        _agent_log(
            "H_passthrough",
            "time_unparsed_returned_raw",
            {"raw_in": raw_in[:120], "clean": s[:120]},
        )
        # #endregion
        return s

    millis_total = int(round(total_seconds * 1000))
    hrs = millis_total // 3_600_000
    mins = (millis_total % 3_600_000) // 60_000
    secs = (millis_total % 60_000) // 1000
    millis = millis_total % 1000
    out = f"{hrs:02d}:{mins:02d}:{secs:02d}.{millis:03d}"
    # #region agent log
    if raw_in.strip() != out and os.environ.get("CURSOR_DEBUG_TIME") == "1":
        _agent_log(
            "H_norm_ok",
            "time_normalized",
            {"raw_in": raw_in[:120], "out": out},
        )
    # #endregion
    return out
