"""Pit-stop FRCS CSV time columns use the same ``HH:MM:SS.mmm`` convention as qualifying ``lap_time``."""

from __future__ import annotations

import csv
import re
from pathlib import Path

from include.nascar.data_sources.frcs_html_to_csv import FrcsHtmlToCsv

_TIME_HHMMSS_MMM = re.compile(r"^\d{2}:\d{2}:\d{2}\.\d{3}$")

_PIT_HTML_MINIMAL = """<!DOCTYPE html>
<html><head></head><body><script>
const pitStopSummary = [
  {
    "rank": "1",
    "vehicle_number": "5",
    "full_name": "Test Driver",
    "stops": "3",
    "start": "1",
    "finish": "10",
    "status": "Running",
    "avg_driver": "12.345",
    "avg_crew": "1:23.456",
    "avg_stop": "13.5",
    "fastest_stop": "11.0",
    "slowest_stop": "15.9"
  }
];
const pitStopDetail = [
  {
    "vehicle_number": "5",
    "full_name": "Test Driver",
    "leader_lap": "5",
    "pitter_lap": "5",
    "type": "FOUR_WHEEL_CHANGE",
    "stop_duration": "12.1",
    "driver_time": "0:03.5",
    "total_duration": "14.2"
  }
];
</script></body></html>
"""


def test_convert_pit_stops_normalizes_time_columns_like_qualifying_lap_time(tmp_path: Path) -> None:
    pit_html = tmp_path / "pit-stops.html"
    pit_html.write_text(_PIT_HTML_MINIMAL, encoding="utf-8")

    FrcsHtmlToCsv.convert_pit_stops(str(pit_html))

    summary_csv = tmp_path / "pit-stops-summary.csv"
    detail_csv = tmp_path / "pit-stops-detail.csv"
    assert summary_csv.is_file()
    assert detail_csv.is_file()

    sum_rows = list(csv.reader(summary_csv.read_text(encoding="utf-8").splitlines()))
    det_rows = list(csv.reader(detail_csv.read_text(encoding="utf-8").splitlines()))
    assert len(sum_rows) == 2
    assert len(det_rows) == 2

    sh = sum_rows[0]
    sr = sum_rows[1]
    idx = {name: i for i, name in enumerate(sh)}
    for col in ("avg_driver", "avg_crew", "avg_stop", "fastest_stop", "slowest_stop"):
        val = sr[idx[col]]
        assert _TIME_HHMMSS_MMM.match(val), f"{col}={val!r}"

    assert sr[idx["avg_driver"]] == "00:00:12.345"
    assert sr[idx["avg_crew"]] == "00:01:23.456"
    assert sr[idx["avg_stop"]] == "00:00:13.500"
    assert sr[idx["fastest_stop"]] == "00:00:11.000"
    assert sr[idx["slowest_stop"]] == "00:00:15.900"

    dh = det_rows[0]
    dr = det_rows[1]
    dix = {name: i for i, name in enumerate(dh)}
    for col in ("stop_duration", "driver_time", "total_duration"):
        val = dr[dix[col]]
        assert _TIME_HHMMSS_MMM.match(val), f"{col}={val!r}"

    assert dr[dix["stop_duration"]] == "00:00:12.100"
    assert dr[dix["driver_time"]] == "00:00:03.500"
    assert dr[dix["total_duration"]] == "00:00:14.200"
