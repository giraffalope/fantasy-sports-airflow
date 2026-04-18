"""
Exercise practice HTML -> CSV against the project ``scraper-data`` tree (optional).

Expected layout (gitignored at repo root; populate locally from your mirror):

    <fantasy-sports-airflow>/scraper-data/nascar/frcs.pro/cup/...

Override with env ``SCRAPER_DATA_ROOT`` if the tree lives elsewhere. If the
default (or override) directory is missing, tests skip.
"""

from __future__ import annotations

import csv
import os
import shutil
from pathlib import Path

import pytest

from include.nascar.data_sources.frcs_html_to_csv import FrcsHtmlToCsv

_AIRFLOW_ROOT = Path(__file__).resolve().parent.parent.parent


def _scraper_data_root() -> Path | None:
    env = os.environ.get("SCRAPER_DATA_ROOT", "").strip()
    if env:
        p = Path(env).expanduser()
    else:
        p = _AIRFLOW_ROOT / "scraper-data" / "nascar"
    return p if p.is_dir() else None


SCRAPER = _scraper_data_root()

pytestmark = pytest.mark.skipif(
    SCRAPER is None,
    reason="scraper-data/nascar not found under repo root (or SCRAPER_DATA_ROOT)",
)


def _copy_practice(tmp_path: Path, rel: str) -> Path:
    src = SCRAPER / rel
    if not src.is_file():
        pytest.skip(f"missing {src}")
    dst = tmp_path / src.name
    shutil.copyfile(src, dst)
    return dst


def test_frcs_practice_2010_legacy_json_parses_rows(tmp_path) -> None:
    html = _copy_practice(tmp_path, "frcs.pro/cup/2010/01/practice-01.html")
    FrcsHtmlToCsv.convert_practices([str(html)])
    csv_path = html.with_suffix(".csv")
    assert csv_path.is_file()

    rows = list(csv.reader(csv_path.read_text(encoding="utf-8").splitlines()))
    assert len(rows) >= 10, "expected header + multiple practice rows"
    assert len(rows[0]) == 16
    assert rows[0][0] == "rank"
    assert rows[0][1] == "driver_name"
    assert rows[1][1].strip(), "driver_name should be populated"


def test_frcs_practice_2025_contains_lap_average_columns(tmp_path) -> None:
    html = _copy_practice(tmp_path, "frcs.pro/cup/2025/25/practice-01.html")
    FrcsHtmlToCsv.convert_practices([str(html)])
    csv_path = html.with_suffix(".csv")
    assert csv_path.is_file()

    rows = list(csv.reader(csv_path.read_text(encoding="utf-8").splitlines()))
    assert len(rows) >= 10
    header = rows[0]
    assert "10_lap_average_time" in header
    assert "20_lap_average_time" in header
    assert "10_lap_average_speed" in header

    ten_idx = header.index("10_lap_average_time")
    avg_speed_idx = header.index("10_lap_average_speed")
    data_rows = rows[1:]
    assert any(r[ten_idx].strip() for r in data_rows), "expected non-empty 10-lap average times"
    assert any(r[avg_speed_idx].strip() for r in data_rows), "expected non-empty 10-lap average speeds"
