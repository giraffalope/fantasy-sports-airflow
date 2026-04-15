"""
Exercise qualifying HTML → CSV against the project ``scraper-data`` tree (optional).

Expected layout (gitignored at repo root; populate locally from your mirror):

    <fantasy-sports-airflow>/scraper-data/nascar/frcs.pro/cup/…
    <fantasy-sports-airflow>/scraper-data/nascar/www.racing-reference.info/cup/…

Override with env ``SCRAPER_DATA_ROOT`` if the tree lives elsewhere. If the
default (or override) directory is missing, tests skip.

**FRCS older seasons:** On frcs.pro there is no qualifying grid in the page
for many historical years, so saved HTML has nothing to parse. Newer seasons
expose embedded JSON and/or tabulator DOM (**2024+** in typical scrapes).

**Racing Reference** qualifying pages use ``qualResultTbl`` and convert for
years such as 2010.
"""

from __future__ import annotations

import csv
import os
import shutil
from pathlib import Path

import pytest

from include.nascar.data_sources.frcs_html_to_csv import FrcsHtmlToCsv
from include.nascar.data_sources.racing_reference_html_to_csv import RacingReferenceHtmlToCsv

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


def _copy_qualifying(tmp_path: Path, rel: str) -> Path:
    src = SCRAPER / rel
    if not src.is_file():
        pytest.skip(f"missing {src}")
    dst = tmp_path / "qualifying.html"
    shutil.copyfile(src, dst)
    return dst


def test_frcs_qualifying_2024_json_round_two(tmp_path) -> None:
    """2024/13: non-empty ``roundTwo`` JSON → CSV with multiple data rows."""
    html = _copy_qualifying(tmp_path, "frcs.pro/cup/2024/13/qualifying.html")
    FrcsHtmlToCsv.convert_qualifying(str(html))
    csv_path = html.with_suffix(".csv")
    assert csv_path.is_file()
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) >= 5, "expected header + several qualifying rows"
    reader = csv.reader(lines)
    header = next(reader)
    assert "driver_name" in header
    rows = list(reader)
    assert len(rows) >= 5
    assert all(len(r) >= 2 for r in rows)
    assert rows[0][1].strip(), "driver_name column should be populated"


def test_frcs_qualifying_2025_tabulator_dom(tmp_path) -> None:
    """2025/25: ``tabulator-row`` DOM (empty JSON arrays) still produces rows."""
    html = _copy_qualifying(tmp_path, "frcs.pro/cup/2025/25/qualifying.html")
    FrcsHtmlToCsv.convert_qualifying(str(html))
    csv_path = html.with_suffix(".csv")
    assert csv_path.is_file()
    rows = list(csv.reader(csv_path.read_text(encoding="utf-8").splitlines()))
    assert len(rows) >= 10, "expected header + ~40 Cup drivers"
    assert len(rows[0]) == 6


def test_rr_qualifying_2010_qual_result_tbl(tmp_path) -> None:
    """Racing Reference markup: ``qualResultTbl`` with many drivers."""
    html = _copy_qualifying(tmp_path, "www.racing-reference.info/cup/2010/01/qualifying.html")
    RacingReferenceHtmlToCsv.convert_qualifying(str(html))
    csv_path = html.with_suffix(".csv")
    assert csv_path.is_file()
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) >= 20, "Cup field + header"


def test_frcs_qualifying_rr_fallback_uses_rr_file(tmp_path) -> None:
    """FRCS entrypoint: RR-only HTML still converts via Racing Reference fallback."""
    html = _copy_qualifying(tmp_path, "www.racing-reference.info/cup/2015/01/qualifying.html")
    FrcsHtmlToCsv.convert_qualifying(str(html))
    csv_path = html.with_suffix(".csv")
    assert csv_path.is_file()
    assert len(csv_path.read_text(encoding="utf-8").splitlines()) >= 20


def test_frcs_qualifying_prehydrated_legacy_shell_empty(tmp_path) -> None:
    """2010 FRCS save: empty JS arrays and no results table → no data rows."""
    html = _copy_qualifying(tmp_path, "frcs.pro/cup/2010/01/qualifying.html")
    FrcsHtmlToCsv.convert_qualifying(str(html))
    csv_path = html.with_suffix(".csv")
    if csv_path.is_file():
        rows = list(csv.reader(csv_path.read_text(encoding="utf-8").splitlines()))
        assert len(rows) <= 1, "header-only or empty; scrape has no embedded grid"
    else:
        assert not csv_path.exists()
