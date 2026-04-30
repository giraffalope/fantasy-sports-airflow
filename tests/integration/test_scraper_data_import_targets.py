"""
Integration tests that validate runtime conversion outputs for importer target domains.

These assert row-bearing CSV outputs for race/penalty/pitstop/DFS feeds plus
entity-producing feeds (entrylist/lineup) that drive teams/drivers/manufacturers/cars links.
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


def _copy_html(tmp_path: Path, rel: str, out_name: str | None = None) -> Path:
    src = SCRAPER / rel
    if not src.is_file():
        pytest.skip(f"missing {src}")
    dst = tmp_path / (out_name or src.name)
    shutil.copyfile(src, dst)
    return dst


def _read_csv(path: Path) -> list[list[str]]:
    assert path.is_file(), f"expected output CSV at {path}"
    return list(csv.reader(path.read_text(encoding="utf-8").splitlines()))


def test_runtime_race_results_penalties_and_pitstops_outputs_rows(tmp_path) -> None:
    race_html = _copy_html(tmp_path, "frcs.pro/cup/2025/25/race-results.html")
    pit_html = _copy_html(tmp_path, "frcs.pro/cup/2025/25/pit-stops.html")

    race_csv = race_html.with_suffix(".csv")
    penalties_csv = race_html.with_name("penalties.csv")
    pit_summary_csv = pit_html.with_name("pit-stops-summary.csv")
    pit_detail_csv = pit_html.with_name("pit-stops-detail.csv")

    FrcsHtmlToCsv.convert_race_results(str(race_html), str(race_csv))
    # FRCS penalty extraction is sourced from race-results markup and writes penalties.csv.
    FrcsHtmlToCsv.convert_penalties(str(race_html))
    FrcsHtmlToCsv.convert_pit_stops(str(pit_html))

    race_rows = _read_csv(race_csv)
    assert len(race_rows) >= 10
    race_header = race_rows[0]
    assert "name" in race_header and "car" in race_header

    penalties_rows = _read_csv(penalties_csv)
    assert len(penalties_rows) >= 2
    penalties_header = penalties_rows[0]
    assert penalties_header == ["driver_name", "lap", "flag", "infraction", "penalty"]

    pit_summary_rows = _read_csv(pit_summary_csv)
    pit_detail_rows = _read_csv(pit_detail_csv)
    assert len(pit_summary_rows) >= 2
    assert len(pit_detail_rows) >= 2


def test_runtime_draftkings_and_fanduel_outputs_rows(tmp_path) -> None:
    dk_html = _copy_html(tmp_path, "frcs.pro/cup/2025/25/draftkings.html")
    fd_html = _copy_html(tmp_path, "frcs.pro/cup/2025/25/fanduel.html")

    dk_csv = dk_html.with_suffix(".csv")
    fd_csv = fd_html.with_suffix(".csv")

    FrcsHtmlToCsv.convert_draftkings_points(str(dk_html))
    FrcsHtmlToCsv.convert_fanduel_points(str(fd_html))

    dk_rows = _read_csv(dk_csv)
    fd_rows = _read_csv(fd_csv)
    assert len(dk_rows) >= 10
    assert len(fd_rows) >= 10
    assert "driver_name" in dk_rows[0] and "salary" in dk_rows[0]
    assert "driver_name" in fd_rows[0] and "salary" in fd_rows[0]


def test_runtime_entrylist_and_lineup_outputs_entity_feeds(tmp_path) -> None:
    entry_html = _copy_html(tmp_path, "frcs.pro/cup/2025/25/entrylist.html")
    lineup_html = _copy_html(tmp_path, "frcs.pro/cup/2025/25/lineup.html")

    entry_csv = entry_html.with_suffix(".csv")
    lineup_csv = lineup_html.with_suffix(".csv")

    FrcsHtmlToCsv.convert_entrylist(str(entry_html), str(entry_csv))
    FrcsHtmlToCsv.convert_lineup(str(lineup_html))

    entry_rows = _read_csv(entry_csv)
    lineup_rows = _read_csv(lineup_csv)

    assert len(entry_rows) >= 10
    assert len(lineup_rows) >= 10

    entry_header = entry_rows[0]
    lineup_header = lineup_rows[0]
    assert "owner" in entry_header
    assert "driver_name" in entry_header
    assert "manufacturer" in entry_header
    assert "team" in lineup_header
    assert "driver_name" in lineup_header
    assert "manufacturer" in lineup_header
