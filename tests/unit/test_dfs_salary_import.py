"""Unit tests for DraftKings salary CSV import helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from include.nascar.dfs.salary_import_task import (
    _normalize_driver_name,
    _read_dk_salaries_csv,
)

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "DKSalaries.csv"


def test_read_dk_salaries_csv_maps_columns() -> None:
    frame = _read_dk_salaries_csv(str(_FIXTURE))
    assert len(frame) == 1
    row = frame.iloc[0]
    assert int(row.dfs_driver_id) == 43109026
    assert row.driver_name == "Chase Elliott"
    assert int(row.salary) == 11500


def test_read_dk_salaries_csv_missing_columns(tmp_path: Path) -> None:
    bad = tmp_path / "bad.csv"
    bad.write_text("Name,Salary\nA,100\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing columns"):
        _read_dk_salaries_csv(str(bad))


def test_normalize_driver_name_strips_suffix() -> None:
    assert _normalize_driver_name("Kyle Larson Jr.") == "kyle larson"
