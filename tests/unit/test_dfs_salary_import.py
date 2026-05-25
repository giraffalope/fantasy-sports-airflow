"""Unit tests for DraftKings salary CSV import helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from include.nascar.dfs.salary_import_task import (
    _CURRENT_WEEK_WITH_RACE_SQL,
    _import_csv_file,
    _normalize_driver_name,
    _read_dk_salaries_csv,
    import_draftkings_salaries_task,
    resolve_current_cup_week,
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


def test_resolve_current_cup_week_uses_view() -> None:
    assert "v_series_current_week" in _CURRENT_WEEK_WITH_RACE_SQL


def test_import_draftkings_salaries_current_week_only(tmp_path: Path) -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(1048, 16, 2026, 99)]

    ctx = resolve_current_cup_week(hook)
    assert ctx is not None
    assert ctx.week_id == 1048

    with (
        patch(
            "include.nascar.dfs.salary_import_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.salary_import_task.resolve_current_cup_week",
            return_value=ctx,
        ),
        patch(
            "include.nascar.dfs.salary_import_task.draftkings_salaries_csv",
        ) as mock_csv,
        patch(
            "include.nascar.dfs.salary_import_task._import_csv_file",
            return_value={"rows_inserted": 3},
        ) as mock_import,
    ):
        mock_csv.return_value = tmp_path / "DKSalaries.csv"
        mock_csv.return_value.write_text("x", encoding="utf-8")

        result = import_draftkings_salaries_task()

    assert result["imported"] == 3
    mock_import.assert_called_once()


def test_import_csv_file_uses_run_in_transaction_and_executemany() -> None:
    hook = MagicMock()
    csv_path = _FIXTURE

    with (
        patch(
            "include.nascar.dfs.salary_import_task.resolve_week_race",
            return_value=(1048, 99),
        ),
        patch(
            "include.nascar.dfs.salary_import_task._match_car_driver_id",
            return_value=501,
        ),
    ):
        result = _import_csv_file(
            hook,
            str(csv_path),
            season_year=2026,
            week_number=16,
        )

    assert result["rows_inserted"] == 1
    hook.run_in_transaction.assert_called_once()
    write_fn = hook.run_in_transaction.call_args.args[0]
    conn = MagicMock()
    cursor = MagicMock()
    write_fn(conn, cursor)
    cursor.executemany.assert_called_once()
