"""Unit tests for DraftKings salary CSV import helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.nascar.dfs.salary_import_task import (
    CupWeekContext,
    _CAR_DRIVER_BY_DFS_ID_SQL,
    _CURRENT_WEEK_WITH_RACE_SQL,
    _RACE_DRIVER_NAMES_SQL,
    _WEEK_RACE_SQL,
    _dedupe_insert_rows_by_car_driver,
    _driver_display_name,
    _import_csv_file,
    _match_car_driver_id,
    _normalize_driver_name,
    _read_dk_salaries_csv,
    ensure_draftkings_week_directory_task,
    import_draftkings_salaries_task,
    resolve_current_cup_week,
    resolve_week_race,
)
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "DKSalaries.csv"


# --- _normalize_driver_name / _driver_display_name ---


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Kyle Larson Jr.", "kyle larson"),
        ("John Smith Sr.", "john smith"),
        ("Robert Brown III", "robert brown"),
        ("James Lee II", "james lee"),
        ("Alex Kim IV", "alex kim"),
        ("  Multiple   Spaces  ", "multiple spaces"),
        ("O'Brien, Jr.", "obrien"),
    ],
)
def test_normalize_driver_name(raw: str, expected: str) -> None:
    assert _normalize_driver_name(raw) == expected


def test_driver_display_name_joins_parts() -> None:
    assert _driver_display_name("Chase", "Elliott", None) == "chase elliott"
    assert _driver_display_name(None, "Elliott", "Jr") == "elliott"


# --- resolve_current_cup_week / resolve_week_race ---


def test_resolve_current_cup_week_returns_none_when_empty() -> None:
    hook = MagicMock()
    hook.get_records.return_value = []
    assert resolve_current_cup_week(hook) is None


def test_resolve_current_cup_week_maps_row_with_race_id() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(1048, 16, 2026, 730)]
    ctx = resolve_current_cup_week(hook)
    assert ctx == CupWeekContext(week_id=1048, week_number=16, season_year=2026, race_id=730)
    hook.get_records.assert_called_once_with(
        _CURRENT_WEEK_WITH_RACE_SQL,
        parameters=(1,),
    )


def test_resolve_current_cup_week_null_race_id() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(1048, 16, 2026, None)]
    ctx = resolve_current_cup_week(hook)
    assert ctx is not None
    assert ctx.race_id is None


def test_resolve_week_race_returns_none_when_empty() -> None:
    hook = MagicMock()
    hook.get_records.return_value = []
    assert resolve_week_race(hook, season_year=2026, week_number=16) is None


def test_resolve_week_race_returns_week_and_race_ids() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(1048, 730)]
    result = resolve_week_race(hook, season_year=2026, week_number=16)
    assert result == (1048, 730)
    hook.get_records.assert_called_once_with(
        _WEEK_RACE_SQL,
        parameters=(1, 2026, 16),
    )


# --- ensure_draftkings_week_directory_task ---


def test_ensure_draftkings_week_directory_skips_without_current_week() -> None:
    hook = MagicMock()
    with (
        patch(
            "include.nascar.dfs.salary_import_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.salary_import_task.resolve_current_cup_week",
            return_value=None,
        ),
    ):
        result = ensure_draftkings_week_directory_task()
    assert result == {"skipped": True, "reason": "no_current_week"}


def test_ensure_draftkings_week_directory_creates_path(tmp_path: Path) -> None:
    ctx = CupWeekContext(week_id=1048, week_number=16, season_year=2026, race_id=730)
    target_dir = tmp_path / "2026" / "16"
    with (
        patch(
            "include.nascar.dfs.salary_import_task.NascarMySqlHook",
        ),
        patch(
            "include.nascar.dfs.salary_import_task.resolve_current_cup_week",
            return_value=ctx,
        ),
        patch(
            "include.nascar.dfs.salary_import_task.draftkings_salaries_dir",
            return_value=target_dir,
        ),
    ):
        result = ensure_draftkings_week_directory_task()
    assert target_dir.is_dir()
    assert result["season_year"] == 2026
    assert result["week_number"] == 16
    assert result["week_id"] == 1048
    assert result["race_id"] == 730
    assert result["directory"] == str(target_dir)


# --- _read_dk_salaries_csv ---


def test_read_dk_salaries_csv_maps_columns() -> None:
    frame = _read_dk_salaries_csv(str(_FIXTURE))
    assert len(frame) == 1
    row = frame.iloc[0]
    assert int(row.dfs_driver_id) == 43109026
    assert row.driver_name == "Chase Elliott"
    assert int(row.salary) == 11500


def test_read_dk_salaries_csv_strips_salary_formatting(tmp_path: Path) -> None:
    csv_path = tmp_path / "formatted.csv"
    csv_path.write_text(
        "ID,Name,Salary\n"
        "1,Driver A,\"$11,500\"\n"
        "2,Driver B,bad\n",
        encoding="utf-8",
    )
    frame = _read_dk_salaries_csv(str(csv_path))
    assert len(frame) == 1
    assert int(frame.iloc[0]["salary"]) == 11500


def test_read_dk_salaries_csv_missing_columns(tmp_path: Path) -> None:
    bad = tmp_path / "bad.csv"
    bad.write_text("Name,Salary\nA,100\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing columns"):
        _read_dk_salaries_csv(str(bad))


def test_resolve_current_cup_week_uses_view() -> None:
    assert "v_series_current_week" in _CURRENT_WEEK_WITH_RACE_SQL


# --- _match_car_driver_id ---


def test_match_car_driver_id_from_existing_dfs_salary() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(42,)]
    result = _match_car_driver_id(
        hook, race_id=730, dfs_driver_id=100, driver_name="Anyone"
    )
    assert result == 42
    hook.get_records.assert_called_once_with(
        _CAR_DRIVER_BY_DFS_ID_SQL,
        parameters=(DRAFTKINGS_DFS_ID, 730, 100),
    )


def test_match_car_driver_id_by_normalized_name() -> None:
    hook = MagicMock()
    hook.get_records.side_effect = [
        [],
        [(4, "Chase", "Elliott", None), (5, "Kyle", "Larson", "Jr")],
    ]
    result = _match_car_driver_id(
        hook, race_id=730, dfs_driver_id=999, driver_name="Chase Elliott"
    )
    assert result == 4
    hook.get_records.assert_any_call(_RACE_DRIVER_NAMES_SQL, parameters=(730,))


def test_match_car_driver_id_returns_none_when_unmatched() -> None:
    hook = MagicMock()
    hook.get_records.side_effect = [[], [(5, "Kyle", "Larson", None)]]
    result = _match_car_driver_id(
        hook, race_id=730, dfs_driver_id=999, driver_name="Unknown Driver"
    )
    assert result is None


# --- _dedupe_insert_rows_by_car_driver ---


def test_dedupe_insert_rows_by_car_driver_keeps_highest_salary() -> None:
    rows = [
        (DRAFTKINGS_DFS_ID, 99, 100, 501, 9000),
        (DRAFTKINGS_DFS_ID, 99, 200, 501, 11500),
    ]
    deduped = _dedupe_insert_rows_by_car_driver(rows)
    assert len(deduped) == 1
    assert deduped[0][3] == 501
    assert deduped[0][4] == 11500


def test_dedupe_insert_rows_empty_list() -> None:
    assert _dedupe_insert_rows_by_car_driver([]) == []


# --- _import_csv_file ---


def test_import_csv_file_skips_when_week_not_in_db() -> None:
    hook = MagicMock()
    with patch(
        "include.nascar.dfs.salary_import_task.resolve_week_race",
        return_value=None,
    ):
        result = _import_csv_file(
            hook, "/any/path.csv", season_year=2026, week_number=16
        )
    assert result["skipped"] is True
    assert result["reason"] == "week_not_in_db"
    hook.run_in_transaction.assert_not_called()


def test_import_csv_file_skips_on_read_error(tmp_path: Path) -> None:
    hook = MagicMock()
    missing = tmp_path / "missing.csv"
    with patch(
        "include.nascar.dfs.salary_import_task.resolve_week_race",
        return_value=(1048, 730),
    ):
        result = _import_csv_file(
            hook, str(missing), season_year=2026, week_number=16
        )
    assert result["skipped"] is True
    hook.run_in_transaction.assert_not_called()


def test_import_csv_file_skips_on_parser_error(tmp_path: Path) -> None:
    hook = MagicMock()
    bad = tmp_path / "bad.csv"
    bad.write_text("not,a,valid\ncsv\x00", encoding="utf-8")
    with (
        patch(
            "include.nascar.dfs.salary_import_task.resolve_week_race",
            return_value=(1048, 730),
        ),
        patch(
            "include.nascar.dfs.salary_import_task._read_dk_salaries_csv",
            side_effect=pd.errors.ParserError("parse failed"),
        ),
    ):
        result = _import_csv_file(
            hook, str(bad), season_year=2026, week_number=16
        )
    assert result["skipped"] is True
    assert "parse failed" in result["reason"]


def test_import_csv_file_counts_unmatched_drivers(tmp_path: Path) -> None:
    hook = MagicMock()
    csv_path = tmp_path / "two.csv"
    csv_path.write_text(
        "ID,Name,Salary\n1,Matched,10000\n2,Unmatched,9000\n",
        encoding="utf-8",
    )

    def match_side_effect(
        _hook: MagicMock,
        *,
        race_id: int,
        dfs_driver_id: int,
        driver_name: str,
    ) -> int | None:
        del race_id, driver_name
        return 501 if dfs_driver_id == 1 else None

    with (
        patch(
            "include.nascar.dfs.salary_import_task.resolve_week_race",
            return_value=(1048, 730),
        ),
        patch(
            "include.nascar.dfs.salary_import_task._match_car_driver_id",
            side_effect=match_side_effect,
        ),
    ):
        result = _import_csv_file(
            hook, str(csv_path), season_year=2026, week_number=16
        )

    assert result["rows_read"] == 2
    assert result["rows_inserted"] == 1
    assert result["rows_unmatched"] == 1


def test_import_csv_file_uses_run_in_transaction_delete_and_executemany() -> None:
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
    assert result["race_id"] == 99
    hook.run_in_transaction.assert_called_once()
    write_fn = hook.run_in_transaction.call_args.args[0]
    conn = MagicMock()
    cursor = MagicMock()
    write_fn(conn, cursor)
    delete_sql = cursor.execute.call_args.args[0]
    assert "DELETE FROM nascar.dfs_salaries" in delete_sql
    assert cursor.execute.call_args.args[1] == (DRAFTKINGS_DFS_ID, 99)
    assert "deleted_at" not in delete_sql
    cursor.executemany.assert_called_once()


def test_import_csv_file_delete_only_when_all_unmatched(tmp_path: Path) -> None:
    hook = MagicMock()
    csv_path = tmp_path / "none.csv"
    csv_path.write_text("ID,Name,Salary\n1,Nobody,10000\n", encoding="utf-8")

    with (
        patch(
            "include.nascar.dfs.salary_import_task.resolve_week_race",
            return_value=(1048, 730),
        ),
        patch(
            "include.nascar.dfs.salary_import_task._match_car_driver_id",
            return_value=None,
        ),
    ):
        result = _import_csv_file(
            hook, str(csv_path), season_year=2026, week_number=16
        )

    assert result["rows_inserted"] == 0
    write_fn = hook.run_in_transaction.call_args.args[0]
    cursor = MagicMock()
    write_fn(MagicMock(), cursor)
    cursor.execute.assert_called_once()
    cursor.executemany.assert_not_called()


def test_import_csv_file_dedupes_before_insert(tmp_path: Path) -> None:
    hook = MagicMock()
    csv_path = tmp_path / "dup.csv"
    csv_path.write_text(
        "ID,Name,Salary\n"
        "1,Same Driver,9000\n"
        "2,Same Driver,11500\n",
        encoding="utf-8",
    )

    with (
        patch(
            "include.nascar.dfs.salary_import_task.resolve_week_race",
            return_value=(1048, 730),
        ),
        patch(
            "include.nascar.dfs.salary_import_task._match_car_driver_id",
            return_value=501,
        ),
    ):
        result = _import_csv_file(
            hook, str(csv_path), season_year=2026, week_number=16
        )

    assert result["rows_inserted"] == 1
    write_fn = hook.run_in_transaction.call_args.args[0]
    cursor = MagicMock()
    write_fn(MagicMock(), cursor)
    insert_rows = cursor.executemany.call_args.args[1]
    assert len(insert_rows) == 1
    assert insert_rows[0][4] == 11500


# --- import_draftkings_salaries_task ---


def test_import_draftkings_salaries_skips_without_current_week() -> None:
    hook = MagicMock()
    with (
        patch(
            "include.nascar.dfs.salary_import_task.NascarMySqlHook",
            return_value=hook,
        ),
        patch(
            "include.nascar.dfs.salary_import_task.resolve_current_cup_week",
            return_value=None,
        ),
    ):
        result = import_draftkings_salaries_task()
    assert result == {
        "files": [],
        "imported": 0,
        "skipped": True,
        "reason": "no_current_week",
    }


def test_import_draftkings_salaries_returns_empty_when_csv_missing() -> None:
    ctx = CupWeekContext(week_id=1048, week_number=16, season_year=2026, race_id=730)
    missing = Path("/nonexistent/DKSalaries.csv")
    with (
        patch(
            "include.nascar.dfs.salary_import_task.NascarMySqlHook",
        ),
        patch(
            "include.nascar.dfs.salary_import_task.resolve_current_cup_week",
            return_value=ctx,
        ),
        patch(
            "include.nascar.dfs.salary_import_task.draftkings_salaries_csv",
            return_value=missing,
        ),
    ):
        result = import_draftkings_salaries_task()
    assert result == {"files": [], "imported": 0}


def test_import_draftkings_salaries_current_week_only(tmp_path: Path) -> None:
    hook = MagicMock()
    ctx = CupWeekContext(week_id=1048, week_number=16, season_year=2026, race_id=99)

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
    assert result["files"] == [{"rows_inserted": 3}]
    mock_import.assert_called_once_with(
        hook,
        str(tmp_path / "DKSalaries.csv"),
        season_year=2026,
        week_number=16,
    )


def test_import_draftkings_salaries_integration_path(tmp_path: Path) -> None:
    """End-to-end through _import_csv_file with mocked DB only."""
    hook = MagicMock()
    ctx = CupWeekContext(week_id=1048, week_number=16, season_year=2026, race_id=730)
    csv_path = tmp_path / "DKSalaries.csv"
    csv_path.write_text(_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")

    hook.get_records.side_effect = [
        [(1048, 730)],  # resolve_week_race
        [],  # _match: dfs lookup
        [(501, "Chase", "Elliott", None)],  # _match: race drivers
    ]

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
            return_value=csv_path,
        ),
    ):
        result = import_draftkings_salaries_task()

    assert result["imported"] == 1
    assert len(result["files"]) == 1
    assert result["files"][0]["rows_inserted"] == 1
