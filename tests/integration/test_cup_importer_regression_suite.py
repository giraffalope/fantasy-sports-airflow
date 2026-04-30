from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import nascar.cup.scraper.importer as importer_mod

from airflow_utils.nascar_mysql_hook import NascarMySqlHook


SQL_ROOT = Path(__file__).resolve().parents[2] / "dags" / "sql" / "nascar.cup.scraper.importer"
_SCRAPER_ROOT = Path("/home/airflow/scraper/nascar")


def _sql_text(name: str) -> str:
    return (SQL_ROOT / name).read_text(encoding="utf-8")


def test_regression_week_tmp_car_does_not_inherit_stale_season_car_id() -> None:
    sql = _sql_text("resolve_drivers_cars_manufacturers_teams.sql")
    assert "w.car_id = COALESCE(w.car_id, s.car_id)" not in sql


def test_regression_candidate_rank_reselects_car_and_backfills_team_fields() -> None:
    sql = _sql_text("resolve_drivers_cars_manufacturers_teams.sql")
    assert "SET t.car_id = r.car_id" in sql
    assert "UPDATE nascar.tmp_car_{{ week_id }}_{{ table_suffix }} t" in sql
    assert "JOIN nascar.cars c" in sql
    assert "t.team_id = COALESCE(t.team_id, c.team_id)" in sql
    assert "t.manufacturer_id = COALESCE(t.manufacturer_id, c.manufacturer_id)" in sql


def test_regression_race_import_prefers_driver_match_over_number_match() -> None:
    sql = _sql_text("import_race_staging_into_db.sql")
    assert "SET t.car_driver_id = COALESCE(tc_driver.car_driver_id, tc_num.car_driver_id)" in sql


class _DummyTI:
    def __init__(self) -> None:
        self._xcom: dict[str, Any] = {}

    def xcom_push(self, key: str, value: Any) -> None:
        self._xcom[key] = value

    def xcom_pull(self, task_ids: str | None = None, key: str | None = None) -> Any:
        del task_ids
        return self._xcom.get(key or "")


def _require_db_conn() -> None:
    if not os.environ.get("AIRFLOW_CONN_NASCAR_CONN"):
        pytest.skip("AIRFLOW_CONN_NASCAR_CONN not configured for DB runtime regression test")


def _ensure_2010_schedule_seeded(hook: NascarMySqlHook, context: dict[str, Any]) -> None:
    existing = hook.get_first(
        """
        SELECT COUNT(*)
        FROM nascar.weeks w
        JOIN nascar.seasons s ON s.id = w.season_id
        WHERE s.series_id = %s
          AND s.year = %s
          AND s.deleted_at IS NULL
          AND w.deleted_at IS NULL
        """,
        parameters=(1, 2010),
    )
    if existing and int(existing[0]) > 0:
        return

    load_params = importer_mod._importer_params_load_schedule(context)
    import_params = importer_mod._importer_params_import_schedule(context)
    hook_seed = NascarMySqlHook(mysql_conn_id="nascar_conn")
    importer_mod._execute_templated_sql_file(
        hook_seed,
        importer_mod._sql_root() / "nascar.cup.scraper.importer" / "load_stage_schedule_csvs.sql",
        load_params,
        import_params["frcs_source_name"],
    )
    importer_mod._execute_templated_sql_file(
        hook_seed,
        importer_mod._sql_root() / "nascar.cup.scraper.importer" / "import_schedule.sql",
        import_params,
        import_params["frcs_source_name"],
    )


def _target_weeks_for_2010(hook: NascarMySqlHook, frcs_year: Path) -> list[tuple[int, int, int, int]]:
    rows = hook.get_records(
        """
        SELECT w.id, w.season_id, w.number
        FROM nascar.weeks w
        JOIN nascar.seasons s ON s.id = w.season_id
        WHERE s.series_id = %s
          AND s.year = %s
          AND s.deleted_at IS NULL
          AND w.deleted_at IS NULL
        ORDER BY w.number
        """,
        parameters=(1, 2010),
    )
    out: list[tuple[int, int, int, int]] = []
    missing: list[str] = []
    for row in rows:
        week_id = int(row[0])
        season_id = int(row[1])
        event_number = int(row[2])
        week_dir = frcs_year / f"{event_number:02d}"
        # Strict full-year regression: all DB weeks must have required race-stage CSVs.
        for required_name in ("race-results.csv", "loop-data.csv"):
            if not (week_dir / required_name).is_file():
                missing.append(f"week {event_number:02d}: missing {required_name}")
        out.append((week_id, season_id, event_number, event_number))
    if missing:
        detail = "\n".join(missing)
        pytest.fail(f"Missing required 2010 fixtures for full-year regression:\n{detail}")
    return out


def _copy_frcs_csvs_to_rr_for_2010(frcs_year: Path, rr_year: Path) -> None:
    if not frcs_year.is_dir():
        pytest.skip("Missing FRCS 2010 fixture folder")
    rr_year.mkdir(parents=True, exist_ok=True)

    names = [
        "entrylist.csv",
        "lineup.csv",
        "race-results.csv",
        "loop-data.csv",
        "qualifying.csv",
        "practice-01.csv",
        "practice-1.csv",
    ]
    for week_dir in sorted(frcs_year.iterdir()):
        if not week_dir.is_dir():
            continue
        rr_week = rr_year / week_dir.name
        rr_week.mkdir(parents=True, exist_ok=True)
        for name in names:
            src = week_dir / name
            if src.is_file():
                dst_name = "practice-1.csv" if name == "practice-01.csv" else name
                shutil.copyfile(src, rr_week / dst_name)


@pytest.mark.regression
def test_regression_runtime_2010_stage_and_resolve_pipeline(monkeypatch) -> None:
    _require_db_conn()
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    frcs_year = _SCRAPER_ROOT / "frcs.pro" / "cup" / "2010"
    rr_year = _SCRAPER_ROOT / "www.racing-reference.info" / "cup" / "2010"
    ti = _DummyTI()
    ctx = {
        "ti": ti,
        "data_interval_start": datetime(2010, 2, 1, tzinfo=UTC),
        "logical_date": datetime(2010, 2, 1, tzinfo=UTC),
    }
    _ensure_2010_schedule_seeded(hook, ctx)
    _copy_frcs_csvs_to_rr_for_2010(frcs_year, rr_year)
    weeks = _target_weeks_for_2010(hook, frcs_year)
    if not weeks:
        pytest.fail("Regression DB has no CUP 2010 weeks after schedule seed/import")
    monkeypatch.setattr(importer_mod, "_pull_target_weeks", lambda *_a, **_k: weeks)
    monkeypatch.setattr(importer_mod, "_emit_cup_importer_domain_event", lambda *_a, **_k: None)

    importer_mod.load_stage_practices_csvs(**ctx)
    importer_mod.load_stage_qualifyings_csvs(**ctx)
    importer_mod.load_stage_lineup_csvs(**ctx)
    importer_mod.load_stage_entrylist_csvs(**ctx)
    importer_mod.load_stage_races_csvs(**ctx)
    importer_mod.load_stage_penalties_csvs(**ctx)
    importer_mod.load_stage_pitstops_csvs(**ctx)
    importer_mod.load_stage_draftkings_csvs(**ctx)
    importer_mod.load_stage_fanduel_csvs(**ctx)
    importer_mod.resolve_drivers_cars_manufacturers_teams(**ctx)
    importer_mod.import_races(**ctx)

    null_car_driver_rows = hook.get_first(
        """
        SELECT COUNT(*)
        FROM nascar.race_results rr
        JOIN nascar.races r ON r.id = rr.race_id
        JOIN nascar.weeks w ON w.id = r.week_id
        JOIN nascar.seasons s ON s.id = w.season_id
        WHERE s.series_id = %s
          AND s.year = %s
          AND rr.deleted_at IS NULL
          AND r.deleted_at IS NULL
          AND w.deleted_at IS NULL
          AND s.deleted_at IS NULL
          AND rr.car_driver_id IS NULL
        """,
        parameters=(1, 2010),
    )
    assert null_car_driver_rows is not None
    assert int(null_car_driver_rows[0]) == 0

    week1_row = hook.get_first(
        """
        SELECT r.id
        FROM nascar.races r
        JOIN nascar.weeks w ON w.id = r.week_id
        JOIN nascar.seasons s ON s.id = w.season_id
        WHERE s.series_id = %s
          AND s.year = %s
          AND w.number = %s
          AND r.deleted_at IS NULL
          AND w.deleted_at IS NULL
          AND s.deleted_at IS NULL
        LIMIT 1
        """,
        parameters=(1, 2010, 1),
    )
    assert week1_row is not None
    race_id = int(week1_row[0])
    rows = hook.get_records("CALL nascar.enriched_race_results(%s)", parameters=(race_id,))
    by_driver: dict[str, tuple] = {}
    for row in rows:
        first_name = row[4]
        last_name = row[5]
        if first_name is None or last_name is None:
            continue
        by_driver[f"{first_name} {last_name}"] = row
    for target in ("Marcos Ambrose", "Michael McDowell"):
        assert target in by_driver
        assert by_driver[target][12] is not None
