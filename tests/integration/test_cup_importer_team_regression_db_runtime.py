from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import nascar.cup.importer as importer_mod
import pytest

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from tests.integration.cup_importer_db_helpers import cleanup_week_race_results

pytestmark = pytest.mark.integration

_SHARED_SCRAPER_ROOT = Path("/home/airflow/scraper/nascar")


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
        pytest.skip("AIRFLOW_CONN_NASCAR_CONN not configured for DB runtime integration test")


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
        importer_mod._sql_root() / "nascar.cup.importer" / "load_stage_schedule_csvs.sql",
        load_params,
        import_params["frcs_source_name"],
    )
    importer_mod._execute_templated_sql_file(
        hook_seed,
        importer_mod._sql_root() / "nascar.cup.importer" / "import_schedule.sql",
        import_params,
        import_params["frcs_source_name"],
    )


def _cup_2010_week_1_ids(hook: NascarMySqlHook) -> tuple[int, int, int] | None:
    row = hook.get_first(
        """
        SELECT r.id, w.id, w.season_id
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
    if not row:
        return None
    return int(row[0]), int(row[1]), int(row[2])


def _prepare_week_1_shared_csv_inputs(scraper_root: Path) -> None:
    frcs_base = scraper_root / "frcs.pro" / "cup" / "2010" / "01"
    if not frcs_base.is_dir():
        pytest.skip("Required FRCS fixture CSV folder missing for 2010 week 1")

    required = ["entrylist.csv", "lineup.csv", "race-results.csv", "loop-data.csv"]
    for name in required:
        if not (frcs_base / name).is_file():
            pytest.skip(f"Missing required FRCS fixture CSV for 2010 week 1: {name}")

    rr_base = scraper_root / "www.racing-reference.info" / "cup" / "2010" / "01"
    rr_base.mkdir(parents=True, exist_ok=True)

    # Use identical week-1 CSVs for FRCS and RR to isolate resolver behavior.
    for name in [
        "entrylist.csv",
        "lineup.csv",
        "race-results.csv",
        "loop-data.csv",
        "qualifying.csv",
        "practice-01.csv",
    ]:
        src = frcs_base / name
        if src.is_file():
            dst_name = "practice-1.csv" if name == "practice-01.csv" else name
            shutil.copyfile(src, rr_base / dst_name)


@pytest.mark.regression
def test_week1_enriched_race_results_has_non_null_team_names_for_target_drivers(
    monkeypatch,
) -> None:
    _require_db_conn()
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    ti = _DummyTI()
    seed_ctx = {
        "ti": ti,
        "data_interval_start": datetime(2010, 2, 1, tzinfo=UTC),
        "logical_date": datetime(2010, 2, 1, tzinfo=UTC),
    }
    _ensure_2010_schedule_seeded(hook, seed_ctx)
    _prepare_week_1_shared_csv_inputs(_SHARED_SCRAPER_ROOT)

    ids = _cup_2010_week_1_ids(hook)
    if ids is None:
        pytest.fail(
            "Could not resolve CUP 2010 week 1 race/week/season ids after schedule seed/import"
        )
    race_id, week_id, season_id = ids
    cleanup_week_race_results(hook, week_id)

    ctx = {
        "ti": ti,
        "data_interval_start": datetime(2010, 2, 1, tzinfo=UTC),
        "logical_date": datetime(2010, 2, 1, tzinfo=UTC),
    }

    monkeypatch.setattr(
        importer_mod, "_pull_target_weeks", lambda *_a, **_k: [(week_id, season_id, 1, 1)]
    )
    monkeypatch.setattr(importer_mod, "_emit_cup_importer_domain_event", lambda *_a, **_k: None)

    importer_mod.load_stage_lineup_csvs(**ctx)
    importer_mod.load_stage_entrylist_csvs(**ctx)
    importer_mod.load_stage_races_csvs(**ctx)
    importer_mod.resolve_drivers_cars_manufacturers_teams(**ctx)
    importer_mod.import_races(**ctx)

    rows = hook.get_records("CALL nascar.enriched_race_results(%s)", parameters=(race_id,))
    by_driver: dict[str, tuple] = {}
    for row in rows:
        first_name = row[4]
        last_name = row[5]
        if first_name is None or last_name is None:
            continue
        by_driver[f"{first_name} {last_name}"] = row

    for target in ("Marcos Ambrose", "Michael McDowell"):
        assert target in by_driver, f"Missing driver in enriched_race_results: {target}"
        assert by_driver[target][12] is not None, f"team_name is NULL for {target}"
