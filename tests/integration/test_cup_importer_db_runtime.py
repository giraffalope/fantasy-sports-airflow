from __future__ import annotations

import csv
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import nascar.cup.importer as importer_mod

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.data_sources.frcs_html_to_csv import FrcsHtmlToCsv


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


def _cup_2025_week_25_id(hook: NascarMySqlHook) -> int | None:
    row = hook.get_first(
        """
        SELECT w.id
        FROM nascar.weeks w
        JOIN nascar.seasons s ON s.id = w.season_id
        WHERE s.series_id = %s
          AND s.year = %s
          AND w.number = %s
          AND s.deleted_at IS NULL
          AND w.deleted_at IS NULL
        LIMIT 1
        """,
        parameters=(1, 2025, 25),
    )
    return int(row[0]) if row else None


def _ensure_cup_2025_week_25(hook: NascarMySqlHook) -> int:
    series_row = hook.get_first(
        """
        SELECT id
        FROM nascar.series
        WHERE deleted_at IS NULL
          AND LOWER(name) = 'cup'
        LIMIT 1
        """
    )
    if not series_row:
        raise RuntimeError("Missing nascar.series row for 'cup'")
    series_id = int(series_row[0])

    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO nascar.seasons (series_id, year)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE deleted_at = NULL
            """,
            (series_id, 2025),
        )
        cur.execute(
            """
            INSERT INTO nascar.tracks (name, description)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE deleted_at = NULL
            """,
            ("Integration Test Track", "Importer runtime integration test track"),
        )
        cur.execute(
            """
            SELECT id FROM nascar.seasons
            WHERE series_id = %s AND year = %s AND deleted_at IS NULL
            LIMIT 1
            """,
            (series_id, 2025),
        )
        season_id = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT id FROM nascar.tracks
            WHERE name = %s AND deleted_at IS NULL
            LIMIT 1
            """,
            ("Integration Test Track",),
        )
        track_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO nascar.weeks (season_id, track_id, number, season_number, race_name, scheduled_start)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                race_name = VALUES(race_name),
                scheduled_start = VALUES(scheduled_start),
                deleted_at = NULL
            """,
            (
                season_id,
                track_id,
                25,
                25,
                "Integration Test Week 25",
                "2025-08-01T00:00:00Z",
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    week_id = _cup_2025_week_25_id(hook)
    if week_id is None:
        raise RuntimeError("Failed to create or resolve CUP 2025 week 25")
    return week_id


def _cleanup_week_results(hook: NascarMySqlHook, week_id: int) -> None:
    statements = [
        """
        DELETE pr
        FROM nascar.practice_results pr
        JOIN nascar.practices p ON p.id = pr.practice_id
        WHERE p.week_id = %s
        """,
        """
        DELETE FROM nascar.practices WHERE week_id = %s
        """,
        """
        DELETE qr
        FROM nascar.qualifying_results qr
        JOIN nascar.qualifyings q ON q.id = qr.qualifying_id
        WHERE q.week_id = %s
        """,
        """
        DELETE FROM nascar.qualifyings WHERE week_id = %s
        """,
    ]
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        for sql in statements:
            cur.execute(sql, (week_id,))
        conn.commit()
    finally:
        cur.close()
        conn.close()


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


def _prepare_2010_week_1_csv_inputs_for_team_resolution() -> None:
    frcs_base = Path("/home/airflow/scraper/nascar/frcs.pro/cup/2010/01")
    if not frcs_base.is_dir():
        pytest.skip("Required FRCS fixture CSV folder missing for 2010 week 1")

    required = ["entrylist.csv", "lineup.csv", "race-results.csv", "loop-data.csv"]
    for name in required:
        if not (frcs_base / name).is_file():
            pytest.skip(f"Missing required FRCS fixture CSV for 2010 week 1: {name}")

    rr_base = Path("/home/airflow/scraper/nascar/www.racing-reference.info/cup/2010/01")
    rr_base.mkdir(parents=True, exist_ok=True)

    # Use the same canonical week fixtures for both sources as requested.
    for name in ["entrylist.csv", "lineup.csv", "race-results.csv", "loop-data.csv", "qualifying.csv", "practice-1.csv"]:
        src = frcs_base / name
        if src.is_file():
            shutil.copyfile(src, rr_base / name)


def _prepare_week_25_csv_inputs() -> None:
    base = Path("/home/airflow/scraper/nascar/frcs.pro/cup/2025/25")
    entry_html = base / "entrylist.html"
    qual_html = base / "qualifying.html"
    practice_html = base / "practice-01.html"
    if not entry_html.is_file() or not qual_html.is_file() or not practice_html.is_file():
        pytest.skip("Required FRCS fixture HTML missing for DB runtime importer test")

    FrcsHtmlToCsv.convert_entrylist(str(entry_html), str(base / "entrylist.csv"))
    FrcsHtmlToCsv.convert_qualifying(str(qual_html))
    FrcsHtmlToCsv.convert_practices([str(practice_html)])
    p01 = base / "practice-01.csv"
    if p01.is_file():
        shutil.copyfile(p01, base / "practice-1.csv")


def _force_first_data_row_zero(path: Path, *, time_col: str, speed_col: str) -> None:
    rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    if not rows:
        return
    rows[0][time_col] = "0"
    rows[0][speed_col] = "0"
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def test_importer_runtime_populates_practice_and_qualifying_results_in_db(monkeypatch) -> None:
    _require_db_conn()
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    _prepare_week_25_csv_inputs()
    week_id = _ensure_cup_2025_week_25(hook)

    _cleanup_week_results(hook, week_id)

    ti = _DummyTI()
    ctx = {
        "ti": ti,
        "data_interval_start": datetime(2025, 8, 1, tzinfo=UTC),
        "logical_date": datetime(2025, 8, 1, tzinfo=UTC),
    }

    monkeypatch.setattr(importer_mod, "_target_weeks", lambda *_a, **_k: [(week_id, 25)])
    monkeypatch.setattr(
        importer_mod,
        "_build_importer_target_weeks_xcom_rows",
        lambda *_a, **_k: [{"week_id": week_id, "event_number": 25, "rr_week_number": None}],
    )
    monkeypatch.setattr(
        importer_mod,
        "_emit_cup_importer_domain_event",
        lambda *_a, **_k: None,
    )

    importer_mod.load_stage_entrylist_csvs(**ctx)
    importer_mod.import_entrylist(**ctx)
    importer_mod.load_stage_practices_csvs(**ctx)
    importer_mod.import_practices(**ctx)
    importer_mod.load_stage_qualifyings_csvs(**ctx)
    importer_mod.import_qualifyings(**ctx)

    practice_count = hook.get_first(
        "SELECT COUNT(*) FROM nascar.practices WHERE week_id = %s",
        parameters=(week_id,),
    )[0]
    practice_results_count = hook.get_first(
        """
        SELECT COUNT(*)
        FROM nascar.practice_results pr
        JOIN nascar.practices p ON p.id = pr.practice_id
        WHERE p.week_id = %s
        """,
        parameters=(week_id,),
    )[0]
    qualifying_count = hook.get_first(
        "SELECT COUNT(*) FROM nascar.qualifyings WHERE week_id = %s",
        parameters=(week_id,),
    )[0]
    qualifying_results_count = hook.get_first(
        """
        SELECT COUNT(*)
        FROM nascar.qualifying_results qr
        JOIN nascar.qualifyings q ON q.id = qr.qualifying_id
        WHERE q.week_id = %s
        """,
        parameters=(week_id,),
    )[0]

    assert int(practice_count) > 0
    assert int(practice_results_count) > 0
    assert int(qualifying_count) > 0
    assert int(qualifying_results_count) > 0


def test_importer_runtime_filters_rows_with_null_best_time_or_speed(monkeypatch) -> None:
    _require_db_conn()
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    _prepare_week_25_csv_inputs()
    base = Path("/home/airflow/scraper/nascar/frcs.pro/cup/2025/25")
    _force_first_data_row_zero(base / "practice-1.csv", time_col="lap_time", speed_col="lap_speed")
    _force_first_data_row_zero(base / "qualifying.csv", time_col="lap_time", speed_col="lap_speed")
    week_id = _ensure_cup_2025_week_25(hook)

    _cleanup_week_results(hook, week_id)

    ti = _DummyTI()
    ctx = {
        "ti": ti,
        "data_interval_start": datetime(2025, 8, 1, tzinfo=UTC),
        "logical_date": datetime(2025, 8, 1, tzinfo=UTC),
    }

    monkeypatch.setattr(importer_mod, "_target_weeks", lambda *_a, **_k: [(week_id, 25)])
    monkeypatch.setattr(
        importer_mod,
        "_build_importer_target_weeks_xcom_rows",
        lambda *_a, **_k: [{"week_id": week_id, "event_number": 25, "rr_week_number": None}],
    )
    monkeypatch.setattr(
        importer_mod,
        "_emit_cup_importer_domain_event",
        lambda *_a, **_k: None,
    )

    importer_mod.load_stage_entrylist_csvs(**ctx)
    importer_mod.import_entrylist(**ctx)
    importer_mod.load_stage_practices_csvs(**ctx)
    importer_mod.import_practices(**ctx)
    importer_mod.load_stage_qualifyings_csvs(**ctx)
    importer_mod.import_qualifyings(**ctx)

    practice_null_metric_rows = hook.get_first(
        """
        SELECT COUNT(*)
        FROM nascar.practice_results pr
        JOIN nascar.practices p ON p.id = pr.practice_id
        WHERE p.week_id = %s
          AND (pr.best_time IS NULL OR pr.best_speed IS NULL)
        """,
        parameters=(week_id,),
    )[0]
    qualifying_null_metric_rows = hook.get_first(
        """
        SELECT COUNT(*)
        FROM nascar.qualifying_results qr
        JOIN nascar.qualifyings q ON q.id = qr.qualifying_id
        WHERE q.week_id = %s
          AND (qr.best_time IS NULL OR qr.best_speed IS NULL)
        """,
        parameters=(week_id,),
    )[0]

    assert int(practice_null_metric_rows) == 0
    assert int(qualifying_null_metric_rows) == 0


def test_week_results_981_does_not_underflow_unsigned_subtraction() -> None:
    _require_db_conn()
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("CALL nascar.agg_week_results(%s)", (981,))
        rows = cur.fetchall()
        col_names = [meta[0] for meta in (cur.description or ())]
        assert "r_green_flag_pass_diff" in col_names
        diff_idx = col_names.index("r_green_flag_pass_diff")
        # We mainly guard against SQL 1690 underflow during proc execution.
        for row in rows:
            diff = row[diff_idx]
            if diff is not None:
                assert isinstance(diff, int | float)
    finally:
        cur.close()
        conn.close()


def test_importer_runtime_week1_enriched_race_results_has_non_null_team_names(monkeypatch) -> None:
    _require_db_conn()
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    _prepare_2010_week_1_csv_inputs_for_team_resolution()

    ids = _cup_2010_week_1_ids(hook)
    if ids is None:
        pytest.skip("Could not resolve CUP 2010 week 1 race/week/season ids")
    race_id, week_id, season_id = ids

    ti = _DummyTI()
    ctx = {
        "ti": ti,
        "data_interval_start": datetime(2010, 2, 1, tzinfo=UTC),
        "logical_date": datetime(2010, 2, 1, tzinfo=UTC),
    }

    monkeypatch.setattr(importer_mod, "_pull_target_weeks", lambda *_a, **_k: [(week_id, season_id, 1, 1)])
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
