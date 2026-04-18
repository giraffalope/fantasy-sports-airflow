from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import nascar.cup.scraper.importer as importer_mod

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

    importer_mod.load_stage_frcs_entrylist_csvs(**ctx)
    importer_mod.import_frcs_entrylist(**ctx)
    importer_mod.load_stage_frcs_practices_csvs(**ctx)
    importer_mod.import_frcs_practices(**ctx)
    importer_mod.load_stage_frcs_qualifyings_csvs(**ctx)
    importer_mod.import_frcs_qualifyings(**ctx)

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
