"""DB runtime: schedule import resolves tracks via data_sources_tracks.track_id."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import pytest

import nascar.cup.importer as importer_mod

from airflow_utils.nascar_mysql_hook import NascarMySqlHook


_CANONICAL_NAME = "Schedule Mapping Canonical Track"
_TRUNCATED_NAME = "Schedule Mapping Canon…"
_RR_NAME = "Schedule Map RR"
_TEST_YEAR = 2098
_TEST_EVENT = 998
_TABLE_SUFFIX = "209801011200"


def _require_db_conn() -> None:
    if not os.environ.get("AIRFLOW_CONN_NASCAR_CONN"):
        pytest.skip("AIRFLOW_CONN_NASCAR_CONN not configured for DB runtime integration test")


def _execute(hook: NascarMySqlHook, sql: str, params: tuple[Any, ...] | None = None) -> None:
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or ())
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _cleanup(hook: NascarMySqlHook) -> None:
    _execute(
        hook,
        """
        DELETE w FROM nascar.weeks w
        JOIN nascar.seasons s ON s.id = w.season_id
        WHERE s.series_id = %s AND s.year = %s AND w.number = %s
        """,
        (1, _TEST_YEAR, _TEST_EVENT),
    )
    _execute(
        hook,
        """
        DELETE FROM nascar.data_sources_tracks
        WHERE name IN (%s, %s, %s)
        """,
        (_TRUNCATED_NAME, _RR_NAME, _CANONICAL_NAME),
    )
    _execute(
        hook,
        """
        DELETE FROM nascar.tracks
        WHERE name IN (%s, %s)
        """,
        (_TRUNCATED_NAME, _CANONICAL_NAME),
    )
    _execute(
        hook,
        """
        DELETE FROM nascar.seasons WHERE series_id = %s AND year = %s
        """,
        (1, _TEST_YEAR),
    )


def test_import_schedule_uses_data_sources_tracks_mapping_not_truncated_name() -> None:
    _require_db_conn()
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    _cleanup(hook)

    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO nascar.tracks (name)
            VALUES (%s)
            ON DUPLICATE KEY UPDATE deleted_at = NULL
            """,
            (_CANONICAL_NAME,),
        )
        cur.execute(
            "SELECT id FROM nascar.tracks WHERE name = %s AND deleted_at IS NULL LIMIT 1",
            (_CANONICAL_NAME,),
        )
        canonical_track_id = int(cur.fetchone()[0])

        cur.execute(
            """
            INSERT INTO nascar.tracks (name, deleted_at)
            VALUES (%s, CURRENT_TIMESTAMP)
            ON DUPLICATE KEY UPDATE deleted_at = CURRENT_TIMESTAMP
            """,
            (_TRUNCATED_NAME,),
        )

        for source_name in ("frcs", "racing_reference"):
            cur.execute(
                """
                SELECT id FROM nascar.data_sources
                WHERE name = %s AND deleted_at IS NULL
                LIMIT 1
                """,
                (source_name,),
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    "INSERT INTO nascar.data_sources (name, url) VALUES (%s, %s)",
                    (source_name, f"https://example.test/{source_name}"),
                )
                cur.execute(
                    "SELECT id FROM nascar.data_sources WHERE name = %s LIMIT 1",
                    (source_name,),
                )
                row = cur.fetchone()
            source_id = int(row[0])

        cur.execute("SELECT id FROM nascar.data_sources WHERE name = 'frcs' LIMIT 1")
        frcs_source_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO nascar.data_sources_tracks (data_source_id, name, track_id)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE track_id = VALUES(track_id), deleted_at = NULL
            """,
            (frcs_source_id, _TRUNCATED_NAME, canonical_track_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    _execute(
        hook,
        f"DROP TABLE IF EXISTS nascar.stg_cup_frcs_schedule_import_{_TABLE_SUFFIX}",
    )
    _execute(
        hook,
        f"""
        CREATE TABLE nascar.stg_cup_frcs_schedule_import_{_TABLE_SUFFIX} (
            event_number INT UNSIGNED NULL,
            week_number INT UNSIGNED NULL,
            week_url VARCHAR(255) NULL,
            week_date DATE NULL,
            week_track VARCHAR(255) NULL,
            week_track_url VARCHAR(255) NULL,
            week_race_name VARCHAR(255) NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
        """,
    )
    _execute(
        hook,
        f"""
        INSERT INTO nascar.stg_cup_frcs_schedule_import_{_TABLE_SUFFIX}
            (event_number, week_number, week_url, week_date, week_track, week_track_url, week_race_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            _TEST_EVENT,
            1,
            "https://example.test/frcs/week",
            "2098-01-01",
            _TRUNCATED_NAME,
            "https://example.test/frcs/track",
            "Schedule Mapping Test Race",
        ),
    )
    _execute(
        hook,
        f"DROP TABLE IF EXISTS nascar.stg_cup_racing_reference_schedule_import_{_TABLE_SUFFIX}",
    )
    _execute(
        hook,
        f"""
        CREATE TABLE nascar.stg_cup_racing_reference_schedule_import_{_TABLE_SUFFIX} (
            week_number INT UNSIGNED NULL,
            week_url VARCHAR(255) NULL,
            week_date DATE NULL,
            week_track VARCHAR(255) NULL,
            week_track_url VARCHAR(255) NULL,
            week_race_name VARCHAR(255) NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
        """,
    )
    _execute(
        hook,
        f"""
        INSERT INTO nascar.stg_cup_racing_reference_schedule_import_{_TABLE_SUFFIX}
            (week_number, week_url, week_date, week_track, week_track_url, week_race_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            1,
            "https://example.test/rr/week",
            "2098-01-01",
            _RR_NAME,
            "https://example.test/rr/track",
            "Schedule Mapping Test Race",
        ),
    )

    context = {
        "data_interval_start": datetime(2098, 1, 1, tzinfo=UTC),
        "logical_date": datetime(2098, 1, 1, tzinfo=UTC),
    }
    import_params = importer_mod._importer_params_import_schedule(context)
    import_params["table_suffix"] = _TABLE_SUFFIX
    import_params["season_year"] = _TEST_YEAR

    importer_mod._execute_templated_sql_file(
        hook,
        importer_mod._sql_root() / "nascar.cup.importer" / "import_schedule.sql",
        import_params,
        import_params["frcs_source_name"],
    )

    revived = hook.get_first(
        """
        SELECT COUNT(*)
        FROM nascar.tracks
        WHERE name = %s AND deleted_at IS NULL
        """,
        parameters=(_TRUNCATED_NAME,),
    )[0]
    assert int(revived) == 0

    week_row = hook.get_first(
        """
        SELECT w.track_id
        FROM nascar.weeks w
        JOIN nascar.seasons s ON s.id = w.season_id
        WHERE s.series_id = %s
          AND s.year = %s
          AND w.number = %s
          AND w.deleted_at IS NULL
          AND s.deleted_at IS NULL
        LIMIT 1
        """,
        parameters=(1, _TEST_YEAR, _TEST_EVENT),
    )
    assert week_row is not None
    assert int(week_row[0]) == canonical_track_id

    mapped = hook.get_first(
        """
        SELECT dst.track_id
        FROM nascar.data_sources_tracks dst
        JOIN nascar.data_sources ds ON ds.id = dst.data_source_id
        WHERE ds.name = 'frcs'
          AND dst.name = %s
          AND dst.deleted_at IS NULL
        LIMIT 1
        """,
        parameters=(_TRUNCATED_NAME,),
    )
    assert mapped is not None
    assert int(mapped[0]) == canonical_track_id

    _cleanup(hook)
