"""Shared helpers for CUP importer DB integration tests."""

from __future__ import annotations

from airflow_utils.nascar_mysql_hook import NascarMySqlHook


def cleanup_week_race_results(hook: NascarMySqlHook, week_id: int) -> None:
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE rr
            FROM nascar.race_results rr
            JOIN nascar.races r ON r.id = rr.race_id
            WHERE r.week_id = %s
            """,
            (week_id,),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
