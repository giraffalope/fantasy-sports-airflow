"""Unit tests for :meth:`NascarMySqlHook.run_insert_returning_id`."""

from __future__ import annotations

from unittest.mock import MagicMock

from airflow_utils.nascar_mysql_hook import NascarMySqlHook  # rebound by airflow_hook_stub fixture


def test_run_insert_returning_id_uses_lastrowid_on_same_connection() -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")

    cursor = MagicMock()
    cursor.lastrowid = 42
    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = False

    conn = MagicMock()
    conn.cursor.return_value = cursor_cm
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    hook.get_conn = MagicMock(return_value=conn_cm)

    insert_id = hook.run_insert_returning_id(
        "INSERT INTO nascar.ml_feature_sets (selected_features) VALUES (%s)",
        parameters=('["a"]',),
    )

    assert insert_id == 42
    cursor.execute.assert_called_once()
    conn.commit.assert_called_once()


def test_run_insert_returning_id_raises_when_lastrowid_is_zero() -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")

    cursor = MagicMock()
    cursor.lastrowid = 0
    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = False

    conn = MagicMock()
    conn.cursor.return_value = cursor_cm
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    hook.get_conn = MagicMock(return_value=conn_cm)

    try:
        hook.run_insert_returning_id("INSERT INTO nascar.ml_models DEFAULT VALUES")
    except RuntimeError as exc:
        assert "auto-increment" in str(exc).lower()
    else:
        raise AssertionError("expected RuntimeError when lastrowid is 0")
