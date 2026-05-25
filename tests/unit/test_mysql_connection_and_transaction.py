"""Unit tests for MySQL connection helpers and transactional hook execution."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from airflow_utils.mysql_connection import connection_set_autocommit
from airflow_utils.mysql_template_file_executor import execute_sql_statements_transactional
from airflow_utils.nascar_mysql_hook import NascarMySqlHook  # rebound by airflow_hook_stub fixture


def test_connection_set_autocommit_noop_when_unsupported() -> None:
    conn = object()
    connection_set_autocommit(conn, False)


def test_run_in_transaction_commits_on_success() -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    conn = MagicMock()
    cursor_cm = MagicMock()
    cursor = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = False
    conn.cursor.return_value = cursor_cm
    hook.get_conn = MagicMock(return_value=conn)

    seen: list[str] = []

    def write_fn(_conn: object, _cursor: object) -> None:
        seen.append("ok")

    hook.run_in_transaction(write_fn)

    assert seen == ["ok"]
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()
    conn.close.assert_called_once()


def test_run_in_transaction_rolls_back_on_write_failure() -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    conn = MagicMock()
    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = MagicMock()
    cursor_cm.__exit__.return_value = False
    conn.cursor.return_value = cursor_cm
    hook.get_conn = MagicMock(return_value=conn)

    def write_fn(_conn: object, _cursor: object) -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        hook.run_in_transaction(write_fn)

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()


def test_execute_sql_statements_transactional_rolls_back_mid_statement() -> None:
    hook = MagicMock()
    conn = MagicMock()
    cursor = MagicMock()
    cursor.execute.side_effect = [None, RuntimeError("stmt2 failed")]
    hook.get_conn.return_value = conn
    conn.cursor.return_value = cursor

    with pytest.raises(RuntimeError, match="stmt2 failed"):
        execute_sql_statements_transactional(hook, ["SELECT 1", "SELECT 2"])

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    conn.close.assert_called_once()
