"""Unit tests for :meth:`NascarMySqlHook.run_insert_returning_id`."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock


class _MySqlHookStub:
    def __init__(self, mysql_conn_id: str | None = None, **_: object) -> None:
        self.mysql_conn_id = mysql_conn_id


def _install_airflow_stubs() -> None:
    mysql_hooks = ModuleType("airflow.providers.mysql.hooks.mysql")
    mysql_hooks.MySqlHook = _MySqlHookStub  # type: ignore[attr-defined]
    providers_mysql_hooks = ModuleType("airflow.providers.mysql.hooks")
    providers_mysql_hooks.mysql = mysql_hooks  # type: ignore[attr-defined]
    providers_mysql = ModuleType("airflow.providers.mysql")
    providers_mysql.hooks = providers_mysql_hooks  # type: ignore[attr-defined]
    providers = ModuleType("airflow.providers")
    providers.mysql = providers_mysql  # type: ignore[attr-defined]
    airflow = ModuleType("airflow")
    airflow.providers = providers  # type: ignore[attr-defined]

    for name, module in (
        ("airflow", airflow),
        ("airflow.providers", providers),
        ("airflow.providers.mysql", providers_mysql),
        ("airflow.providers.mysql.hooks", providers_mysql_hooks),
        ("airflow.providers.mysql.hooks.mysql", mysql_hooks),
    ):
        sys.modules[name] = module


_install_airflow_stubs()

# Import after stubs so NascarMySqlHook subclasses _MySqlHookStub, not MagicMock.
from airflow_utils.nascar_mysql_hook import NascarMySqlHook  # noqa: E402


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
