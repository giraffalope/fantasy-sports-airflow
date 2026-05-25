"""Install minimal Airflow MySQL provider stubs for hook unit tests; restore after module."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

import pytest

_AIRFLOW_STUB_NAMES = (
    "airflow",
    "airflow.providers",
    "airflow.providers.mysql",
    "airflow.providers.mysql.hooks",
    "airflow.providers.mysql.hooks.mysql",
)

_RELOAD_AFTER_STUB = (
    "airflow_utils.nascar_mysql_hook",
    "airflow_utils.mysql_connection",
    "airflow_utils.mysql_template_file_executor",
)


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


@pytest.fixture(scope="module", autouse=True)
def _airflow_mysql_hook_stub_module(request: pytest.FixtureRequest) -> None:
    """Stub Airflow only while this test module runs; restore real packages afterward."""
    mod = request.module
    _stub_modules = (
        "test_mysql_connection_and_transaction",
        "test_nascar_mysql_hook_insert_id",
    )
    if not any(mod.__name__ == n or mod.__name__.endswith(f".{n}") for n in _stub_modules):
        yield
        return

    saved = {name: sys.modules.get(name) for name in _AIRFLOW_STUB_NAMES + _RELOAD_AFTER_STUB}
    _install_airflow_stubs()
    hook_mod = importlib.reload(importlib.import_module("airflow_utils.nascar_mysql_hook"))
    conn_mod = importlib.reload(importlib.import_module("airflow_utils.mysql_connection"))
    exec_mod = importlib.reload(
        importlib.import_module("airflow_utils.mysql_template_file_executor")
    )
    mod.NascarMySqlHook = hook_mod.NascarMySqlHook  # type: ignore[attr-defined]
    if hasattr(mod, "connection_set_autocommit"):
        mod.connection_set_autocommit = conn_mod.connection_set_autocommit  # type: ignore[attr-defined]
    if hasattr(mod, "execute_sql_statements_transactional"):
        mod.execute_sql_statements_transactional = exec_mod.execute_sql_statements_transactional  # type: ignore[attr-defined]

    yield

    for name, previous in saved.items():
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
    for name in _RELOAD_AFTER_STUB:
        if sys.modules.get(name) is not None:
            importlib.reload(importlib.import_module(name))
