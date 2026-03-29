# airflow_utils/__init__.py
# Lazy exports: importing a submodule (e.g. wait_operator) must not pull in MySQL hooks,
# so lightweight test images and DAGs that only need WaitOperator stay free of mysqlclient.

from __future__ import annotations

__all__ = [
    "MySQLTemplateFileOperator",
    "MySqlTemplateFileExecutor",
    "WaitOperator",
]


def __getattr__(name: str):
    if name == "MySQLTemplateFileOperator":
        from .mysql_template_file_operator import MySQLTemplateFileOperator

        return MySQLTemplateFileOperator
    if name == "MySqlTemplateFileExecutor":
        from .mysql_template_file_executor import MySqlTemplateFileExecutor

        return MySqlTemplateFileExecutor
    if name == "WaitOperator":
        from .wait_operator import WaitOperator

        return WaitOperator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
