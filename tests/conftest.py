"""Pytest hooks shared across the Airflow test suite."""

from __future__ import annotations

import os

import pytest

# Register backfill.* DAGs during unit tests (DAG modules import at collection time).
os.environ.setdefault("FS_ENABLE_BACKFILL_DAGS", "1")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: tests that require a live MariaDB (AIRFLOW_CONN_NASCAR_CONN)",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    del config
    if os.environ.get("AIRFLOW_CONN_NASCAR_CONN"):
        return
    skip_db = pytest.mark.skip(reason="AIRFLOW_CONN_NASCAR_CONN not configured")
    for item in items:
        if item.get_closest_marker("integration"):
            item.add_marker(skip_db)
