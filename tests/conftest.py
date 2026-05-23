"""Pytest hooks shared across the Airflow test suite."""

from __future__ import annotations

import os

# Register backfill.* DAGs during unit tests (DAG modules import at collection time).
os.environ.setdefault("FS_ENABLE_BACKFILL_DAGS", "1")
