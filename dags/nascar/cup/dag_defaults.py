"""Shared default_args for Cup NASCAR DAGs."""

from __future__ import annotations

from datetime import timedelta

CUP_DAG_DEFAULT_ARGS: dict = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}
