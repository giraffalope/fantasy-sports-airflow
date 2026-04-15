"""Shared helpers for Cup race-week DAG callables (FRCS, Racing Reference)."""

from __future__ import annotations

from typing import Any


def is_backfill_race_week_dag(context: dict[str, Any]) -> bool:
    """True when the active DAG is a backfill race-week DAG (``dag_id`` starts with ``backfill.``)."""
    dag = context.get("dag")
    if dag is None:
        return False
    dag_id = getattr(dag, "dag_id", None) or ""
    return str(dag_id).startswith("backfill.")
