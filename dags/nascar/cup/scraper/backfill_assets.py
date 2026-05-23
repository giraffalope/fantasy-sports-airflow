"""
Asset URIs for the yearly ``backfill.*`` Cup scraper DAGs.

Separate from production assets so backfill runs do not trigger prod DAGs (or vice versa).

Set ``FS_ENABLE_BACKFILL_DAGS`` to a truthy value (``1``, ``true``, ``yes``, ``on``) to
register ``backfill.*`` DAGs in the Airflow UI; default is off when unset or ``0``.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime
from typing import Any

from airflow.sdk import Asset, DAG

_ENABLE_BACKFILL_DAGS_ENV = "FS_ENABLE_BACKFILL_DAGS"

# Inclusive calendar-year backfill window (one DAG run per year on Dec 31).
BACKFILL_START_DATE = datetime(2010, 12, 31)
BACKFILL_END_DATE = datetime(2025, 12, 31)
BACKFILL_YEARLY_SCHEDULE = "0 0 31 12 *"

# Prod Cup scraper DAGs that must align on logical_date (hourly tick, top of hour UTC).
CUP_SCRAPER_PROD_HOURLY_CRON = "0 * * * *"

BACKFILL_FRCS_CUP_SCHEDULE_ASSET = Asset(
    uri="nascar://backfill/frcs/cup/schedule.csv",
    name="backfill.nascar.cup.scraper.frcs.schedule",
)
BACKFILL_RACING_REFERENCE_CUP_SCHEDULE_ASSET = Asset(
    uri="nascar://backfill/racing-reference/cup/schedule.csv",
    name="backfill.nascar.cup.scraper.racing_reference.schedule",
)
BACKFILL_FRCS_CUP_RACE_WEEK_ASSET = Asset(
    uri="nascar://backfill/frcs/cup/race-week.csv",
    name="backfill.nascar.cup.scraper.frcs.race_week",
)
BACKFILL_RACING_REFERENCE_CUP_RACE_WEEK_ASSET = Asset(
    uri="nascar://backfill/racing-reference/cup/race-week.csv",
    name="backfill.nascar.cup.scraper.racing_reference.race_week",
)


def backfill_dags_enabled() -> bool:
    """Return whether ``backfill.*`` Cup DAGs should be registered at import time."""
    raw = os.environ.get(_ENABLE_BACKFILL_DAGS_ENV)
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def maybe_register_backfill_dag(
    register_fn: Callable[..., DAG],
    /,
    **kwargs: Any,
) -> DAG | None:
    """Call ``register_fn`` only when :func:`backfill_dags_enabled` is true."""
    if not backfill_dags_enabled():
        return None
    return register_fn(**kwargs)
