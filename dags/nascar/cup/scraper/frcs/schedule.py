"""
Hourly FRCS.pro Cup schedule scrape (HTML → CSV).

Uses task helpers in ``include.nascar.data_sources.frcs`` with the same
``storage_path_series_year`` layout as other nascar scraper sources under
``…/<host>/cup/<year>/``.

Downstream DAGs should depend on the emitted **Asset** after each successful run::

    from airflow.sdk import DAG
    from nascar.cup.scraper.frcs.schedule import NASCAR_FRCS_CUP_SCHEDULE_ASSET

    with DAG(
        dag_id=\"my_downstream_dag\",
        schedule=[NASCAR_FRCS_CUP_SCHEDULE_ASSET],
        ...
    ):
        ...
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import Asset

from nascar.cup.scraper.backfill_assets import (
    BACKFILL_END_DATE,
    BACKFILL_FRCS_CUP_SCHEDULE_ASSET,
    BACKFILL_START_DATE,
    BACKFILL_YEARLY_SCHEDULE,
)
from nascar.cup.scraper.dag_factories import register_frcs_schedule_dag

DAG_ID = "nascar.cup.scraper.frcs.schedule"

NASCAR_FRCS_CUP_SCHEDULE_ASSET = Asset(
    uri="nascar://frcs/cup/schedule.csv",
    name=DAG_ID,
)

_BACKFILL_DOC = (
    "Yearly historical Cup FRCS schedule scrape (2010–2025): Dec 31 00:00 UTC, "
    "``catchup=True``. Uses backfill-only assets so prod DAGs are not triggered.\n\n"
    "**Source:** FRCS.pro Cup schedule HTML → CSV."
)

register_frcs_schedule_dag(
    dag_id=DAG_ID,
    schedule="0 * * * *",
    start_date=datetime(2025, 7, 11),
    end_date=None,
    catchup=False,
    outlet_asset=NASCAR_FRCS_CUP_SCHEDULE_ASSET,
    doc_md=__doc__,
)

register_frcs_schedule_dag(
    dag_id=f"backfill.{DAG_ID}",
    schedule=BACKFILL_YEARLY_SCHEDULE,
    start_date=BACKFILL_START_DATE,
    end_date=BACKFILL_END_DATE,
    catchup=True,
    outlet_asset=BACKFILL_FRCS_CUP_SCHEDULE_ASSET,
    doc_md=_BACKFILL_DOC,
)
