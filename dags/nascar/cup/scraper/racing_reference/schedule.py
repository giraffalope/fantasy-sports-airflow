"""
Hourly Racing Reference Cup schedule scrape (HTML → CSV).

Uses Racing Reference task helpers in ``include.nascar.data_sources.racing_reference``
(same ``storage_path_series_year`` layout as the Cup pipeline).

Downstream DAGs should depend on the emitted **Asset** so they run after each
successful hourly conversion::

    from airflow.sdk import DAG
    from nascar.cup.scraper.racing_reference.schedule import (
        NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET,
    )

    with DAG(
        dag_id=\"my_downstream_dag\",
        schedule=[NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET],
        ...
    ):
        ...
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import Asset

from nascar.cup.scraper.backfill_assets import (
    BACKFILL_END_DATE,
    BACKFILL_RACING_REFERENCE_CUP_SCHEDULE_ASSET,
    BACKFILL_START_DATE,
    BACKFILL_YEARLY_SCHEDULE,
)
from nascar.cup.scraper.dag_factories import register_racing_reference_schedule_dag

DAG_ID = "nascar.cup.scraper.racing_reference.schedule"

# Logical asset updated when Cup schedule.csv is regenerated (last task succeeds).
NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET = Asset(
    uri="nascar://racing-reference/cup/schedule.csv",
    name=DAG_ID,
)

_BACKFILL_DOC = (
    "Yearly historical Cup Racing Reference schedule scrape (2010–2025): Dec 31 00:00 UTC, "
    "``catchup=True``. Uses backfill-only assets so prod DAGs are not triggered.\n\n"
    "**Source:** Racing Reference Cup schedule HTML → CSV."
)

register_racing_reference_schedule_dag(
    dag_id=DAG_ID,
    schedule="0 * * * *",
    start_date=datetime(2025, 7, 11),
    end_date=None,
    catchup=False,
    outlet_asset=NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET,
    doc_md=__doc__,
)

register_racing_reference_schedule_dag(
    dag_id=f"backfill.{DAG_ID}",
    schedule=BACKFILL_YEARLY_SCHEDULE,
    start_date=BACKFILL_START_DATE,
    end_date=BACKFILL_END_DATE,
    catchup=True,
    outlet_asset=BACKFILL_RACING_REFERENCE_CUP_SCHEDULE_ASSET,
    doc_md=_BACKFILL_DOC,
)
