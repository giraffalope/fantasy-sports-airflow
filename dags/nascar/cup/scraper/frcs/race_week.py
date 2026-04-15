"""
Per-week FRCS.pro Cup HTML → CSV sync, driven by ``schedule.csv``.

**Schedule:** hourly (same cron as ``nascar.cup.scraper.frcs.schedule``). A
``wait_upstream_schedule_convert`` task blocks until that DAG's
``convert_year_schedule_html_to_csv`` succeeds for the same logical date, so
``data_interval_start`` / ``logical_date`` stay aligned with the schedule run.

Layout matches other scrapers: ``…/frcs.pro/cup/<year>/<week>/``.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import Asset
from nascar.cup.scraper.backfill_assets import (
    BACKFILL_END_DATE,
    BACKFILL_FRCS_CUP_RACE_WEEK_ASSET,
    BACKFILL_START_DATE,
    BACKFILL_YEARLY_SCHEDULE,
    CUP_SCRAPER_PROD_HOURLY_CRON,
)
from nascar.cup.scraper.dag_factories import register_frcs_race_week_dag

DAG_ID = "nascar.cup.scraper.frcs.race_week"

NASCAR_FRCS_CUP_RACE_WEEK_ASSET = Asset(
    uri="nascar://frcs/cup/race-week.csv",
    name=DAG_ID,
)

_BACKFILL_DOC = (
    "Historical Cup FRCS per-week HTML → CSV (2010–2025). Yearly cron with "
    "``wait_upstream_schedule_convert`` on ``backfill.nascar.cup.scraper.frcs.schedule``.\n\n"
    + (__doc__ or "")
)

dag = register_frcs_race_week_dag(
    dag_id=DAG_ID,
    schedule=CUP_SCRAPER_PROD_HOURLY_CRON,
    start_date=datetime(2025, 7, 11),
    end_date=None,
    catchup=False,
    outlet_asset=NASCAR_FRCS_CUP_RACE_WEEK_ASSET,
    wait_for_schedule_dag_id="nascar.cup.scraper.frcs.schedule",
    doc_md=__doc__,
)

backfill_dag = register_frcs_race_week_dag(
    dag_id=f"backfill.{DAG_ID}",
    schedule=BACKFILL_YEARLY_SCHEDULE,
    start_date=BACKFILL_START_DATE,
    end_date=BACKFILL_END_DATE,
    catchup=True,
    outlet_asset=BACKFILL_FRCS_CUP_RACE_WEEK_ASSET,
    wait_for_schedule_dag_id="backfill.nascar.cup.scraper.frcs.schedule",
    doc_md=_BACKFILL_DOC,
)
