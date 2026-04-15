"""
Per-week Racing Reference Cup HTML → CSV sync, driven by ``schedule.csv``.

**Schedule:** hourly (same cron as ``nascar.cup.scraper.racing_reference.schedule``).
``wait_upstream_schedule_convert`` waits for that DAG's
``convert_year_schedule_html_to_csv`` for the same logical date so context
dates match the schedule run.

Downloads missing HTML where needed and emits CSVs under ``…/cup/<year>/<week>/``.
The last task is ``sync_race_results`` (race results CSV plus loop-data HTML/CSV).
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import Asset
from nascar.cup.scraper.backfill_assets import (
    BACKFILL_END_DATE,
    BACKFILL_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
    BACKFILL_START_DATE,
    BACKFILL_YEARLY_SCHEDULE,
    CUP_SCRAPER_PROD_HOURLY_CRON,
)
from nascar.cup.scraper.dag_factories import register_racing_reference_race_week_dag

DAG_ID = "nascar.cup.scraper.racing_reference.race_week"

NASCAR_RACING_REFERENCE_CUP_RACE_WEEK_ASSET = Asset(
    uri="nascar://racing-reference/cup/race-week.csv",
    name=DAG_ID,
)

_BACKFILL_DOC = (
    "Historical Cup Racing Reference per-week HTML → CSV (2010–2025). Yearly cron "
    "with ``wait_upstream_schedule_convert`` on "
    "``backfill.nascar.cup.scraper.racing_reference.schedule``.\n\n"
    + (__doc__ or "")
)

dag = register_racing_reference_race_week_dag(
    dag_id=DAG_ID,
    schedule=CUP_SCRAPER_PROD_HOURLY_CRON,
    start_date=datetime(2025, 7, 11),
    end_date=None,
    catchup=False,
    outlet_asset=NASCAR_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
    wait_for_schedule_dag_id="nascar.cup.scraper.racing_reference.schedule",
    doc_md=__doc__,
)

backfill_dag = register_racing_reference_race_week_dag(
    dag_id=f"backfill.{DAG_ID}",
    schedule=BACKFILL_YEARLY_SCHEDULE,
    start_date=BACKFILL_START_DATE,
    end_date=BACKFILL_END_DATE,
    catchup=True,
    outlet_asset=BACKFILL_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
    wait_for_schedule_dag_id="backfill.nascar.cup.scraper.racing_reference.schedule",
    doc_md=_BACKFILL_DOC,
)
