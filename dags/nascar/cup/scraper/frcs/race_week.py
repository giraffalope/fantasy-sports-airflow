"""
Per-week FRCS.pro Cup HTML → CSV sync, driven by ``schedule.csv``.

**Schedule:** runs when ``NASCAR_FRCS_CUP_SCHEDULE_ASSET`` updates — in practice each
time ``nascar.cup.scraper.frcs.schedule`` completes successfully (that DAG is hourly).
Layout matches other scrapers: ``…/frcs.pro/cup/<year>/<week>/``.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import Asset

from nascar.cup.scraper.backfill_assets import (
    BACKFILL_END_DATE,
    BACKFILL_FRCS_CUP_RACE_WEEK_ASSET,
    BACKFILL_FRCS_CUP_SCHEDULE_ASSET,
    BACKFILL_START_DATE,
)
from nascar.cup.scraper.dag_factories import register_frcs_race_week_dag
from nascar.cup.scraper.frcs.schedule import NASCAR_FRCS_CUP_SCHEDULE_ASSET

DAG_ID = "nascar.cup.scraper.frcs.race_week"

NASCAR_FRCS_CUP_RACE_WEEK_ASSET = Asset(
    uri="nascar://frcs/cup/race-week.csv",
    name=DAG_ID,
)

_BACKFILL_DOC = (
    "Historical Cup FRCS per-week HTML → CSV (2010–2025), triggered when the "
    "backfill FRCS schedule asset updates. Uses backfill-only assets.\n\n"
    + (__doc__ or "")
)

register_frcs_race_week_dag(
    dag_id=DAG_ID,
    schedule=[NASCAR_FRCS_CUP_SCHEDULE_ASSET],
    start_date=datetime(2025, 7, 11),
    end_date=None,
    catchup=False,
    outlet_asset=NASCAR_FRCS_CUP_RACE_WEEK_ASSET,
    doc_md=__doc__,
)

register_frcs_race_week_dag(
    dag_id=f"backfill.{DAG_ID}",
    schedule=[BACKFILL_FRCS_CUP_SCHEDULE_ASSET],
    start_date=BACKFILL_START_DATE,
    end_date=BACKFILL_END_DATE,
    catchup=True,
    outlet_asset=BACKFILL_FRCS_CUP_RACE_WEEK_ASSET,
    doc_md=_BACKFILL_DOC,
)
