"""
Per-week Racing Reference Cup HTML → CSV sync, driven by ``schedule.csv``.

**Schedule:** this DAG is triggered when ``NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET``
updates — in practice, each time ``nascar.cup.scraper.racing_reference.schedule`` finishes
successfully (that DAG runs hourly). A short random wait task runs first, then tasks
download missing HTML (only when files are absent) and emit CSVs alongside the main
Cup pipeline layout under ``…/cup/<year>/<week>/`` (including prerace penalties,
caution flags, and lap leaders parsed from ``race-results.html``). The last task is
``sync_race_results`` (race results CSV plus loop-data HTML/CSV).

Downstream DAGs can depend on a future asset emitted from the last task if needed.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import Asset

from nascar.cup.scraper.backfill_assets import (
    BACKFILL_END_DATE,
    BACKFILL_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
    BACKFILL_RACING_REFERENCE_CUP_SCHEDULE_ASSET,
    BACKFILL_START_DATE,
)
from nascar.cup.scraper.dag_factories import register_racing_reference_race_week_dag
from nascar.cup.scraper.racing_reference.schedule import (
    NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET,
)

DAG_ID = "nascar.cup.scraper.racing_reference.race_week"

NASCAR_RACING_REFERENCE_CUP_RACE_WEEK_ASSET = Asset(
    uri="nascar://racing-reference/cup/race-week.csv",
    name=DAG_ID,
)

_BACKFILL_DOC = (
    "Historical Cup Racing Reference per-week HTML → CSV (2010–2025), triggered when "
    "the backfill RR schedule asset updates. Uses backfill-only assets.\n\n"
    + (__doc__ or "")
)

register_racing_reference_race_week_dag(
    dag_id=DAG_ID,
    schedule=[NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET],
    start_date=datetime(2025, 7, 11),
    end_date=None,
    catchup=False,
    outlet_asset=NASCAR_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
    doc_md=__doc__,
)

register_racing_reference_race_week_dag(
    dag_id=f"backfill.{DAG_ID}",
    schedule=[BACKFILL_RACING_REFERENCE_CUP_SCHEDULE_ASSET],
    start_date=BACKFILL_START_DATE,
    end_date=BACKFILL_END_DATE,
    catchup=True,
    outlet_asset=BACKFILL_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
    doc_md=_BACKFILL_DOC,
)
