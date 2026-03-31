"""
Per-week FRCS.pro Cup HTML → CSV sync, driven by ``schedule.csv``.

**Schedule:** runs when ``NASCAR_FRCS_CUP_SCHEDULE_ASSET`` updates — in practice each
time ``nascar.cup.scraper.frcs.schedule`` completes successfully (that DAG is hourly).
Layout matches other scrapers: ``…/frcs.pro/cup/<year>/<week>/``.
"""

from __future__ import annotations

import random
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset
from nascar.cup.scraper.frcs.schedule import NASCAR_FRCS_CUP_SCHEDULE_ASSET

from airflow_utils.wait_operator import WaitOperator
from include.nascar.data_sources.frcs_race_week import (
    cup_series_sync_draftkings,
    cup_series_sync_entrylists,
    cup_series_sync_fanduel,
    cup_series_sync_lineups,
    cup_series_sync_penalties,
    cup_series_sync_pit_stall_selections,
    cup_series_sync_pit_stops,
    cup_series_sync_practices,
    cup_series_sync_qualifyings,
    cup_series_sync_race_results,
    cup_series_sync_weeks,
)

DAG_ID = "nascar.cup.scraper.frcs.race_week"

NASCAR_FRCS_CUP_RACE_WEEK_ASSET = Asset(
    uri="nascar://frcs/cup/race-week.csv",
    name=DAG_ID,
)

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 7, 11),
    schedule=[NASCAR_FRCS_CUP_SCHEDULE_ASSET],
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    wait_random_time_task = WaitOperator(
        task_id="wait_random_time",
        seconds=random.randint(0, 5),
    )

    sync_weeks_task = PythonOperator(
        task_id="sync_weeks",
        python_callable=cup_series_sync_weeks,
    )

    sync_entrylists_task = PythonOperator(
        task_id="sync_entrylists",
        python_callable=cup_series_sync_entrylists,
    )

    sync_practices_task = PythonOperator(
        task_id="sync_practices",
        python_callable=cup_series_sync_practices,
    )

    sync_qualifyings_task = PythonOperator(
        task_id="sync_qualifyings",
        python_callable=cup_series_sync_qualifyings,
    )

    sync_lineups_task = PythonOperator(
        task_id="sync_lineups",
        python_callable=cup_series_sync_lineups,
    )

    sync_pit_stops_task = PythonOperator(
        task_id="sync_pit_stops",
        python_callable=cup_series_sync_pit_stops,
    )

    sync_draftkings_task = PythonOperator(
        task_id="sync_draftkings",
        python_callable=cup_series_sync_draftkings,
    )

    sync_fanduel_task = PythonOperator(
        task_id="sync_fanduel",
        python_callable=cup_series_sync_fanduel,
    )

    sync_pit_stall_selections_task = PythonOperator(
        task_id="sync_pit_stall_selections",
        python_callable=cup_series_sync_pit_stall_selections,
    )

    sync_penalties_task = PythonOperator(
        task_id="sync_penalties",
        python_callable=cup_series_sync_penalties,
    )

    sync_race_results_task = PythonOperator(
        task_id="sync_race_results",
        python_callable=cup_series_sync_race_results,
        outlets=[NASCAR_FRCS_CUP_RACE_WEEK_ASSET],
    )

    (
        wait_random_time_task
        >> sync_weeks_task
        >> sync_entrylists_task
        >> sync_practices_task
        >> sync_qualifyings_task
        >> sync_lineups_task
        >> sync_pit_stall_selections_task
        >> sync_pit_stops_task
        >> sync_penalties_task
        >> sync_draftkings_task
        >> sync_fanduel_task
        >> sync_race_results_task
    )
