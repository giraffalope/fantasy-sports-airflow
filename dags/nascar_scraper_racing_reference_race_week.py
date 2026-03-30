"""
Per-week Racing Reference Cup HTML → CSV sync, driven by ``schedule.csv``.

**Schedule:** this DAG is triggered when ``NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET``
updates — in practice, each time ``nascar_scraper_racing_reference_schedule`` finishes
successfully (that DAG runs hourly). A short random wait task runs first, then tasks
download missing HTML (only when files are absent) and emit CSVs alongside the main
``nascar_scraper`` layout under ``…/cup/<year>/<week>/`` (including prerace penalties,
caution flags, and lap leaders parsed from ``race-results.html``). The last task is
``cup_series_sync_race_results`` (race results CSV plus loop-data HTML/CSV).

Downstream DAGs can depend on a future asset emitted from the last task if needed.
"""

from __future__ import annotations

import random
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from nascar_scraper_racing_reference_schedule import (
    NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET,
)

from airflow_utils.wait_operator import WaitOperator
from include.nascar.data_sources.racing_reference_race_week import (
    cup_series_sync_caution_flags,
    cup_series_sync_entrylists,
    cup_series_sync_penalties_and_lap_leaders,
    cup_series_sync_practices,
    cup_series_sync_prerace_penalties,
    cup_series_sync_qualifyings,
    cup_series_sync_race_results,
    cup_series_sync_weeks,
)

with DAG(
    dag_id="nascar_scraper_racing_reference_race_week",
    start_date=datetime(2025, 7, 11),
    schedule=[NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET],
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    wait_random_time_task = WaitOperator(
        task_id="wait_random_time",
        seconds=random.randint(0, 5),
    )

    cup_series_sync_weeks_task = PythonOperator(
        task_id="cup_series_sync_weeks",
        python_callable=cup_series_sync_weeks,
    )

    cup_series_sync_entrylists_task = PythonOperator(
        task_id="cup_series_sync_entrylists",
        python_callable=cup_series_sync_entrylists,
    )

    cup_series_sync_practices_task = PythonOperator(
        task_id="cup_series_sync_practices",
        python_callable=cup_series_sync_practices,
    )

    cup_series_sync_qualifyings_task = PythonOperator(
        task_id="cup_series_sync_qualifyings",
        python_callable=cup_series_sync_qualifyings,
    )

    cup_series_sync_prerace_penalties_task = PythonOperator(
        task_id="cup_series_sync_prerace_penalties",
        python_callable=cup_series_sync_prerace_penalties,
    )

    cup_series_sync_caution_flags_task = PythonOperator(
        task_id="cup_series_sync_caution_flags",
        python_callable=cup_series_sync_caution_flags,
    )

    cup_series_sync_penalties_and_lap_leaders_task = PythonOperator(
        task_id="cup_series_sync_penalties_and_lap_leaders",
        python_callable=cup_series_sync_penalties_and_lap_leaders,
    )

    cup_series_sync_race_results_task = PythonOperator(
        task_id="cup_series_sync_race_results",
        python_callable=cup_series_sync_race_results,
    )

    (
        wait_random_time_task
        >> cup_series_sync_weeks_task
        >> cup_series_sync_entrylists_task
        >> cup_series_sync_practices_task
        >> cup_series_sync_qualifyings_task
        >> cup_series_sync_prerace_penalties_task
        >> cup_series_sync_caution_flags_task
        >> cup_series_sync_penalties_and_lap_leaders_task
        >> cup_series_sync_race_results_task
    )
