"""
Shared Cup scraper DAG construction (prod + backfill).

Call the ``register_*`` functions from small per-DAG modules so task graphs stay
single-sourced without duplicating operator chains.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

from airflow_utils.wait_operator import WaitOperator
from include.nascar.data_sources.frcs import (
    cup_series_convert_year_schedule_html_to_csv as frcs_convert_schedule,
    cup_series_download_year_schedule_html as frcs_download_schedule,
)
from include.nascar.data_sources.frcs_race_week import (
    cup_series_sync_draftkings,
    cup_series_sync_entrylists as frcs_sync_entrylists,
    cup_series_sync_fanduel,
    cup_series_sync_lineups,
    cup_series_sync_penalties,
    cup_series_sync_pit_stall_selections,
    cup_series_sync_pit_stops,
    cup_series_sync_practices as frcs_sync_practices,
    cup_series_sync_qualifyings as frcs_sync_qualifyings,
    cup_series_sync_race_results as frcs_sync_race_results,
    cup_series_sync_weeks as frcs_sync_weeks,
)
from include.nascar.data_sources.racing_reference import (
    cup_series_convert_year_schedule_html_to_csv as rr_convert_schedule,
    cup_series_download_year_schedule_html as rr_download_schedule,
)
from include.nascar.data_sources.racing_reference_race_week import (
    cup_series_sync_caution_flags,
    cup_series_sync_entrylists as rr_sync_entrylists,
    cup_series_sync_penalties_and_lap_leaders,
    cup_series_sync_practices as rr_sync_practices,
    cup_series_sync_prerace_penalties,
    cup_series_sync_qualifyings as rr_sync_qualifyings,
    cup_series_sync_race_results as rr_sync_race_results,
    cup_series_sync_weeks as rr_sync_weeks,
)


def register_frcs_schedule_dag(
    *,
    dag_id: str,
    schedule: str | None,
    start_date: datetime,
    end_date: datetime | None,
    catchup: bool,
    outlet_asset: Asset,
    doc_md: str,
) -> None:
    with DAG(
        dag_id=dag_id,
        start_date=start_date,
        end_date=end_date,
        schedule=schedule,
        catchup=catchup,
        max_active_runs=1,
        doc_md=doc_md,
    ):
        wait_random_time_task = WaitOperator(
            task_id="wait_random_time",
            seconds=random.randint(0, 5),
        )
        download_year_schedule_html_task = PythonOperator(
            task_id="download_year_schedule_html",
            python_callable=frcs_download_schedule,
        )
        convert_year_schedule_html_to_csv_task = PythonOperator(
            task_id="convert_year_schedule_html_to_csv",
            python_callable=frcs_convert_schedule,
            outlets=[outlet_asset],
        )
        (
            wait_random_time_task
            >> download_year_schedule_html_task
            >> convert_year_schedule_html_to_csv_task
        )


def register_racing_reference_schedule_dag(
    *,
    dag_id: str,
    schedule: str | None,
    start_date: datetime,
    end_date: datetime | None,
    catchup: bool,
    outlet_asset: Asset,
    doc_md: str,
) -> None:
    with DAG(
        dag_id=dag_id,
        start_date=start_date,
        end_date=end_date,
        schedule=schedule,
        catchup=catchup,
        max_active_runs=1,
        doc_md=doc_md,
    ):
        wait_random_time_task = WaitOperator(
            task_id="wait_random_time",
            seconds=random.randint(0, 5),
        )
        download_year_schedule_html_task = PythonOperator(
            task_id="download_year_schedule_html",
            python_callable=rr_download_schedule,
        )
        convert_year_schedule_html_to_csv_task = PythonOperator(
            task_id="convert_year_schedule_html_to_csv",
            python_callable=rr_convert_schedule,
            outlets=[outlet_asset],
        )
        (
            wait_random_time_task
            >> download_year_schedule_html_task
            >> convert_year_schedule_html_to_csv_task
        )


def register_frcs_race_week_dag(
    *,
    dag_id: str,
    schedule: str | list[Asset] | None,
    start_date: datetime,
    end_date: datetime | None,
    catchup: bool,
    outlet_asset: Asset,
    doc_md: str,
) -> None:
    with DAG(
        dag_id=dag_id,
        start_date=start_date,
        end_date=end_date,
        schedule=schedule,
        catchup=catchup,
        max_active_runs=1,
        doc_md=doc_md,
    ):
        wait_random_time_task = WaitOperator(
            task_id="wait_random_time",
            seconds=random.randint(0, 5),
        )
        sync_weeks_task = PythonOperator(
            task_id="sync_weeks",
            python_callable=frcs_sync_weeks,
        )
        sync_entrylists_task = PythonOperator(
            task_id="sync_entrylists",
            python_callable=frcs_sync_entrylists,
        )
        sync_practices_task = PythonOperator(
            task_id="sync_practices",
            python_callable=frcs_sync_practices,
        )
        sync_qualifyings_task = PythonOperator(
            task_id="sync_qualifyings",
            python_callable=frcs_sync_qualifyings,
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
            python_callable=frcs_sync_race_results,
            outlets=[outlet_asset],
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


def register_racing_reference_race_week_dag(
    *,
    dag_id: str,
    schedule: str | Sequence[Asset] | None,
    start_date: datetime,
    end_date: datetime | None,
    catchup: bool,
    outlet_asset: Asset,
    doc_md: str,
) -> None:
    sched: str | list[Asset] | None
    if isinstance(schedule, (list, tuple)):
        sched = list(schedule)
    else:
        sched = schedule

    with DAG(
        dag_id=dag_id,
        start_date=start_date,
        end_date=end_date,
        schedule=sched,
        catchup=catchup,
        max_active_runs=1,
        doc_md=doc_md,
    ):
        wait_random_time_task = WaitOperator(
            task_id="wait_random_time",
            seconds=random.randint(0, 5),
        )
        sync_weeks_task = PythonOperator(
            task_id="sync_weeks",
            python_callable=rr_sync_weeks,
        )
        sync_entrylists_task = PythonOperator(
            task_id="sync_entrylists",
            python_callable=rr_sync_entrylists,
        )
        sync_practices_task = PythonOperator(
            task_id="sync_practices",
            python_callable=rr_sync_practices,
        )
        sync_qualifyings_task = PythonOperator(
            task_id="sync_qualifyings",
            python_callable=rr_sync_qualifyings,
        )
        sync_prerace_penalties_task = PythonOperator(
            task_id="sync_prerace_penalties",
            python_callable=cup_series_sync_prerace_penalties,
        )
        sync_caution_flags_task = PythonOperator(
            task_id="sync_caution_flags",
            python_callable=cup_series_sync_caution_flags,
        )
        sync_penalties_and_lap_leaders_task = PythonOperator(
            task_id="sync_penalties_and_lap_leaders",
            python_callable=cup_series_sync_penalties_and_lap_leaders,
        )
        sync_race_results_task = PythonOperator(
            task_id="sync_race_results",
            python_callable=rr_sync_race_results,
            outlets=[outlet_asset],
        )
        (
            wait_random_time_task
            >> sync_weeks_task
            >> sync_entrylists_task
            >> sync_practices_task
            >> sync_qualifyings_task
            >> sync_prerace_penalties_task
            >> sync_caution_flags_task
            >> sync_penalties_and_lap_leaders_task
            >> sync_race_results_task
        )
