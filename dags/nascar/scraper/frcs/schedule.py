"""
Hourly FRCS.pro Cup schedule scrape (HTML → CSV).

Uses task helpers in ``include.nascar.data_sources.frcs`` with the same
``storage_path_series_year`` layout as other nascar scraper sources under
``…/<host>/cup/<year>/``.

Downstream DAGs should depend on the emitted **Asset** after each successful run::

    from airflow.sdk import DAG
    from nascar.scraper.frcs.schedule import NASCAR_FRCS_CUP_SCHEDULE_ASSET

    with DAG(
        dag_id=\"my_downstream_dag\",
        schedule=[NASCAR_FRCS_CUP_SCHEDULE_ASSET],
        ...
    ):
        ...
"""

from __future__ import annotations

import random
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

from airflow_utils.wait_operator import WaitOperator
from include.nascar.data_sources.frcs import (
    cup_series_convert_year_schedule_html_to_csv,
    cup_series_download_year_schedule_html,
)

NASCAR_FRCS_CUP_SCHEDULE_ASSET = Asset(
    uri="nascar://frcs/cup/schedule.csv",
    name="nascar_frcs_cup_schedule",
)

with DAG(
    dag_id="nascar_scraper_frcs_schedule",
    start_date=datetime(2025, 7, 11),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    wait_random_time_task = WaitOperator(
        task_id="wait_random_time",
        seconds=random.randint(0, 5),
    )

    cup_series_download_year_schedule_html_task = PythonOperator(
        task_id="cup_series_download_year_schedule_html",
        python_callable=cup_series_download_year_schedule_html,
    )

    cup_series_convert_year_schedule_html_to_csv_task = PythonOperator(
        task_id="cup_series_convert_year_schedule_html_to_csv",
        python_callable=cup_series_convert_year_schedule_html_to_csv,
        outlets=[NASCAR_FRCS_CUP_SCHEDULE_ASSET],
    )

    (
        wait_random_time_task
        >> cup_series_download_year_schedule_html_task
        >> cup_series_convert_year_schedule_html_to_csv_task
    )
