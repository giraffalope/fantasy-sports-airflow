"""
Cup DraftKings salary importer DAG.

**Schedule:** hourly (``CUP_SCRAPER_PROD_HOURLY_CRON``). Ensures the current-week
DFS folder exists on the mounted volume, then imports any ``DKSalaries.csv`` files
found under ``draftkings/nascar/cup/{year}/{week}/``.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from nascar.cup.dag_defaults import CUP_DAG_DEFAULT_ARGS
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON

from include.nascar.dfs.salary_import_task import (
    ensure_draftkings_week_directory_task,
    import_draftkings_salaries_task,
)

DAG_ID = "nascar.cup.dfs.salary_importer"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 7, 11),
    schedule=CUP_SCRAPER_PROD_HOURLY_CRON,
    catchup=False,
    max_active_runs=1,
    default_args=CUP_DAG_DEFAULT_ARGS,
    doc_md=__doc__ or "",
) as dag:
    ensure_draftkings_week_directory = PythonOperator(
        task_id="ensure_draftkings_week_directory",
        python_callable=ensure_draftkings_week_directory_task,
    )

    import_draftkings_salaries = PythonOperator(
        task_id="import_draftkings_salaries",
        python_callable=import_draftkings_salaries_task,
    )

    ensure_draftkings_week_directory >> import_draftkings_salaries
