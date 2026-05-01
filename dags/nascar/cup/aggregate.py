"""
Top-level Cup aggregate DAG.

**Schedule:** hourly (see ``CUP_SCRAPER_PROD_HOURLY_CRON``), aligned with
``nascar.cup.importer``.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON
from nascar.cup.scraper.dag_factories import (
    CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
    CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
)

from airflow_utils import MySQLTemplateFileOperatorFromContext

DAG_ID = "nascar.cup.aggregate"


def _sql_root() -> Path:
    # ``aggregate.py`` lives at ``dags/nascar/cup/`` -> ``parents[2]`` is ``dags/``.
    return Path(__file__).resolve().parents[2] / "sql"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 7, 11),
    schedule=CUP_SCRAPER_PROD_HOURLY_CRON,
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__ or "",
) as dag:
    wait_importer_complete = ExternalTaskSensor(
        task_id="wait_importer_complete",
        external_dag_id="nascar.cup.importer",
        external_task_id="cleanup_importer_staging_and_temp_tables",
        mode="reschedule",
        poke_interval=CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
        timeout=CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
        check_existence=True,
    )

    load_agg_week_results = MySQLTemplateFileOperatorFromContext(
        task_id="load_agg_week_results",
        sql_file_path=_sql_root() / "nascar.cup.aggregate" / "load_agg_week_results.sql",
        mysql_conn_id="nascar_conn",
        parameters_factory=lambda _context: {},
    )

    wait_importer_complete >> load_agg_week_results
