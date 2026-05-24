"""
Top-level Cup aggregate DAG.

**Schedule:** hourly (see ``CUP_SCRAPER_PROD_HOURLY_CRON``), aligned with
``nascar.cup.importer``.

Each hourly run calls ``load_agg_week_results`` (backfill missing base rows through
the current race week; refresh only the current week when importer data changes),
then runs wave-1 / wave-2 loaders in two modes: **backfill** all weeks with
``race_results`` through ``current_week`` that lack aggregates when history is
incomplete; **steady-state** only ``current_week`` when history is complete and
importer data may still be changing. Wave 1 (manufacturers,
teams) runs in parallel after the base load; wave 2 (prior-track entity,
manufacturer, and CG/CGM aggregate tables, 34 parallel tasks) runs after wave 1
completes; the terminal ``aggregate_done`` sentinel fans-in from every wave 2 task
so downstream DAGs (such as ``nascar.cup.machine_learning``) can wait on a single
stable task id.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG
from include.nascar.cup.aggregate_load import (
    WAVE1_LOAD_TASKS,
    WAVE2_PREV_TRACK_LOAD_TASKS,
    aggregate_base_load_parameters,
    make_aggregate_load_operator,
)
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

    sql_root = _sql_root()

    load_agg_week_results = MySQLTemplateFileOperatorFromContext(
        task_id="load_agg_week_results",
        sql_file_path=sql_root / "nascar.cup.aggregate" / "load_agg_week_results.sql",
        mysql_conn_id="nascar_conn",
        parameters_factory=aggregate_base_load_parameters,
    )

    wave1_tasks = [
        make_aggregate_load_operator(task_id, sql_root=sql_root)
        for task_id in WAVE1_LOAD_TASKS
    ]
    wave2_tasks = [
        make_aggregate_load_operator(task_id, sql_root=sql_root)
        for task_id in WAVE2_PREV_TRACK_LOAD_TASKS
    ]

    aggregate_done = EmptyOperator(task_id="aggregate_done")

    wait_importer_complete >> load_agg_week_results >> wave1_tasks
    for wave1_task in wave1_tasks:
        wave1_task >> wave2_tasks
    for wave2_task in wave2_tasks:
        wave2_task >> aggregate_done
