"""
Cup DraftKings DFS lineup generator DAG.

**Schedule:** hourly, aligned with ML. Waits for ``validate_simulation_batch``
on ``nascar.cup.machine_learning.models.monte_carlo`` (which itself waits on
``predict_upcoming_week``), then builds top-K ILP lineups per strategy when
Monte Carlo summaries and DraftKings salaries exist for the upcoming race.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG
from nascar.cup.dag_defaults import CUP_DAG_DEFAULT_ARGS
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON
from nascar.cup.scraper.dag_factories import (
    CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
    CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
)

from include.nascar.dfs.lineup_generator_task import generate_draftkings_lineups_task

DAG_ID = "nascar.cup.dfs.lineup_generator"
SALARY_IMPORTER_DAG_ID = "nascar.cup.dfs.salary_importer"
ML_MONTE_CARLO_DAG_ID = "nascar.cup.machine_learning.models.monte_carlo"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 7, 11),
    schedule=CUP_SCRAPER_PROD_HOURLY_CRON,
    catchup=False,
    max_active_runs=1,
    default_args=CUP_DAG_DEFAULT_ARGS,
    doc_md=__doc__ or "",
) as dag:
    wait_dk_salaries_complete = ExternalTaskSensor(
        task_id="wait_dk_salaries_complete",
        external_dag_id=SALARY_IMPORTER_DAG_ID,
        external_task_id="import_draftkings_salaries",
        mode="reschedule",
        poke_interval=CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
        timeout=CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
        check_existence=True,
    )

    wait_mc_complete = ExternalTaskSensor(
        task_id="wait_mc_complete",
        external_dag_id=ML_MONTE_CARLO_DAG_ID,
        external_task_id="validate_simulation_batch",
        mode="reschedule",
        poke_interval=CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
        timeout=CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
        check_existence=True,
    )

    generate_draftkings_lineups = PythonOperator(
        task_id="generate_draftkings_lineups",
        python_callable=generate_draftkings_lineups_task,
    )

    [wait_mc_complete, wait_dk_salaries_complete] >> generate_draftkings_lineups
