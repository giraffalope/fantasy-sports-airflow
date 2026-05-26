"""
Cup ML Monte Carlo race-simulation DAG.

**Schedule:** hourly, aligned with ``nascar.cup.machine_learning.models``. Waits
for ``predict_upcoming_week`` on the production ML models DAG, simulates ≥ 1,000
DraftKings-point outcomes per driver from calibrated prediction uncertainty,
and persists batch + per-driver summaries to MariaDB for the lineup generator
and downstream BI.
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

from include.nascar.ml.monte_carlo_task import (
    run_monte_carlo_simulation_task,
    validate_simulation_batch_task,
)

DAG_ID = "nascar.cup.machine_learning.models.monte_carlo"
ML_DAG_ID = "nascar.cup.machine_learning.models"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 7, 11),
    schedule=CUP_SCRAPER_PROD_HOURLY_CRON,
    catchup=False,
    max_active_runs=1,
    default_args=CUP_DAG_DEFAULT_ARGS,
    doc_md=__doc__ or "",
) as dag:
    wait_ml_predict_complete = ExternalTaskSensor(
        task_id="wait_ml_predict_complete",
        external_dag_id=ML_DAG_ID,
        external_task_id="predict_upcoming_week",
        mode="reschedule",
        poke_interval=CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
        timeout=CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
        check_existence=True,
    )

    run_monte_carlo_simulation = PythonOperator(
        task_id="run_monte_carlo_simulation",
        python_callable=run_monte_carlo_simulation_task,
    )

    validate_simulation_batch = PythonOperator(
        task_id="validate_simulation_batch",
        python_callable=validate_simulation_batch_task,
    )

    wait_ml_predict_complete >> run_monte_carlo_simulation >> validate_simulation_batch
