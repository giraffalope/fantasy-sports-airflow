"""
Cup machine-learning DAG.

**Schedule:** hourly, aligned with ``nascar.cup.aggregate``. Waits for that DAG's
terminal ``aggregate_done`` sentinel, then:

1. ``build_ml_training_snapshots`` — materializes ``ml_training_snapshots`` + child
   rows for all finished weeks (``kind='train'``) and the **current week** only
   for predict (``kind='predict'``, via ``v_series_current_week``).
2. ``train_models_if_needed`` — per DraftKings target stat, retrains an XGBoost
   regressor when models do not yet cover the latest train week through
   ``v_series_before_current_week`` and ``v_series_current_week``.
3. ``predict_upcoming_week`` — scores the current week's ``kind='predict'``
   snapshot and writes ``ml_predictions`` rows with derived DFS-point breakdowns.
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

from include.nascar.ml.predict_task import predict_upcoming_week_task
from include.nascar.ml.train_task import train_models_if_needed_task
from include.nascar.ml.training_snapshot_task import build_ml_training_snapshots_task

DAG_ID = "nascar.cup.machine_learning"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 7, 11),
    schedule=CUP_SCRAPER_PROD_HOURLY_CRON,
    catchup=False,
    max_active_runs=1,
    default_args=CUP_DAG_DEFAULT_ARGS,
    doc_md=__doc__ or "",
) as dag:
    wait_aggregate_complete = ExternalTaskSensor(
        task_id="wait_aggregate_complete",
        external_dag_id="nascar.cup.aggregate",
        external_task_id="aggregate_done",
        mode="reschedule",
        poke_interval=CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
        timeout=CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
        check_existence=True,
    )

    build_ml_training_snapshots = PythonOperator(
        task_id="build_ml_training_snapshots",
        python_callable=build_ml_training_snapshots_task,
    )

    train_models_if_needed = PythonOperator(
        task_id="train_models_if_needed",
        python_callable=train_models_if_needed_task,
    )

    predict_upcoming_week = PythonOperator(
        task_id="predict_upcoming_week",
        python_callable=predict_upcoming_week_task,
    )

    (
        wait_aggregate_complete
        >> build_ml_training_snapshots
        >> train_models_if_needed
        >> predict_upcoming_week
    )
