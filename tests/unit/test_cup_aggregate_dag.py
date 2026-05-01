"""Unit tests for DAG ``nascar.cup.aggregate``."""

from __future__ import annotations

import nascar.cup.aggregate as aggregate_dag
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON
from nascar.cup.scraper.dag_factories import (
    CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
    CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
)


def test_dag_metadata() -> None:
    dag = aggregate_dag.dag
    assert dag.dag_id == "nascar.cup.aggregate"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON


def test_wait_sensor_targets_importer_cleanup_task() -> None:
    dag = aggregate_dag.dag
    wait_importer_complete = dag.get_task("wait_importer_complete")

    assert wait_importer_complete.external_dag_id == "nascar.cup.importer"
    assert wait_importer_complete.external_task_id == "cleanup_importer_staging_and_temp_tables"
    assert wait_importer_complete.mode == "reschedule"
    assert wait_importer_complete.poke_interval == CUP_EXTERNAL_SENSOR_POKE_INTERVAL.total_seconds()
    assert wait_importer_complete.timeout == CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT.total_seconds()


def test_task_order_wait_then_load() -> None:
    dag = aggregate_dag.dag
    wait_importer_complete = dag.get_task("wait_importer_complete")
    load_agg_week_results = dag.get_task("load_agg_week_results")

    assert set(dag.task_dict.keys()) == {"wait_importer_complete", "load_agg_week_results"}
    assert wait_importer_complete.downstream_task_ids == {load_agg_week_results.task_id}
    assert load_agg_week_results.downstream_task_ids == set()
