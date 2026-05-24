"""Unit tests for DAG ``nascar.cup.dfs.salary_importer``."""

from __future__ import annotations

import nascar.cup.dfs.salary_importer as salary_dag
from nascar.cup.scraper.backfill_assets import CUP_SCRAPER_PROD_HOURLY_CRON


def test_dag_metadata() -> None:
    dag = salary_dag.dag
    assert dag.dag_id == "nascar.cup.dfs.salary_importer"
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON
    assert dag.catchup is False


def test_task_chain() -> None:
    dag = salary_dag.dag
    ensure = dag.get_task("ensure_draftkings_week_directory")
    import_task = dag.get_task("import_draftkings_salaries")
    assert ensure.downstream_task_ids == {import_task.task_id}
    assert import_task.upstream_task_ids == {ensure.task_id}
