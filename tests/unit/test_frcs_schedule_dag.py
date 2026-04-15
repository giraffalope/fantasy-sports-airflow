"""Unit tests for DAG ``nascar.cup.scraper.frcs.schedule``."""

from __future__ import annotations

import nascar.cup.scraper.frcs.schedule as schedule_dag
from nascar.cup.scraper.backfill_assets import (
    BACKFILL_YEARLY_SCHEDULE,
    CUP_SCRAPER_PROD_HOURLY_CRON,
)

from include.nascar.data_sources.frcs import (
    cup_series_convert_year_schedule_html_to_csv,
    cup_series_download_year_schedule_html,
)


def test_dag_metadata() -> None:
    dag = schedule_dag.dag
    assert dag.dag_id == "nascar.cup.scraper.frcs.schedule"
    assert str(dag.schedule) == CUP_SCRAPER_PROD_HOURLY_CRON
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_backfill_dag_metadata() -> None:
    dag = schedule_dag.backfill_dag
    assert dag.dag_id == "backfill.nascar.cup.scraper.frcs.schedule"
    assert str(dag.schedule) == BACKFILL_YEARLY_SCHEDULE
    assert dag.catchup is True


def test_task_ids_and_count() -> None:
    dag = schedule_dag.dag
    assert set(dag.task_dict.keys()) == {
        "wait_random_time",
        "download_year_schedule_html",
        "convert_year_schedule_html_to_csv",
    }
    assert len(dag.tasks) == 3


def test_linear_dependencies() -> None:
    dag = schedule_dag.dag
    wait = dag.get_task("wait_random_time")
    download = dag.get_task("download_year_schedule_html")
    convert = dag.get_task("convert_year_schedule_html_to_csv")

    assert wait.downstream_task_ids == {download.task_id}
    assert download.downstream_task_ids == {convert.task_id}
    assert convert.downstream_task_ids == set()

    assert wait.upstream_task_ids == set()
    assert download.upstream_task_ids == {wait.task_id}
    assert convert.upstream_task_ids == {download.task_id}


def test_convert_task_outlets_emit_schedule_asset() -> None:
    dag = schedule_dag.dag
    convert = dag.get_task("convert_year_schedule_html_to_csv")
    assert convert.outlets
    assert len(convert.outlets) == 1
    outlet = convert.outlets[0]
    assert outlet.uri == schedule_dag.NASCAR_FRCS_CUP_SCHEDULE_ASSET.uri
    assert outlet.name == schedule_dag.NASCAR_FRCS_CUP_SCHEDULE_ASSET.name


def test_module_asset_constant() -> None:
    asset = schedule_dag.NASCAR_FRCS_CUP_SCHEDULE_ASSET
    assert asset.uri == "nascar://frcs/cup/schedule.csv"
    assert asset.name == "nascar.cup.scraper.frcs.schedule"


def test_download_and_convert_callables_match_dag_operators() -> None:
    dag = schedule_dag.dag
    dl = dag.get_task("download_year_schedule_html")
    cv = dag.get_task("convert_year_schedule_html_to_csv")
    assert dl.python_callable is cup_series_download_year_schedule_html
    assert cv.python_callable is cup_series_convert_year_schedule_html_to_csv
