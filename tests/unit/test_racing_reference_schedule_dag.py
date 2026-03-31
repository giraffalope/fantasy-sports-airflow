"""Unit tests for DAG ``nascar.cup.scraper.racing_reference.schedule``."""

from __future__ import annotations

import nascar.cup.scraper.racing_reference.schedule as schedule_dag


def test_dag_metadata() -> None:
    dag = schedule_dag.dag
    assert dag.dag_id == "nascar.cup.scraper.racing_reference.schedule"
    assert str(dag.schedule) == "0 * * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_task_ids_and_count() -> None:
    dag = schedule_dag.dag
    assert set(dag.task_dict.keys()) == {
        "wait_random_time",
        "download_year_schedule_html",
        "convert_year_schedule_html_to_csv",
    }
    assert len(dag.tasks) == 3


def test_schedule_asset_name_matches_dag_id() -> None:
    asset = schedule_dag.NASCAR_RACING_REFERENCE_CUP_SCHEDULE_ASSET
    assert asset.uri == "nascar://racing-reference/cup/schedule.csv"
    assert asset.name == "nascar.cup.scraper.racing_reference.schedule"
