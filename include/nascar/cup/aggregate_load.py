"""Helpers for ``nascar.cup.aggregate`` load tasks (week discovery + SQL operators)."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from airflow_utils import MySQLTemplateFileOperatorFromContext
from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.enums import NascarSeries

_LOG = logging.getLogger(__name__)

WEEKS_TO_LOAD_SQL = """
SELECT DISTINCT s.series_id, w.id AS week_id
FROM nascar.agg_week_results awr
JOIN nascar.weeks w ON awr.week_id = w.id AND w.deleted_at IS NULL
JOIN nascar.seasons s ON w.season_id = s.id AND s.deleted_at IS NULL
WHERE awr.deleted_at IS NULL
  AND awr.r_finish_position IS NOT NULL
  AND s.series_id = %s
ORDER BY w.id
"""

WAVE1_LOAD_TASKS: tuple[str, ...] = (
    "load_agg_week_manufacturers_results",
    "load_agg_week_teams_results",
)

WAVE2_PREV_TRACK_LOAD_TASKS: tuple[str, ...] = (
    "load_agg_week_prev_track_car_generation_models_results",
    "load_agg_week_prev_track_types_car_generation_models_results",
    "load_agg_week_prev_track_car_generations_results",
    "load_agg_week_prev_track_types_car_generations_results",
    "load_agg_week_prev_track_cars_drivers_results",
    "load_agg_week_prev_track_types_cars_drivers_results",
    "load_agg_week_prev_track_cars_results",
    "load_agg_week_prev_track_types_cars_results",
    "load_agg_week_prev_track_drivers_results",
    "load_agg_week_prev_track_types_drivers_results",
    "load_agg_week_prev_track_teams_results",
    "load_agg_week_prev_track_types_teams_results",
)


def aggregate_load_parameters(context: Mapping[str, Any]) -> dict[str, Any]:
    """Query weeks with finished agg_week_results for Cup; used by Jinja SQL templates."""
    series_id = NascarSeries.CUP.series_id
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    rows = hook.get_records(WEEKS_TO_LOAD_SQL, parameters=(series_id,))
    weeks = [{"week_id": int(week_id)} for _series_id, week_id in rows]
    _LOG.info(
        "aggregate_load_parameters series_id=%s weeks_to_load=%s",
        series_id,
        [w["week_id"] for w in weeks],
    )
    return {"series_id": series_id, "weeks": weeks}


def make_aggregate_load_operator(
    task_id: str,
    *,
    sql_root: Path,
    mysql_conn_id: str = "nascar_conn",
) -> MySQLTemplateFileOperatorFromContext:
    return MySQLTemplateFileOperatorFromContext(
        task_id=task_id,
        sql_file_path=sql_root / "nascar.cup.aggregate" / f"{task_id}.sql",
        mysql_conn_id=mysql_conn_id,
        parameters_factory=aggregate_load_parameters,
    )
