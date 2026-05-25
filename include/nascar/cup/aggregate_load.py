"""Helpers for ``nascar.cup.aggregate`` load tasks (week discovery + SQL operators).

``FINISHED_AGG_WEEKS_SQL`` — every Cup week with finished ``agg_week_results`` (ML snapshots).

``build_weeks_to_aggregate_sql`` — per-task week selection for wave-1 / wave-2 loaders:

- **Weeks in scope**: only weeks that already have rows in ``agg_week_results``
  (current week or historical before ``current_week``).
- **Historical**: include when the target ``agg_*`` table has no rows for that week.
- **Current week**: always refresh every run when base agg has rows.

``load_agg_week_results`` (proc 6019) uses the same view for ``current_week``.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from airflow_utils import MySQLTemplateFileOperatorFromContext
from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.enums import NascarSeries

_LOG = logging.getLogger(__name__)

FINISHED_AGG_WEEKS_SQL = """
SELECT DISTINCT s.series_id, w.id AS week_id
FROM nascar.agg_week_results awr
JOIN nascar.weeks w ON awr.week_id = w.id AND w.deleted_at IS NULL
JOIN nascar.seasons s ON w.season_id = s.id AND s.deleted_at IS NULL
WHERE awr.deleted_at IS NULL
  AND awr.r_finish_position IS NOT NULL
  AND s.series_id = %s
ORDER BY w.id
"""

# Backward-compatible alias (ML and legacy callers).
WEEKS_TO_LOAD_SQL = FINISHED_AGG_WEEKS_SQL

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
    "load_agg_week_prev_track_manufacturers_results",
    "load_agg_week_prev_track_types_manufacturers_results",
    "load_agg_week_prev_track_cg_cars_drivers_results",
    "load_agg_week_prev_track_cgm_cars_drivers_results",
    "load_agg_week_prev_track_cg_cars_results",
    "load_agg_week_prev_track_cgm_cars_results",
    "load_agg_week_prev_track_cg_drivers_results",
    "load_agg_week_prev_track_cgm_drivers_results",
    "load_agg_week_prev_track_cg_manufacturers_results",
    "load_agg_week_prev_track_cgm_manufacturers_results",
    "load_agg_week_prev_track_cg_teams_results",
    "load_agg_week_prev_track_cgm_teams_results",
    "load_agg_week_prev_track_types_cg_cars_drivers_results",
    "load_agg_week_prev_track_types_cgm_cars_drivers_results",
    "load_agg_week_prev_track_types_cg_cars_results",
    "load_agg_week_prev_track_types_cgm_cars_results",
    "load_agg_week_prev_track_types_cg_drivers_results",
    "load_agg_week_prev_track_types_cgm_drivers_results",
    "load_agg_week_prev_track_types_cg_manufacturers_results",
    "load_agg_week_prev_track_types_cgm_manufacturers_results",
    "load_agg_week_prev_track_types_cg_teams_results",
    "load_agg_week_prev_track_types_cgm_teams_results",
)

ALL_AGG_LOAD_TASKS: tuple[str, ...] = WAVE1_LOAD_TASKS + WAVE2_PREV_TRACK_LOAD_TASKS

AGG_TABLE_BY_TASK: dict[str, str] = {
    task_id: task_id.removeprefix("load_") for task_id in ALL_AGG_LOAD_TASKS
}


def build_weeks_to_aggregate_sql(agg_table: str) -> str:
    """Return week-discovery SQL for one wave loader's target ``agg_*`` table."""
    if agg_table not in AGG_TABLE_BY_TASK.values():
        raise ValueError(f"unknown aggregate table: {agg_table}")
    return f"""
WITH current_week AS (
    SELECT week_id, series_id
    FROM nascar.v_series_current_week
    WHERE series_id = %s
),
weeks_in_scope AS (
    SELECT DISTINCT awr.series_id, awr.week_id
    FROM nascar.agg_week_results awr
    JOIN current_week cw ON cw.series_id = awr.series_id
    WHERE awr.deleted_at IS NULL
      AND awr.series_id = %s
      AND (
          awr.week_id = cw.week_id
          OR awr.week_id < cw.week_id
      )
)
SELECT DISTINCT wr.series_id, wr.week_id
FROM weeks_in_scope wr
JOIN current_week cw ON cw.week_id IS NOT NULL
WHERE wr.week_id = cw.week_id
   OR (
        wr.week_id < cw.week_id
        AND NOT EXISTS (
            SELECT 1
            FROM nascar.{agg_table} t
            WHERE t.series_id = wr.series_id
              AND t.week_id = wr.week_id
              AND t.deleted_at IS NULL
        )
   )
ORDER BY wr.week_id
"""


# Default SQL for tests and backward-compatible imports (teams wave-1 table).
WEEKS_TO_AGGREGATE_SQL = build_weeks_to_aggregate_sql("agg_week_teams_results")


def _rebuild_all_weeks_enabled() -> bool:
    raw = os.environ.get("FS_AGG_REBUILD_ALL_WEEKS", "0")
    return raw.strip().lower() in ("1", "true", "yes", "on")


def aggregate_load_parameters(
    context: Mapping[str, Any],
    *,
    task_id: str,
) -> dict[str, Any]:
    """Weeks needing reload for one wave loader after ``load_agg_week_results``."""
    series_id = NascarSeries.CUP.series_id
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    if _rebuild_all_weeks_enabled():
        sql = FINISHED_AGG_WEEKS_SQL
        parameters: tuple[int, ...] = (series_id,)
        mode = "rebuild_all"
    else:
        agg_table = AGG_TABLE_BY_TASK[task_id]
        sql = build_weeks_to_aggregate_sql(agg_table)
        parameters = (series_id, series_id)
        mode = "incremental"
    rows = hook.get_records(sql, parameters=parameters)
    weeks = [{"week_id": int(week_id)} for _series_id, week_id in rows]
    _LOG.info(
        "aggregate_load_parameters task_id=%s series_id=%s mode=%s weeks_to_load=%s",
        task_id,
        series_id,
        mode,
        [w["week_id"] for w in weeks],
    )
    return {"series_id": series_id, "weeks": weeks}


def aggregate_base_load_parameters(_context: Mapping[str, Any]) -> dict[str, Any]:
    """Series-scoping params for the no-weeks-list ``load_agg_week_results`` proc."""
    return {"series_id": NascarSeries.CUP.series_id}


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
        parameters_factory=lambda ctx, tid=task_id: aggregate_load_parameters(
            ctx, task_id=tid
        ),
    )
