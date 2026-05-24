"""Helpers for ``nascar.cup.aggregate`` load tasks (week discovery + SQL operators).

``FINISHED_AGG_WEEKS_SQL`` — every Cup week with finished ``agg_week_results`` (ML snapshots).

``WEEKS_TO_AGGREGATE_SQL`` — two-mode week selection for wave-1 / wave-2 loaders:

- **Backfill mode** (history before ``current_week`` not fully aggregated): every week
  with ``race_results`` through ``current_week`` that lacks base or derived aggregates.
- **Steady-state mode** (history complete): only ``current_week`` when derived rows are
  missing or base ``agg_week_results`` is newer than downstream.

``load_agg_week_results`` (proc 6019) uses the same current-week rules for base rows.
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

WEEKS_TO_AGGREGATE_SQL = """
WITH current_week AS (
    SELECT MAX(w.id) AS week_id
    FROM nascar.race_results rr
    JOIN nascar.races r ON rr.race_id = r.id AND r.deleted_at IS NULL
    JOIN nascar.weeks w ON r.week_id = w.id AND w.deleted_at IS NULL
    JOIN nascar.seasons s ON w.season_id = s.id AND s.deleted_at IS NULL
    WHERE rr.deleted_at IS NULL
      AND s.series_id = %s
),
weeks_with_race AS (
    SELECT DISTINCT s.series_id, w.id AS week_id
    FROM nascar.race_results rr
    JOIN nascar.races r ON rr.race_id = r.id AND r.deleted_at IS NULL
    JOIN nascar.weeks w ON r.week_id = w.id AND w.deleted_at IS NULL
    JOIN nascar.seasons s ON w.season_id = s.id AND s.deleted_at IS NULL
    JOIN current_week cw ON cw.week_id IS NOT NULL
    WHERE rr.deleted_at IS NULL
      AND s.series_id = %s
      AND w.id <= cw.week_id
),
week_completeness AS (
    SELECT wr.series_id,
           wr.week_id,
           CASE
               WHEN EXISTS (
                   SELECT 1
                   FROM nascar.agg_week_results awr
                   WHERE awr.series_id = wr.series_id
                     AND awr.week_id = wr.week_id
                     AND awr.deleted_at IS NULL
               )
               AND EXISTS (
                   SELECT 1
                   FROM nascar.agg_week_teams_results t
                   WHERE t.series_id = wr.series_id
                     AND t.week_id = wr.week_id
                     AND t.deleted_at IS NULL
               )
               AND EXISTS (
                   SELECT 1
                   FROM nascar.agg_week_manufacturers_results m
                   WHERE m.series_id = wr.series_id
                     AND m.week_id = wr.week_id
                     AND m.deleted_at IS NULL
               )
               AND EXISTS (
                   SELECT 1
                   FROM nascar.agg_week_prev_track_cars_drivers_results p
                   WHERE p.series_id = wr.series_id
                     AND p.week_id = wr.week_id
                     AND p.deleted_at IS NULL
               )
               THEN 1
               ELSE 0
           END AS is_complete
    FROM weeks_with_race wr
),
history_coverage_complete AS (
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM week_completeness wc
            JOIN current_week cw ON cw.week_id IS NOT NULL
            WHERE wc.week_id < cw.week_id
              AND wc.is_complete = 0
        ) THEN 1
        ELSE 0
    END AS is_complete
),
derived_downstream_max AS (
    SELECT awr.series_id,
           awr.week_id,
           MAX(d.updated_at) AS max_downstream_updated_at
    FROM nascar.agg_week_results awr
    LEFT JOIN (
        SELECT series_id, week_id, updated_at
        FROM nascar.agg_week_teams_results
        WHERE deleted_at IS NULL
        UNION ALL
        SELECT series_id, week_id, updated_at
        FROM nascar.agg_week_manufacturers_results
        WHERE deleted_at IS NULL
        UNION ALL
        SELECT series_id, week_id, updated_at
        FROM nascar.agg_week_prev_track_cars_drivers_results
        WHERE deleted_at IS NULL
    ) d ON d.series_id = awr.series_id AND d.week_id = awr.week_id
    WHERE awr.deleted_at IS NULL
    GROUP BY awr.series_id, awr.week_id
)
SELECT DISTINCT wr.series_id, wr.week_id
FROM weeks_with_race wr
JOIN current_week cw ON cw.week_id IS NOT NULL
CROSS JOIN history_coverage_complete hcc
LEFT JOIN week_completeness wc
    ON wc.series_id = wr.series_id AND wc.week_id = wr.week_id
LEFT JOIN nascar.agg_week_results awr
    ON awr.series_id = wr.series_id
   AND awr.week_id = wr.week_id
   AND awr.deleted_at IS NULL
LEFT JOIN derived_downstream_max ddm
    ON ddm.series_id = wr.series_id AND ddm.week_id = wr.week_id
WHERE (
    hcc.is_complete = 0
    AND COALESCE(wc.is_complete, 0) = 0
)
OR (
    hcc.is_complete = 1
    AND wr.week_id = cw.week_id
    AND (
        COALESCE(wc.is_complete, 0) = 0
        OR awr.updated_at > COALESCE(
            ddm.max_downstream_updated_at,
            TIMESTAMP '1970-01-01 00:00:00'
        )
    )
)
ORDER BY wr.week_id
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


def _rebuild_all_weeks_enabled() -> bool:
    raw = os.environ.get("FS_AGG_REBUILD_ALL_WEEKS", "0")
    return raw.strip().lower() in ("1", "true", "yes", "on")


def aggregate_load_parameters(context: Mapping[str, Any]) -> dict[str, Any]:
    """Weeks needing wave-1 / wave-2 reload after ``load_agg_week_results`` (incremental)."""
    series_id = NascarSeries.CUP.series_id
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    if _rebuild_all_weeks_enabled():
        sql = FINISHED_AGG_WEEKS_SQL
        parameters: tuple[int, ...] = (series_id,)
        mode = "rebuild_all"
    else:
        sql = WEEKS_TO_AGGREGATE_SQL
        parameters = (series_id, series_id)
        mode = "incremental"
    rows = hook.get_records(sql, parameters=parameters)
    weeks = [{"week_id": int(week_id)} for _series_id, week_id in rows]
    _LOG.info(
        "aggregate_load_parameters series_id=%s mode=%s weeks_to_load=%s",
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
        parameters_factory=aggregate_load_parameters,
    )
