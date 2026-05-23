"""
Top-level Cup scraper importer DAG.

**Schedule:** hourly (see ``CUP_SCRAPER_PROD_HOURLY_CRON`` in
``nascar.cup.scraper.backfill_assets``), aligned with race-week DAGs. The first
tasks wait for ``sync_race_results`` on both FRCS and Racing Reference Cup
race-week DAGs for the same logical date (``ExternalTaskSensor``; default lookup
uses the sensor task's ``logical_date``, which matches when schedules are
identical—Airflow 3.1).

The yearly ``backfill.nascar.cup.importer`` DAG is bound as
``backfill_dag`` in this module (same graph, waits on ``backfill.*`` race-week
DAGs).

Qualifying CSV load prefers **FRCS** ``qualifying.csv`` under the week folder;
if that file is missing, the same importer path uses **Racing Reference**
``qualifying.csv`` when present. If neither exists, no qualifying rows are
staged or imported for that week.

Practice CSVs ``practice-<n>.csv`` follow the same preference per session
number. Racing Reference practice exports omit FRCS-only columns (multi-lap
averages and 10-lap window fields); those staging columns are left **NULL**
when loading RR files.

Racing Reference week folders use ``week_number`` from RR ``schedule.csv``, which can
differ from FRCS ``event_number`` stored in ``nascar.weeks.number``. The importer maps
RR disk paths using ``week_date`` + ``week_race_name`` (see
``include.nascar.cup_rr_week_folder_map``) with FRCS-derived DB rows as source of truth.

Week-scoped staging/temp tables include ``table_suffix`` (run anchor, same
``strftime('%Y%m%d%H%M')`` as schedule staging) so each DAG run only touches its own
tables. The final task ``cleanup_importer_staging_and_temp_tables`` drops staging and
temp tables for this run whose names end with that suffix.
"""

from __future__ import annotations

import glob
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG
from jinja2 import Template
from nascar.cup.scraper.backfill_assets import (
    BACKFILL_END_DATE,
    BACKFILL_START_DATE,
    BACKFILL_YEARLY_SCHEDULE,
    CUP_SCRAPER_PROD_HOURLY_CRON,
    maybe_register_backfill_dag,
)
from nascar.cup.scraper.dag_factories import (
    CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
    CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
)

from airflow_utils import MySQLTemplateFileOperatorFromContext
from airflow_utils.mysql_template_file_executor import execute_sql_statements_transactional
from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.cup_rr_week_folder_map import (
    load_racing_reference_schedule_rows,
    match_rr_week_number,
)
from include.nascar.data_sources.frcs import FrcsSource
from include.nascar.data_sources.racing_reference import RacingReferenceSource
from include.nascar.data_sources.racing_reference_race_week import (
    _cup_series_schedule_year_from_context,
    race_week_run_anchor_from_context,
)
from include.nascar.domain_events import (
    CupImporterEventType,
    DomainEvent,
    airflow_context_from_mapping,
    emit,
)
from include.nascar.enums import NascarSeries

DAG_ID = "nascar.cup.importer"

_SQL_DAG_SUBDIR = "nascar.cup.importer"
_SQL_SENTINEL = Path(_SQL_DAG_SUBDIR) / "load_stage_schedule_csvs.sql"

# Airflow task logger so PythonOperator callables emit to the task log UI.
_LOG = logging.getLogger("airflow.task")

# Table names from information_schema must match before DROP (defense in depth).
_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
_IMPORTER_STAGING_PREFIXES = (
    "stg_cup_frcs_schedule_import_",
    "stg_cup_racing_reference_schedule_import_",
    "stg_frcs_",
    "stg_car_",
    "stg_car_season_",
    "tmp_cup_schedule_frcs_",
    "tmp_cup_schedule_rr_",
    "tmp_cup_schedule_rr_match_guard_",
    "tmp_cup_schedule_merged_",
    "tmp_cup_schedule_",
    "tmp_car_",
    "tmp_car_season_",
    "tmp_preload_driver_car_wide_",
    "tmp_driver_car_map_",
    "tmp_practice_",
    "tmp_qualifying_",
    "tmp_race_",
)
_TABLE_SUFFIX_RE = re.compile(r"^\d{12}$")


def _sql_root() -> Path:
    """
    Resolve the SQL root that contains the ``nascar.cup.importer`` folder.

    Canonical layout: ``dags/sql/nascar.cup.importer/`` (top of the DAGs folder).
    Fallbacks: ``…/cup/scraper/sql/`` next to this file, repo-root ``sql/``, ``$AIRFLOW_HOME/sql``,
    or ``NASCAR_AIRFLOW_SQL_ROOT`` (directory that contains the DAG subfolder).
    """
    override = os.environ.get("NASCAR_AIRFLOW_SQL_ROOT", "").strip()
    if override:
        root = Path(override)
        if (root / _SQL_SENTINEL).is_file():
            return root
        raise FileNotFoundError(
            f"NASCAR_AIRFLOW_SQL_ROOT={override!r} does not contain {_SQL_SENTINEL.as_posix()}"
        )

    dag_file = Path(__file__).resolve()
    # ``importer.py`` lives at ``dags/nascar/cup/`` → ``parents[2]`` is ``dags/``.
    candidates = [
        dag_file.parents[2] / "sql",
        dag_file.parent / "sql",
        dag_file.parents[3] / "sql",
        Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "sql",
    ]
    for root in candidates:
        if (root / _SQL_SENTINEL).is_file():
            return root

    searched = ", ".join(str(r) for r in candidates)
    raise FileNotFoundError(
        f"Could not find {_SQL_DAG_SUBDIR} SQL files. Tried: {searched}. "
        f"Set NASCAR_AIRFLOW_SQL_ROOT to the directory containing {_SQL_DAG_SUBDIR!r}."
    )


def _render_sql(sql_file_path: Path, context: dict[str, object]) -> str:
    with open(sql_file_path, encoding="utf-8") as f:
        return Template(f.read()).render(context)


def _emit_cup_importer_domain_event(
    context: dict[str, Any],
    *,
    event_type: str,
    entity_tables: tuple[str, ...],
    payload: dict[str, Any],
) -> None:
    emit(
        DomainEvent(
            event_type=event_type,
            entity_tables=entity_tables,
            payload=payload,
            airflow=airflow_context_from_mapping(context),
        )
    )


def _emit_cup_schedule_staged_domain_event(context: dict[str, Any]) -> None:
    """``on_success_callback`` for ``load_stage_schedule_csvs`` (no Python callable wraps the SQL)."""
    year = _cup_series_schedule_year_from_context(context)
    anchor = race_week_run_anchor_from_context(context)
    frcs = FrcsSource()
    racing_reference = RacingReferenceSource()
    _emit_cup_importer_domain_event(
        context,
        event_type=CupImporterEventType.SCHEDULE_STAGED,
        entity_tables=("stg_cup_frcs_schedule_import",),
        payload={
            "season_year": year,
            "table_suffix": anchor.strftime("%Y%m%d%H%M"),
            "data_source": frcs.id,
        },
    )
    _emit_cup_importer_domain_event(
        context,
        event_type=CupImporterEventType.SCHEDULE_STAGED,
        entity_tables=("stg_cup_racing_reference_schedule_import",),
        payload={
            "season_year": year,
            "table_suffix": anchor.strftime("%Y%m%d%H%M"),
            "data_source": racing_reference.id,
        },
    )
    


def _emit_cup_schedule_imported_domain_event(context: dict[str, Any]) -> None:
    """``on_success_callback`` for ``import_schedule``."""
    year = _cup_series_schedule_year_from_context(context)
    anchor = race_week_run_anchor_from_context(context)
    frcs = FrcsSource()
    _emit_cup_importer_domain_event(
        context,
        event_type=CupImporterEventType.SCHEDULE_IMPORTED,
        entity_tables=(
            "series",
            "seasons",
            "tracks",
            "weeks",
            "data_sources",
            "data_sources_tracks",
        ),
        payload={
            "series_id": NascarSeries.CUP.series_id,
            "season_year": year,
            "table_suffix": anchor.strftime("%Y%m%d%H%M"),
            "data_source": frcs.id,
        },
    )


def _execute_templated_sql_file(
    hook: NascarMySqlHook, sql_file_path: Path, context: dict[str, object], source_name: str
) -> None:
    """
    Run the file's ``---``-separated statements on one connection with commit/rollback.

    See :func:`airflow_utils.mysql_template_file_executor.execute_sql_statements_transactional`
    for MySQL DDL / implicit-commit limitations.
    """
    render_context = {**context, "source_name": source_name}
    context_keys = ("week_id", "number", "table_suffix")
    context_summary = ", ".join(
        f"{key}={render_context[key]}" for key in context_keys if key in render_context
    )
    if context_summary:
        _LOG.info(
            "Cup importer SQL file=%s source_name=%s (%s)",
            sql_file_path,
            source_name,
            context_summary,
        )
    else:
        _LOG.info("Cup importer SQL file=%s source_name=%s", sql_file_path, source_name)
    rendered_sql = _render_sql(sql_file_path, render_context)
    statements = [s.strip() for s in rendered_sql.split("---") if s.strip()]
    execute_sql_statements_transactional(hook, statements)


def _nascar_staging_table_exists(hook: NascarMySqlHook, table_name: str) -> bool:
    """
    Whether ``nascar.<table_name>`` exists.

    Import tasks must not rely only on ``os.path.isfile`` for the CSV: the load task may run on a
    different executor with access to the scraper volume while this worker only sees MySQL.
    """
    row = hook.get_first(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'nascar' AND table_name = %s
        LIMIT 1
        """,
        parameters=(table_name,),
    )
    return row is not None


def _target_weeks(hook: NascarMySqlHook, *, series_id: int, year: int) -> list[tuple[int, int]]:
    """
    All Cup ``weeks`` rows for the season (see ``select_weeks_needing_csv_import.sql``).

    Post-race CSVs must still be staged after ``race_results`` exist, so the list is not
    limited to weeks missing finish data.
    """
    sql = _render_sql(
        _sql_root() / "nascar.cup.importer" / "select_weeks_needing_csv_import.sql",
        {"series_id": series_id, "year": year},
    )
    rows = hook.get_records(sql)
    return [(int(r[0]), int(r[1])) for r in rows]


def _racing_reference_schedule_csv_path(year: int) -> str:
    return f"{RacingReferenceSource().storage_path_series_year(NascarSeries.CUP.db_name, year)}/schedule.csv"


def _cup_weeks_metadata_for_rr_map(
    hook: NascarMySqlHook, *, series_id: int, year: int, week_ids: list[int]
) -> dict[int, tuple[str | None, Any]]:
    """``week_id`` -> ``(race_name, scheduled_start)`` for Racing Reference folder matching."""
    if not week_ids:
        return {}
    placeholders = ",".join(["%s"] * len(week_ids))
    sql = f"""
    SELECT w.id, w.race_name, w.scheduled_start
    FROM nascar.weeks w
    JOIN nascar.seasons s
        ON s.id = w.season_id
        AND s.deleted_at IS NULL
    WHERE s.series_id = %s
      AND s.year = %s
      AND w.deleted_at IS NULL
      AND w.id IN ({placeholders})
    """
    rows = hook.get_records(sql, parameters=(series_id, year, *week_ids))
    return {int(r[0]): (r[1], r[2]) for r in rows}


def _season_ids_for_weeks(hook: NascarMySqlHook, week_ids: list[int]) -> dict[int, int]:
    if not week_ids:
        return {}
    placeholders = ",".join(["%s"] * len(week_ids))
    sql = f"""
    SELECT w.id, w.season_id
    FROM nascar.weeks w
    WHERE w.deleted_at IS NULL
      AND w.id IN ({placeholders})
    """
    rows = hook.get_records(sql, parameters=tuple(week_ids))
    return {int(r[0]): int(r[1]) for r in rows}


def _build_importer_target_weeks_xcom_rows(
    hook: NascarMySqlHook,
    weeks: list[tuple[int, int]],
    *,
    series_id: int,
    year: int,
) -> list[dict[str, Any]]:
    """
    One XCom row per target week: ``rr_week_number`` is Racing Reference ``schedule.csv`` folder
    index when it differs from FRCS ``event_number``; ``None`` means fall back to ``event_number``
    for RR disk paths (legacy / no match).
    """
    week_ids = [w for w, _ in weeks]
    meta = _cup_weeks_metadata_for_rr_map(hook, series_id=series_id, year=year, week_ids=week_ids)
    season_ids = _season_ids_for_weeks(hook, week_ids)
    rr_rows = load_racing_reference_schedule_rows(_racing_reference_schedule_csv_path(year))
    out: list[dict[str, Any]] = []
    for week_id, event_number in weeks:
        race_name, scheduled_start = meta.get(week_id, (None, None))
        rr_wn = match_rr_week_number(rr_rows, scheduled_start=scheduled_start, race_name=race_name)
        season_id = season_ids.get(week_id)
        if season_id is None:
            raise ValueError(f"missing season_id for week_id={week_id}")
        out.append(
            {
                "week_id": week_id,
                "season_id": season_id,
                "event_number": event_number,
                "rr_week_number": rr_wn,
            }
        )
    return out


def _push_target_weeks(kwargs: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    kwargs["ti"].xcom_push(key="importer_target_weeks", value=rows)


def _push_staged_practices(kwargs: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    """Persist stage-time practice metadata for deterministic imports."""
    kwargs["ti"].xcom_push(key="importer_staged_practices", value=rows)


def _pull_staged_practices(kwargs: dict[str, Any]) -> list[dict[str, Any]]:
    raw = kwargs["ti"].xcom_pull(task_ids="load_stage_practices_csvs", key="importer_staged_practices") or []
    out: list[dict[str, Any]] = []
    for row in raw:
        out.append(
            {
                "week_id": int(row["week_id"]),
                "event_number": int(row["event_number"]),
                "number": int(row["number"]),
                "source_id": str(row["source_id"]),
                "staged_table_name": str(row["staged_table_name"]),
            }
        )
    return out


def _push_staged_qualifyings(kwargs: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    """Persist stage-time qualifying metadata for deterministic imports."""
    kwargs["ti"].xcom_push(key="importer_staged_qualifyings", value=rows)


def _pull_staged_qualifyings(kwargs: dict[str, Any]) -> list[dict[str, Any]]:
    raw = (
        kwargs["ti"].xcom_pull(task_ids="load_stage_qualifyings_csvs", key="importer_staged_qualifyings")
        or []
    )
    out: list[dict[str, Any]] = []
    for row in raw:
        out.append(
            {
                "week_id": int(row["week_id"]),
                "event_number": int(row["event_number"]),
                "source_id": str(row["source_id"]),
                "staged_table_name": str(row["staged_table_name"]),
            }
        )
    return out


def _importer_target_week_rows(kwargs: dict[str, Any]) -> list[dict[str, Any]]:
    """Rebuild ``importer_target_weeks`` XCom payload from DB + RR schedule (same as entrylist staging)."""
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    weeks = _target_weeks(hook, series_id=series.series_id, year=year)
    return _build_importer_target_weeks_xcom_rows(
        hook, weeks, series_id=series.series_id, year=year
    )


def _pull_target_weeks(kwargs: dict[str, Any]) -> list[tuple[int, int, int, int | None]]:
    """
    ``(week_id, season_id, frcs_event_number, rr_week_number)``.

    ``rr_week_number`` is the RR disk folder index when present; ``None`` means use
    ``frcs_event_number`` for ``RacingReferenceSource.storage_path_week`` (backward compatible).

    Week rows are rebuilt from DB + RR schedule each invocation so downstream CSV loaders
    remain independent of task-specific XCom propagation behavior.
    """
    raw = _importer_target_week_rows(kwargs)
    hook: NascarMySqlHook | None = None
    out: list[tuple[int, int, int, int | None]] = []
    for r in raw:
        week_id = int(r["week_id"])
        season_raw = r.get("season_id")
        if season_raw is None or season_raw == "":
            if hook is None:
                hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
            row = hook.get_first(
                """
                SELECT season_id
                FROM nascar.weeks
                WHERE id = %s
                  AND deleted_at IS NULL
                LIMIT 1
                """,
                parameters=(week_id,),
            )
            if not row or row[0] is None:
                raise KeyError(
                    f"season_id missing for week_id={week_id} in importer target weeks payload"
                )
            season_id = int(row[0])
        else:
            season_id = int(season_raw)
        event_number = int(r["event_number"])
        rr_raw = r.get("rr_week_number", None)
        if rr_raw is None or rr_raw == "":
            rr_wn: int | None = None
        else:
            rr_wn = int(rr_raw)
        out.append((week_id, season_id, event_number, rr_wn))
    return out


def _rr_week_disk_folder_index(rr_week_number: int | None, frcs_event_number: int) -> int:
    """Racing Reference ``storage_path_week`` index: explicit map, else FRCS event folder."""
    return frcs_event_number if rr_week_number is None else rr_week_number


def _context_year(kwargs: dict[str, Any]) -> int:
    return _cup_series_schedule_year_from_context(kwargs)


def _importer_table_suffix(context: dict[str, Any]) -> str:
    """Matches ``table_suffix`` used in schedule load/import (per DAG run)."""
    return race_week_run_anchor_from_context(context).strftime("%Y%m%d%H%M")


def _stg_car_table_name(week_id: int, suffix: str) -> str:
    return f"stg_car_{week_id}_{suffix}"


def _stg_car_season_table_name(season_id: int, suffix: str) -> str:
    return f"stg_car_season_{season_id}_{suffix}"


def _table_exists(hook: NascarMySqlHook, table_name: str) -> bool:
    row = hook.get_first(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'nascar'
          AND table_name = %s
        LIMIT 1
        """,
        parameters=(table_name,),
    )
    return bool(row)


def _cup_practice_csv_path_in_week_dir(base_dir: str, session_num: int) -> str | None:
    """
    Resolve ``practice-<n>.csv`` under ``base_dir``, tolerating leading zeros in ``n``
    (FRCS on-disk names like ``practice-05.csv`` while session numbers are ``5``).
    """
    direct = os.path.join(base_dir, f"practice-{session_num}.csv")
    if os.path.isfile(direct):
        return direct
    for path in sorted(glob.glob(os.path.join(base_dir, "practice-*.csv"))):
        match = re.search(r"practice-(\d+)\.csv$", path)
        if match and int(match.group(1)) == session_num:
            return path
    return None


def _cup_practice_session_numbers(
    series: NascarSeries, year: int, event_number: int, rr_week_number: int | None
) -> list[int]:
    """Sorted practice session indices that have a CSV under FRCS and/or Racing Reference."""
    frcs = FrcsSource()
    rr = RacingReferenceSource()
    rr_disk = _rr_week_disk_folder_index(rr_week_number, event_number)
    nums: set[int] = set()
    for base in (
        frcs.storage_path_week(series.db_name, year, event_number),
        rr.storage_path_week(series.db_name, year, rr_disk),
    ):
        for path in glob.glob(os.path.join(base, "practice-*.csv")):
            match = re.search(r"practice-(\d+)\.csv$", path)
            if match:
                nums.add(int(match.group(1)))
    return sorted(nums)


def _cup_practice_csv_for_session(
    series: NascarSeries,
    year: int,
    event_number: int,
    session_num: int,
    rr_week_number: int | None,
) -> tuple[str | None, str]:
    """
    Return ``(path, 'frcs'|'racing_reference')`` for ``practice-<session>.csv``, or ``(None, '')``.

    Prefer FRCS; Racing Reference rows map only the eight shared columns (see
    ``load_practice_csv_into_staging_table.sql`` when
    ``practice_csv_source == 'racing_reference'``).
    """
    frcs = FrcsSource()
    rr = RacingReferenceSource()
    rr_disk = _rr_week_disk_folder_index(rr_week_number, event_number)
    frcs_base = frcs.storage_path_week(series.db_name, year, event_number)
    frcs_p = _cup_practice_csv_path_in_week_dir(frcs_base, session_num)
    if frcs_p:
        return frcs_p, "frcs"
    rr_base = rr.storage_path_week(series.db_name, year, rr_disk)
    rr_p = _cup_practice_csv_path_in_week_dir(rr_base, session_num)
    if rr_p:
        _LOG.info(
            "Cup importer: %s from Racing Reference (FRCS missing) year=%s event=%s",
            os.path.basename(rr_p),
            year,
            event_number,
        )
        return rr_p, "racing_reference"
    return None, ""


def _cup_practice_data_source_id(
    series: NascarSeries,
    year: int,
    event_number: int,
    session_num: int,
    rr_week_number: int | None,
) -> str:
    """``data_sources.name`` for the CSV used for this practice session (FRCS vs Racing Reference)."""
    _, src = _cup_practice_csv_for_session(
        series, year, event_number, session_num, rr_week_number
    )
    if src == "racing_reference":
        return RacingReferenceSource().id
    return FrcsSource().id


def _cup_qualifying_csv_path(
    series: NascarSeries, year: int, event_number: int, rr_week_number: int | None
) -> tuple[str | None, str]:
    """
    Return ``(path, 'frcs'|'racing_reference'|'')`` for ``qualifying.csv``.

    Prefer FRCS; fall back to Racing Reference (same six-column layout as
    ``load_qualifying_csv_into_staging_table.sql``).
    """
    frcs = FrcsSource()
    rr = RacingReferenceSource()
    rr_disk = _rr_week_disk_folder_index(rr_week_number, event_number)
    frcs_csv = os.path.join(frcs.storage_path_week(series.db_name, year, event_number), "qualifying.csv")
    if os.path.isfile(frcs_csv):
        return frcs_csv, "frcs"
    rr_csv = os.path.join(rr.storage_path_week(series.db_name, year, rr_disk), "qualifying.csv")
    if os.path.isfile(rr_csv):
        _LOG.info(
            "Cup importer: qualifying.csv from Racing Reference (FRCS missing) year=%s event=%s",
            year,
            event_number,
        )
        return rr_csv, "racing_reference"
    _LOG.info(
        "Cup importer: no qualifying.csv for FRCS or Racing Reference year=%s event=%s",
        year,
        event_number,
    )
    return None, ""


def _cup_qualifying_data_source_id(
    series: NascarSeries, year: int, event_number: int, rr_week_number: int | None
) -> str:
    """``data_sources.name`` for the qualifying CSV origin (FRCS vs Racing Reference)."""
    _, src = _cup_qualifying_csv_path(series, year, event_number, rr_week_number)
    if src == "racing_reference":
        return RacingReferenceSource().id
    return FrcsSource().id


def _importer_params_load_schedule(context: dict[str, Any]) -> dict[str, Any]:
    year = _cup_series_schedule_year_from_context(context)
    anchor = race_week_run_anchor_from_context(context)
    frcs = FrcsSource()
    racing_reference = RacingReferenceSource()
    return {
        "frcs_schedule_csv_file_path": f"{frcs.storage_path_series_year('cup', year)}/schedule.csv",
        "racing_reference_schedule_csv_file_path": (
            f"{racing_reference.storage_path_series_year('cup', year)}/schedule.csv"
        ),
        "table_suffix": anchor.strftime("%Y%m%d%H%M"),
    }


def _importer_params_import_schedule(context: dict[str, Any]) -> dict[str, Any]:
    year = _cup_series_schedule_year_from_context(context)
    anchor = race_week_run_anchor_from_context(context)
    frcs = FrcsSource()
    racing_reference = RacingReferenceSource()
    return {
        "series_id": NascarSeries.CUP.series_id,
        "series_name": NascarSeries.CUP.db_name,
        "source_name": frcs.id,
        "frcs_source_name": frcs.id,
        "racing_reference_source_name": racing_reference.id,
        "source_url": frcs.base_url,
        "season_year": year,
        "table_suffix": anchor.strftime("%Y%m%d%H%M"),
    }


def load_stage_practices_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    stage_sql = (
        _sql_root() / "nascar.cup.importer" / "load_practice_csv_into_staging_table.sql"
    )
    staged_rows: list[dict[str, Any]] = []
    target_weeks = _pull_target_weeks(kwargs)
    _LOG.info("Cup importer: load_stage_practices target_weeks=%s", len(target_weeks))
    for week_id, season_id, event_number, rr_week_number in target_weeks:
        for n in _cup_practice_session_numbers(series, year, event_number, rr_week_number):
            csv_path, src = _cup_practice_csv_for_session(
                series, year, event_number, n, rr_week_number
            )
            if not csv_path:
                continue
            source_id = RacingReferenceSource().id if src == "racing_reference" else FrcsSource().id
            practice_csv_source = "racing_reference" if src == "racing_reference" else "frcs"
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {
                    "week_id": week_id,
                    "season_id": season_id,
                    "number": n,
                    "practice_csv_file_path": csv_path,
                    "table_suffix": suffix,
                    "practice_csv_source": practice_csv_source,
                    "frcs_source_name": FrcsSource().id,
                    "racing_reference_source_name": RacingReferenceSource().id,
                },
                source_id,
            )
            staged_rows.append(
                {
                    "week_id": week_id,
                    "event_number": event_number,
                    "number": n,
                    "source_id": source_id,
                    "staged_table_name": f"stg_frcs_practice_{week_id}_{n}_{suffix}",
                }
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.PRACTICE_STAGED,
                entity_tables=("stg_frcs_practice",),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "practice_session": n,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source_id,
                },
            )
    _push_staged_practices(kwargs, staged_rows)


def import_practices(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    sql = _sql_root() / "nascar.cup.importer" / "import_practice_staging_into_db.sql"
    for row in _pull_staged_practices(kwargs):
        week_id = row["week_id"]
        event_number = row["event_number"]
        n = row["number"]
        source_id = row["source_id"]
        stg = row["staged_table_name"]
        if not _nascar_staging_table_exists(hook, stg):
            continue
        temp_car_map = f"tmp_car_{week_id}_{suffix}"
        has_car_map_temp = _nascar_staging_table_exists(hook, temp_car_map)
        if not has_car_map_temp:
            _LOG.warning(
                "Cup importer: skipping practice import week_id=%s session=%s: missing %s "
                "(resolver did not materialize canonical tmp_car for this run)",
                week_id,
                n,
                temp_car_map,
            )
            continue
        _execute_templated_sql_file(
            hook,
            sql,
            {
                "week_id": week_id,
                "series_id": series.series_id,
                "number": n,
                "table_suffix": suffix,
            },
            source_id,
        )
        _emit_cup_importer_domain_event(
            kwargs,
            event_type=CupImporterEventType.PRACTICE_IMPORTED,
            entity_tables=(
                "practices",
                "practice_results",
                "cars",
                "drivers",
                "cars_drivers",
                "manufacturers",
                "teams",
                "data_sources",
            ),
            payload={
                "week_id": week_id,
                "event_number": event_number,
                "practice_session": n,
                "season_year": year,
                "series_id": series.series_id,
                "table_suffix": suffix,
                "data_source": source_id,
            },
        )


def load_stage_qualifyings_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    stage_sql = (
        _sql_root() / "nascar.cup.importer" / "load_qualifying_csv_into_staging_table.sql"
    )
    staged_rows: list[dict[str, Any]] = []
    for week_id, season_id, event_number, rr_week_number in _pull_target_weeks(kwargs):
        csv, src = _cup_qualifying_csv_path(series, year, event_number, rr_week_number)
        if csv:
            source_id = RacingReferenceSource().id if src == "racing_reference" else FrcsSource().id
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "season_id": season_id, "qualifying_csv_file_path": csv, "table_suffix": suffix, "frcs_source_name": FrcsSource().id, "racing_reference_source_name": RacingReferenceSource().id},
                source_id,
            )
            staged_rows.append(
                {
                    "week_id": week_id,
                    "event_number": event_number,
                    "source_id": source_id,
                    "staged_table_name": f"stg_frcs_qualifying_{week_id}_{suffix}",
                }
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.QUALIFYING_STAGED,
                entity_tables=("stg_frcs_qualifying",),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source_id,
                },
            )
    _push_staged_qualifyings(kwargs, staged_rows)


def import_qualifyings(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    sql = _sql_root() / "nascar.cup.importer" / "import_qualifying_staging_into_db.sql"
    for row in _pull_staged_qualifyings(kwargs):
        week_id = row["week_id"]
        event_number = row["event_number"]
        source_id = row["source_id"]
        stg = row["staged_table_name"]
        if not _nascar_staging_table_exists(hook, stg):
            continue
        temp_car_map = f"tmp_car_{week_id}_{suffix}"
        has_car_map_temp = _nascar_staging_table_exists(hook, temp_car_map)
        if not has_car_map_temp:
            _LOG.warning(
                "Cup importer: skipping qualifying import week_id=%s: missing %s "
                "(resolver did not materialize canonical tmp_car for this run)",
                week_id,
                temp_car_map,
            )
            continue
        _execute_templated_sql_file(
            hook,
            sql,
            {"week_id": week_id, "series_id": series.series_id, "table_suffix": suffix, "frcs_source_name": FrcsSource().id, "racing_reference_source_name": RacingReferenceSource().id},
            source_id,
        )
        _emit_cup_importer_domain_event(
            kwargs,
            event_type=CupImporterEventType.QUALIFYING_IMPORTED,
            entity_tables=(
                "qualifyings",
                "qualifying_results",
                "cars",
                "drivers",
                "cars_drivers",
                "manufacturers",
                "teams",
                "data_sources",
            ),
            payload={
                "week_id": week_id,
                "event_number": event_number,
                "season_year": year,
                "series_id": series.series_id,
                "table_suffix": suffix,
                "data_source": source_id,
            },
        )


def load_stage_lineup_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = _sql_root() / "nascar.cup.importer" / "load_lineup_csv_into_staging_table.sql"
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "lineup.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "season_id": season_id, "lineup_csv_file_path": csv, "table_suffix": suffix, "frcs_source_name": FrcsSource().id, "racing_reference_source_name": RacingReferenceSource().id},
                source.id,
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.LINEUP_STAGED,
                entity_tables=("stg_frcs_lineup",),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source.id,
                },
            )


def load_stage_entrylist_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = _sql_root() / "nascar.cup.importer" / "load_entrylist_csv_into_staging_table.sql"
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "entrylist.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {
                    "week_id": week_id,
                    "season_id": season_id,
                    "entrylist_csv_file_path": csv,
                    "table_suffix": suffix,
                    "frcs_source_name": FrcsSource().id,
                    "racing_reference_source_name": RacingReferenceSource().id,
                },
                source.id,
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.LINEUP_STAGED,
                entity_tables=("stg_frcs_entrylist",),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source.id,
                },
            )


def import_lineup(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    sql = _sql_root() / "nascar.cup.importer" / "import_lineup_staging_into_db.sql"
    source = FrcsSource()
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "lineup.csv")
        stg = f"stg_frcs_lineup_{week_id}_{suffix}"
        if not os.path.isfile(csv) and not _nascar_staging_table_exists(hook, stg):
            continue
        _execute_templated_sql_file(
            hook,
            sql,
            {
                "week_id": week_id,
                "series_id": series.series_id,
                "source_name": source.id,
                "table_suffix": suffix,
            },
            source.id,
        )
        _emit_cup_importer_domain_event(
            kwargs,
            event_type=CupImporterEventType.LINEUP_IMPORTED,
            entity_tables=(
                "races",
                "race_results",
                "cars",
                "drivers",
                "cars_drivers",
                "manufacturers",
                "teams",
                "data_sources",
            ),
            payload={
                "week_id": week_id,
                "event_number": event_number,
                "season_year": year,
                "series_id": series.series_id,
                "table_suffix": suffix,
                "data_source": source.id,
            },
        )


def load_stage_races_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = _sql_root() / "nascar.cup.importer" / "load_race_csvs_into_staging_tables.sql"
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        base = source.storage_path_week(series.db_name, year, event_number)
        race_csv = os.path.join(base, "race-results.csv")
        loop_csv = os.path.join(base, "loop-data.csv")
        if os.path.isfile(race_csv) and os.path.isfile(loop_csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {
                    "week_id": week_id,
                    "season_id": season_id,
                    "race_results_csv_file_path": race_csv,
                    "race_loop_data_csv_file_path": loop_csv,
                    "table_suffix": suffix,
                    "frcs_source_name": FrcsSource().id,
                    "racing_reference_source_name": RacingReferenceSource().id,
                },
                source.id,
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.RACE_STAGED,
                entity_tables=("stg_frcs_race_results", "stg_frcs_race_loop_data"),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source.id,
                },
            )


def import_races(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.importer" / "import_race_staging_into_db.sql"
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        base = source.storage_path_week(series.db_name, year, event_number)
        race_csv = os.path.join(base, "race-results.csv")
        loop_csv = os.path.join(base, "loop-data.csv")
        if os.path.isfile(race_csv) and os.path.isfile(loop_csv):
            temp_car_map = f"tmp_car_{week_id}_{suffix}"
            has_car_map_temp = _nascar_staging_table_exists(hook, temp_car_map)
            if not has_car_map_temp:
                _LOG.warning(
                    "Cup importer: skipping race import week_id=%s: missing %s "
                    "(resolver did not materialize canonical tmp_car for this run)",
                    week_id,
                    temp_car_map,
                )
                continue
            _execute_templated_sql_file(
                hook,
                sql,
                {
                    "week_id": week_id,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                },
                source.id,
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.RACE_IMPORTED,
                entity_tables=(
                    "races",
                    "race_results",
                    "cars",
                    "drivers",
                    "cars_drivers",
                    "manufacturers",
                    "teams",
                    "data_sources",
                ),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source.id,
                },
            )


def load_stage_penalties_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = _sql_root() / "nascar.cup.importer" / "load_penalties_csv_into_staging_table.sql"
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "penalties.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "season_id": season_id, "penalties_csv_file_path": csv, "table_suffix": suffix, "frcs_source_name": FrcsSource().id, "racing_reference_source_name": RacingReferenceSource().id},
                source.id,
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.PENALTIES_STAGED,
                entity_tables=("stg_frcs_penalties",),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source.id,
                },
            )


def import_penalties(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    sql = _sql_root() / "nascar.cup.importer" / "import_penalties_staging_into_db.sql"
    source = FrcsSource()
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "penalties.csv")
        stg = f"stg_frcs_penalties_{week_id}_{suffix}"
        if not os.path.isfile(csv) and not _nascar_staging_table_exists(hook, stg):
            continue
        _execute_templated_sql_file(
            hook,
            sql,
            {"week_id": week_id, "source_name": source.id, "table_suffix": suffix, "frcs_source_name": FrcsSource().id, "racing_reference_source_name": RacingReferenceSource().id},
            source.id,
        )
        _emit_cup_importer_domain_event(
            kwargs,
            event_type=CupImporterEventType.PENALTIES_IMPORTED,
            entity_tables=("penalties", "cars_drivers", "data_sources"),
            payload={
                "week_id": week_id,
                "event_number": event_number,
                "season_year": year,
                "series_id": series.series_id,
                "table_suffix": suffix,
                "data_source": source.id,
            },
        )


def load_stage_pitstops_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = (
        _sql_root() / "nascar.cup.importer" / "load_pit_stops_detail_csv_into_staging_table.sql"
    )
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(
            source.storage_path_week(series.db_name, year, event_number), "pit-stops-detail.csv"
        )
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "season_id": season_id, "pit_stops_detail_csv_file_path": csv, "table_suffix": suffix, "frcs_source_name": FrcsSource().id, "racing_reference_source_name": RacingReferenceSource().id},
                source.id,
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.PIT_STOPS_STAGED,
                entity_tables=("stg_frcs_pit_stops_detail",),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source.id,
                },
            )


def import_pitstops(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    sql = _sql_root() / "nascar.cup.importer" / "import_pit_stops_detail_staging_into_db.sql"
    source = FrcsSource()
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(
            source.storage_path_week(series.db_name, year, event_number), "pit-stops-detail.csv"
        )
        stg = f"stg_frcs_pit_stops_detail_{week_id}_{suffix}"
        if not os.path.isfile(csv) and not _nascar_staging_table_exists(hook, stg):
            continue
        try:
            _execute_templated_sql_file(
                hook,
                sql,
                {"week_id": week_id, "source_name": source.id, "table_suffix": suffix, "frcs_source_name": FrcsSource().id, "racing_reference_source_name": RacingReferenceSource().id},
                source.id,
            )
        except Exception as exc:
            # #region agent log
            try:
                with open(
                    "/home/jackalope/bin/.cursor/debug-d4461a.log",
                    "a",
                    encoding="utf-8",
                ) as _dbg_fp:
                    _dbg_fp.write(
                        json.dumps(
                            {
                                "sessionId": "d4461a",
                                "hypothesisId": "H-pitstops-import",
                                "location": "importer.py:import_pitstops",
                                "message": "import_pitstops SQL failed",
                                "data": {
                                    "week_id": week_id,
                                    "event_number": event_number,
                                    "table_suffix": suffix,
                                    "error_type": type(exc).__name__,
                                    "error": str(exc),
                                },
                                "timestamp": int(time.time() * 1000),
                            },
                            default=str,
                        )
                        + "\n"
                    )
            except OSError:
                pass
            # #endregion
            raise
        _emit_cup_importer_domain_event(
            kwargs,
            event_type=CupImporterEventType.PIT_STOPS_IMPORTED,
            entity_tables=("pitstops", "cars_drivers", "data_sources"),
            payload={
                "week_id": week_id,
                "event_number": event_number,
                "season_year": year,
                "series_id": series.series_id,
                "table_suffix": suffix,
                "data_source": source.id,
            },
        )


def load_stage_draftkings_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = (
        _sql_root() / "nascar.cup.importer" / "load_draftkings_csv_into_staging_table.sql"
    )
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "draftkings.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "season_id": season_id, "draftkings_csv_file_path": csv, "table_suffix": suffix, "frcs_source_name": FrcsSource().id, "racing_reference_source_name": RacingReferenceSource().id},
                source.id,
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.DRAFTKINGS_STAGED,
                entity_tables=("stg_frcs_draftkings",),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source.id,
                },
            )


def import_draftkings(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.importer" / "import_draftkings_staging_into_db.sql"
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "draftkings.csv")
        stg = f"stg_frcs_draftkings_{week_id}_{suffix}"
        if not os.path.isfile(csv) and not _nascar_staging_table_exists(hook, stg):
            continue
        _execute_templated_sql_file(
            hook,
            sql,
            {
                "week_id": week_id,
                "source_name": source.id,
                "source_url": source.base_url,
                "table_suffix": suffix,
            },
            source.id,
        )
        _emit_cup_importer_domain_event(
            kwargs,
            event_type=CupImporterEventType.DRAFTKINGS_IMPORTED,
            entity_tables=("dfs", "dfs_salaries", "cars_drivers", "data_sources"),
            payload={
                "week_id": week_id,
                "event_number": event_number,
                "season_year": year,
                "series_id": series.series_id,
                "table_suffix": suffix,
                "data_source": source.id,
            },
        )


def load_stage_fanduel_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = (
        _sql_root() / "nascar.cup.importer" / "load_fanduel_csv_into_staging_table.sql"
    )
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "fanduel.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "season_id": season_id, "fanduel_csv_file_path": csv, "table_suffix": suffix, "frcs_source_name": FrcsSource().id, "racing_reference_source_name": RacingReferenceSource().id},
                source.id,
            )
            _emit_cup_importer_domain_event(
                kwargs,
                event_type=CupImporterEventType.FANDUEL_STAGED,
                entity_tables=("stg_frcs_fanduel",),
                payload={
                    "week_id": week_id,
                    "event_number": event_number,
                    "season_year": year,
                    "series_id": series.series_id,
                    "table_suffix": suffix,
                    "data_source": source.id,
                },
            )


def import_fanduel(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.importer" / "import_fanduel_staging_into_db.sql"
    for week_id, season_id, event_number, _ in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "fanduel.csv")
        stg = f"stg_frcs_fanduel_{week_id}_{suffix}"
        if not os.path.isfile(csv) and not _nascar_staging_table_exists(hook, stg):
            continue
        _execute_templated_sql_file(
            hook,
            sql,
            {
                "week_id": week_id,
                "source_name": source.id,
                "source_url": source.base_url,
                "table_suffix": suffix,
            },
            source.id,
        )
        _emit_cup_importer_domain_event(
            kwargs,
            event_type=CupImporterEventType.FANDUEL_IMPORTED,
            entity_tables=("dfs", "dfs_salaries", "cars_drivers", "data_sources"),
            payload={
                "week_id": week_id,
                "event_number": event_number,
                "season_year": year,
                "series_id": series.series_id,
                "table_suffix": suffix,
                "data_source": source.id,
            },
        )


def bootstrap_shared_stg_car_tables(**kwargs: Any) -> None:
    """
    Ensure shared resolver inputs exist for every target week/season for this run suffix.
    """
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    created_or_owned: set[str] = set()
    for week_id, season_id, _, _ in _pull_target_weeks(kwargs):
        week_table = _stg_car_table_name(week_id, suffix)
        season_table = _stg_car_season_table_name(season_id, suffix)
        created_or_owned.add(week_table)
        created_or_owned.add(season_table)
        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS `nascar`.`{week_table}` (
                `car_number` VARCHAR(255) NULL,
                `driver_name` VARCHAR(255) NULL,
                `manufacturer_name` VARCHAR(255) NULL,
                `team_name` VARCHAR(255) NULL,
                `data_source_id` BIGINT UNSIGNED NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
            """
        )
        hook.run(
            f"""
            CREATE TABLE IF NOT EXISTS `nascar`.`{season_table}` (
                `week_id` BIGINT UNSIGNED NOT NULL,
                `car_number` VARCHAR(255) NULL,
                `driver_name` VARCHAR(255) NULL,
                `manufacturer_name` VARCHAR(255) NULL,
                `team_name` VARCHAR(255) NULL,
                `data_source_id` BIGINT UNSIGNED NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
            """
        )
    kwargs["ti"].xcom_push(
        key="importer_owned_tables",
        value=sorted(created_or_owned),
    )


def _assert_resolver_staging_ready(
    hook: NascarMySqlHook, *, week_id: int, season_id: int, suffix: str
) -> None:
    week_table = _stg_car_table_name(week_id, suffix)
    season_table = _stg_car_season_table_name(season_id, suffix)
    if not _table_exists(hook, week_table):
        raise ValueError(
            f"Resolver preflight failed: missing nascar.{week_table} (week_id={week_id}, suffix={suffix})"
        )
    if not _table_exists(hook, season_table):
        raise ValueError(
            f"Resolver preflight failed: missing nascar.{season_table} (season_id={season_id}, suffix={suffix})"
        )


def resolve_drivers_cars_manufacturers_teams(**kwargs) -> None:
    """Resolve drivers, teams, manufacturers, cars, and cars_drivers; backfill missing car_driver_id."""
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    suffix = _importer_table_suffix(kwargs)
    frcs = FrcsSource()
    rr = RacingReferenceSource()
    sql = (
        _sql_root()
        / "nascar.cup.importer"
        / "resolve_drivers_cars_manufacturers_teams.sql"
    )
    for week_id, season_id, _, _ in _pull_target_weeks(kwargs):
        _assert_resolver_staging_ready(
            hook,
            week_id=week_id,
            season_id=season_id,
            suffix=suffix,
        )
        _execute_templated_sql_file(
            hook,
            sql,
            {
                "week_id": week_id,
                "season_id": season_id,
                "series_id": series.series_id,
                "season_year": year,
                "table_suffix": suffix,
                "frcs_source_name": frcs.id,
                "racing_reference_source_name": rr.id,
            },
            "cup_importer_resolver",
        )


def cleanup_importer_staging_and_temp_tables(**kwargs: Any) -> None:
    """
    Drop importer staging/temp tables for **this DAG run** only, identified by
    ``table_suffix`` (``_importer_table_suffix`` / schedule load anchor).
    """
    suffix = _importer_table_suffix(kwargs)
    # #region agent log
    try:
        dis = kwargs.get("data_interval_start")
        log_anchor = {
            "sessionId": "0954fc",
            "hypothesisId": "H1",
            "location": "importer.py:cleanup_importer_staging_and_temp_tables",
            "message": "resolved_suffix_and_context_times",
            "data": {
                "suffix": suffix,
                "suffix_valid": bool(_TABLE_SUFFIX_RE.fullmatch(suffix)),
                "data_interval_start": dis.isoformat() if dis is not None else None,
                "logical_date": kwargs.get("logical_date").isoformat()
                if kwargs.get("logical_date") is not None
                else None,
                "run_id": getattr(kwargs.get("dag_run"), "run_id", None),
            },
            "timestamp": int(time.time() * 1000),
        }
        with open(
            "/home/jackalope/bin/.cursor/debug-0954fc.log", "a", encoding="utf-8"
        ) as _agent_dbg:
            _agent_dbg.write(json.dumps(log_anchor) + "\n")
    except OSError:
        pass
    # #endregion
    if not _TABLE_SUFFIX_RE.fullmatch(suffix):
        raise ValueError(f"refusing cleanup: invalid table_suffix {suffix!r}")

    like_patterns = (
        "stg_frcs_%",
        "stg_car_%",
        "stg_car_season_%",
        "stg_cup_frcs_schedule_import_%",
        "stg_cup_racing_reference_schedule_import_%",
        "tmp_cup_schedule_frcs_%",
        "tmp_cup_schedule_rr_%",
        "tmp_cup_schedule_rr_match_guard_%",
        "tmp_cup_schedule_merged_%",
        "tmp_cup_schedule_%",
        "tmp_car_%",
        "tmp_car_season_%",
        "tmp_preload_driver_car_wide_%",
        "tmp_driver_car_map_%",
        "tmp_practice_%",
        "tmp_qualifying_%",
        "tmp_race_%",
    )
    or_clauses = " OR ".join("table_name LIKE %s" for _ in like_patterns)
    list_sql = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'nascar'
      AND ({or_clauses})
    ORDER BY table_name
    """
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    rows = hook.get_records(list_sql, parameters=tuple(like_patterns)) or []
    # #region agent log
    try:
        raw_names: list[str] = []
        if rows:
            for row in rows:
                if not row:
                    continue
                r0 = row[0]
                raw_names.append(r0 if isinstance(r0, str) else str(r0))
        with open(
            "/home/jackalope/bin/.cursor/debug-0954fc.log", "a", encoding="utf-8"
        ) as _agent_dbg:
            _agent_dbg.write(
                json.dumps(
                    {
                        "sessionId": "0954fc",
                        "hypothesisId": "H2",
                        "location": "importer.py:cleanup_importer_staging_and_temp_tables",
                        "message": "information_schema_candidates",
                        "data": {
                            "row_count": len(rows),
                            "sample_table_names": raw_names[:30],
                        },
                        "timestamp": int(time.time() * 1000),
                    }
                )
                + "\n"
            )
    except OSError:
        pass
    # #endregion
    tail = f"_{suffix}"
    names: list[str] = []
    skipped_suffix: list[str] = []
    skipped_prefix: list[str] = []
    for row in rows:
        if not row:
            continue
        raw = row[0]
        name = raw if isinstance(raw, str) else str(raw)
        if not name.endswith(tail):
            if len(skipped_suffix) < 15:
                skipped_suffix.append(name)
            continue
        if not any(name.startswith(p) for p in _IMPORTER_STAGING_PREFIXES):
            if len(skipped_prefix) < 15:
                skipped_prefix.append(name)
            continue
        if not _TABLE_NAME_RE.match(name):
            _LOG.warning("Cup importer cleanup: skipping invalid table name %r", name)
            continue
        names.append(name)

    # #region agent log
    try:
        with open(
            "/home/jackalope/bin/.cursor/debug-0954fc.log", "a", encoding="utf-8"
        ) as _agent_dbg:
            _agent_dbg.write(
                json.dumps(
                    {
                        "sessionId": "0954fc",
                        "hypothesisId": "H3",
                        "location": "importer.py:cleanup_importer_staging_and_temp_tables",
                        "message": "after_suffix_and_prefix_filters",
                        "data": {
                            "tail": tail,
                            "names_to_drop_count": len(names),
                            "names_to_drop": names[:40],
                            "skipped_suffix_sample": skipped_suffix,
                            "skipped_prefix_sample": skipped_prefix,
                        },
                        "timestamp": int(time.time() * 1000),
                    }
                )
                + "\n"
            )
    except OSError:
        pass
    # #endregion

    if not names:
        _LOG.info(
            "Cup importer cleanup: no tables for this run (suffix=%s); nothing to drop", suffix
        )
        return

    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        for name in names:
            drop_sql = f"DROP TABLE IF EXISTS `nascar`.`{name}`"
            _LOG.info("Cup importer cleanup: %s", drop_sql)
            cur.execute(drop_sql)
        conn.commit()
        # #region agent log
        try:
            with open(
                "/home/jackalope/bin/.cursor/debug-0954fc.log", "a", encoding="utf-8"
            ) as _agent_dbg:
                _agent_dbg.write(
                    json.dumps(
                        {
                            "sessionId": "0954fc",
                            "hypothesisId": "H4",
                            "location": "importer.py:cleanup_importer_staging_and_temp_tables",
                            "message": "drop_statements_committed",
                            "data": {"dropped_count": len(names)},
                            "timestamp": int(time.time() * 1000),
                        }
                    )
                    + "\n"
                )
        except OSError:
            pass
        # #endregion
    finally:
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()
        finally:
            cur.close()
            conn.close()


def _register_cup_importer_dag(
    *,
    dag_id: str,
    schedule: str,
    start_date: datetime,
    end_date: datetime | None,
    catchup: bool,
    doc_md: str,
    wait_frcs_race_week_dag_id: str,
    wait_rr_race_week_dag_id: str,
) -> DAG:
    with DAG(
        dag_id=dag_id,
        start_date=start_date,
        end_date=end_date,
        schedule=schedule,
        catchup=catchup,
        max_active_runs=1,
        doc_md=doc_md,
    ) as dag:
        _build_cup_importer_task_graph(
            dag,
            wait_frcs_race_week_dag_id=wait_frcs_race_week_dag_id,
            wait_rr_race_week_dag_id=wait_rr_race_week_dag_id,
        )
    return dag


def _build_cup_importer_task_graph(
    dag: DAG,
    *,
    wait_frcs_race_week_dag_id: str,
    wait_rr_race_week_dag_id: str,
) -> None:
    wait_frcs_race_week_task = ExternalTaskSensor(
        dag=dag,
        task_id="wait_frcs_race_week_sync_race_results",
        external_dag_id=wait_frcs_race_week_dag_id,
        external_task_id="sync_race_results",
        mode="reschedule",
        poke_interval=CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
        timeout=CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
        check_existence=True,
    )
    wait_rr_race_week_task = ExternalTaskSensor(
        dag=dag,
        task_id="wait_racing_reference_race_week_sync_race_results",
        external_dag_id=wait_rr_race_week_dag_id,
        external_task_id="sync_race_results",
        mode="reschedule",
        poke_interval=CUP_EXTERNAL_SENSOR_POKE_INTERVAL,
        timeout=CUP_IMPORTER_WAIT_RACE_WEEK_TIMEOUT,
        check_existence=True,
    )

    load_stage_schedule_csvs_task = MySQLTemplateFileOperatorFromContext(
        dag=dag,
        task_id="load_stage_schedule_csvs",
        sql_file_path=_sql_root() / "nascar.cup.importer" / "load_stage_schedule_csvs.sql",
        mysql_conn_id="nascar_conn",
        parameters_factory=_importer_params_load_schedule,
        on_success_callback=_emit_cup_schedule_staged_domain_event,
    )

    import_schedule_task = MySQLTemplateFileOperatorFromContext(
        dag=dag,
        task_id="import_schedule",
        sql_file_path=_sql_root() / "nascar.cup.importer" / "import_schedule.sql",
        mysql_conn_id="nascar_conn",
        parameters_factory=_importer_params_import_schedule,
        on_success_callback=_emit_cup_schedule_imported_domain_event,
    )

    load_stage_practices_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_practices_csvs",
        python_callable=load_stage_practices_csvs,
    )
    import_practices_task = PythonOperator(
        dag=dag,
        task_id="import_practices",
        python_callable=import_practices,
    )
    load_stage_qualifyings_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_qualifyings_csvs",
        python_callable=load_stage_qualifyings_csvs,
    )
    import_qualifyings_task = PythonOperator(
        dag=dag,
        task_id="import_qualifyings",
        python_callable=import_qualifyings,
    )
    load_stage_lineup_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_lineup_csvs",
        python_callable=load_stage_lineup_csvs,
    )
    load_stage_entrylist_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_entrylist_csvs",
        python_callable=load_stage_entrylist_csvs,
    )
    import_lineup_task = PythonOperator(
        dag=dag,
        task_id="import_lineup",
        python_callable=import_lineup,
    )
    load_stage_races_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_races_csvs",
        python_callable=load_stage_races_csvs,
    )
    import_races_task = PythonOperator(
        dag=dag,
        task_id="import_races",
        python_callable=import_races,
    )
    load_stage_penalties_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_penalties_csvs",
        python_callable=load_stage_penalties_csvs,
    )
    import_penalties_task = PythonOperator(
        dag=dag,
        task_id="import_penalties",
        python_callable=import_penalties,
    )
    load_stage_pitstops_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_pitstops_csvs",
        python_callable=load_stage_pitstops_csvs,
    )
    import_pitstops_task = PythonOperator(
        dag=dag,
        task_id="import_pitstops",
        python_callable=import_pitstops,
    )
    load_stage_draftkings_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_draftkings_csvs",
        python_callable=load_stage_draftkings_csvs,
    )
    import_draftkings_task = PythonOperator(
        dag=dag,
        task_id="import_draftkings",
        python_callable=import_draftkings,
    )
    load_stage_fanduel_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_fanduel_csvs",
        python_callable=load_stage_fanduel_csvs,
    )
    bootstrap_shared_stg_car_tables_task = PythonOperator(
        dag=dag,
        task_id="bootstrap_shared_stg_car_tables",
        python_callable=bootstrap_shared_stg_car_tables,
    )
    import_fanduel_task = PythonOperator(
        dag=dag,
        task_id="import_fanduel",
        python_callable=import_fanduel,
    )
    resolve_drivers_cars_manufacturers_teams_task = PythonOperator(
        dag=dag,
        task_id="resolve_drivers_cars_manufacturers_teams",
        python_callable=resolve_drivers_cars_manufacturers_teams,
    )

    cleanup_importer_staging_task = PythonOperator(
        dag=dag,
        task_id="cleanup_importer_staging_and_temp_tables",
        python_callable=cleanup_importer_staging_and_temp_tables,
    )

    wait_frcs_race_week_task >> load_stage_schedule_csvs_task
    wait_rr_race_week_task >> load_stage_schedule_csvs_task

    (
        load_stage_schedule_csvs_task
        >> import_schedule_task
        >> load_stage_practices_csvs_task
        >> load_stage_qualifyings_csvs_task
        >> load_stage_lineup_csvs_task
        >> load_stage_entrylist_csvs_task
        >> load_stage_races_csvs_task
        >> load_stage_penalties_csvs_task
        >> load_stage_pitstops_csvs_task
        >> load_stage_draftkings_csvs_task
        >> load_stage_fanduel_csvs_task
        >> bootstrap_shared_stg_car_tables_task
        >> resolve_drivers_cars_manufacturers_teams_task
        >> import_practices_task
        >> import_qualifyings_task
        >> import_lineup_task
        >> import_races_task
        >> import_penalties_task
        >> import_pitstops_task
        >> import_draftkings_task
        >> import_fanduel_task
        >> cleanup_importer_staging_task
    )


dag = _register_cup_importer_dag(
    dag_id=DAG_ID,
    schedule=CUP_SCRAPER_PROD_HOURLY_CRON,
    start_date=datetime(2025, 7, 11),
    end_date=None,
    catchup=False,
    doc_md=__doc__ or "",
    wait_frcs_race_week_dag_id="nascar.cup.scraper.frcs.race_week",
    wait_rr_race_week_dag_id="nascar.cup.scraper.racing_reference.race_week",
)

backfill_dag: DAG | None = maybe_register_backfill_dag(
    _register_cup_importer_dag,
    dag_id=f"backfill.{DAG_ID}",
    schedule=BACKFILL_YEARLY_SCHEDULE,
    start_date=BACKFILL_START_DATE,
    end_date=BACKFILL_END_DATE,
    catchup=True,
    doc_md=(
        "Yearly backfill of the Cup importer (2010–2025), same cron as other "
        "``backfill.*`` Cup DAGs (Dec 31). ``wait_*`` tasks require both backfill "
        "race-week DAGs' ``sync_race_results`` for the same logical date. Uses the "
        "same load/import tasks as ``nascar.cup.importer``."
    ),
    wait_frcs_race_week_dag_id="backfill.nascar.cup.scraper.frcs.race_week",
    wait_rr_race_week_dag_id="backfill.nascar.cup.scraper.racing_reference.race_week",
)
