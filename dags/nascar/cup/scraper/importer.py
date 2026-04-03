"""
Top-level Cup scraper importer DAG.

Week-scoped staging/temp tables include ``table_suffix`` (run anchor, same
``strftime('%Y%m%d%H%M')`` as schedule staging) so each DAG run only touches its own
tables. The final task
``cleanup_importer_staging_and_temp_tables`` drops only tables whose names end with that
suffix (plus the schedule import tables for this run).
"""

from __future__ import annotations

import glob
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset, DAG
from jinja2 import Template
from nascar.cup.scraper.backfill_assets import (
    BACKFILL_END_DATE,
    BACKFILL_START_DATE,
    BACKFILL_YEARLY_SCHEDULE,
)
from nascar.cup.scraper.frcs.race_week import NASCAR_FRCS_CUP_RACE_WEEK_ASSET
from nascar.cup.scraper.racing_reference.race_week import (
    NASCAR_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
)

from airflow_utils import MySQLTemplateFileOperatorFromContext
from airflow_utils.mysql_template_file_executor import execute_sql_statements_transactional
from include.nascar.data_sources.frcs import FrcsSource
from include.nascar.data_sources.racing_reference import RacingReferenceSource
from include.nascar.data_sources.racing_reference_race_week import (
    _cup_series_schedule_year_from_context,
    race_week_run_anchor_from_context,
)
from include.nascar.enums import NascarSeries

DAG_ID = "nascar.cup.scraper.importer"

_SQL_DAG_SUBDIR = "nascar.cup.scraper.importer"
_SQL_SENTINEL = Path(_SQL_DAG_SUBDIR) / "load_stage_frcs_schedule_csv.sql"

_LOG = logging.getLogger(__name__)

# Table names from information_schema must match before DROP (defense in depth).
_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
_IMPORTER_STAGING_PREFIXES = (
    "stg_cup_frcs_schedule_import_",
    "stg_cup_racing_reference_schedule_import_",
    "stg_frcs_",
    "temp_entrylist_",
    "temp_practice_",
    "temp_qualifying_",
    "temp_race_",
)
_TABLE_SUFFIX_RE = re.compile(r"^\d{12}$")


def _sql_root() -> Path:
    """
    Resolve the SQL root that contains the ``nascar.cup.scraper.importer`` folder.

    Canonical layout: ``dags/sql/nascar.cup.scraper.importer/`` (top of the DAGs folder).
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
    # ``importer.py`` lives at ``dags/nascar/cup/scraper/`` → ``parents[3]`` is ``dags/``.
    candidates = [
        dag_file.parents[3] / "sql",
        dag_file.parent / "sql",
        dag_file.parents[4] / "sql",
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


def _execute_templated_sql_file(
    hook: NascarMySqlHook, sql_file_path: Path, context: dict[str, object], source_name: str
) -> None:
    """
    Run the file's ``---``-separated statements on one connection with commit/rollback.

    See :func:`airflow_utils.mysql_template_file_executor.execute_sql_statements_transactional`
    for MySQL DDL / implicit-commit limitations.
    """
    rendered_sql = _render_sql(sql_file_path, {**context, "source_name": source_name})
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
    All Cup ``weeks`` rows for the season (see ``select_frcs_weeks_needing_csv_import.sql``).

    Post-race CSVs must still be staged after ``race_results`` exist, so the list is not
    limited to weeks missing finish data.
    """
    sql = _render_sql(
        _sql_root() / "nascar.cup.scraper.importer" / "select_frcs_weeks_needing_csv_import.sql",
        {"series_id": series_id, "year": year},
    )
    rows = hook.get_records(sql)
    return [(int(r[0]), int(r[1])) for r in rows]


def _push_target_weeks(kwargs: dict[str, Any], weeks: list[tuple[int, int]]) -> None:
    kwargs["ti"].xcom_push(
        key="frcs_target_weeks",
        value=[{"week_id": w, "event_number": e} for w, e in weeks],
    )


def _pull_target_weeks(kwargs: dict[str, Any]) -> list[tuple[int, int]]:
    rows = kwargs["ti"].xcom_pull(
        task_ids="load_stage_frcs_entrylist_csvs", key="frcs_target_weeks"
    ) or []
    return [(int(r["week_id"]), int(r["event_number"])) for r in rows]


def _context_year(kwargs: dict[str, Any]) -> int:
    return _cup_series_schedule_year_from_context(kwargs)


def _importer_table_suffix(context: dict[str, Any]) -> str:
    """Matches ``table_suffix`` used in schedule load/import (per DAG run)."""
    return race_week_run_anchor_from_context(context).strftime("%Y%m%d%H%M")


def _importer_params_load_frcs_schedule(context: dict[str, Any]) -> dict[str, Any]:
    year = _cup_series_schedule_year_from_context(context)
    anchor = race_week_run_anchor_from_context(context)
    frcs = FrcsSource()
    return {
        "schedule_csv_file_path": f"{frcs.storage_path_series_year('cup', year)}/schedule.csv",
        "table_suffix": anchor.strftime("%Y%m%d%H%M"),
    }


def _importer_params_load_rr_schedule(context: dict[str, Any]) -> dict[str, Any]:
    year = _cup_series_schedule_year_from_context(context)
    anchor = race_week_run_anchor_from_context(context)
    rr = RacingReferenceSource()
    return {
        "schedule_csv_file_path": f"{rr.storage_path_series_year('cup', year)}/schedule.csv",
        "table_suffix": anchor.strftime("%Y%m%d%H%M"),
    }


def _importer_params_import_frcs_schedule(context: dict[str, Any]) -> dict[str, Any]:
    year = _cup_series_schedule_year_from_context(context)
    anchor = race_week_run_anchor_from_context(context)
    frcs = FrcsSource()
    return {
        "series_id": NascarSeries.CUP.series_id,
        "series_name": NascarSeries.CUP.db_name,
        "source_name": frcs.id,
        "source_url": frcs.base_url,
        "season_year": year,
        "table_suffix": anchor.strftime("%Y%m%d%H%M"),
    }


def load_stage_frcs_entrylist_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    weeks = _target_weeks(hook, series_id=series.series_id, year=year)
    _push_target_weeks(kwargs, weeks)
    stage_sql = (
        _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_entrylist_csv_into_staging_table.sql"
    )
    for week_id, event_number in weeks:
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "entrylist.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "entrylist_csv_file_path": csv, "table_suffix": suffix},
                source.id,
            )


def import_frcs_entrylist(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_entrylist_staging_into_db.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "entrylist.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                sql,
                {"week_id": week_id, "series_id": series.series_id, "table_suffix": suffix},
                source.id,
            )


def load_stage_frcs_practices_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = (
        _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_practice_csv_into_staging_table.sql"
    )
    for week_id, event_number in _pull_target_weeks(kwargs):
        for csv in sorted(
            glob.glob(os.path.join(source.storage_path_week(series.db_name, year, event_number), "practice-*.csv"))
        ):
            match = re.search(r"practice-(\d+)\.csv$", csv)
            if match:
                _execute_templated_sql_file(
                    hook,
                    stage_sql,
                    {
                        "week_id": week_id,
                        "number": int(match.group(1)),
                        "practice_csv_file_path": csv,
                        "table_suffix": suffix,
                    },
                    source.id,
                )


def import_frcs_practices(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_practice_staging_into_db.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
        for csv in sorted(
            glob.glob(os.path.join(source.storage_path_week(series.db_name, year, event_number), "practice-*.csv"))
        ):
            match = re.search(r"practice-(\d+)\.csv$", csv)
            if match:
                _execute_templated_sql_file(
                    hook,
                    sql,
                    {
                        "week_id": week_id,
                        "series_id": series.series_id,
                        "number": int(match.group(1)),
                        "table_suffix": suffix,
                    },
                    source.id,
                )


def load_stage_frcs_qualifyings_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = (
        _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_qualifying_csv_into_staging_table.sql"
    )
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "qualifying.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "qualifying_csv_file_path": csv, "table_suffix": suffix},
                source.id,
            )


def import_frcs_qualifyings(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_qualifying_staging_into_db.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "qualifying.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                sql,
                {"week_id": week_id, "series_id": series.series_id, "table_suffix": suffix},
                source.id,
            )


def load_stage_frcs_lineup_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_lineup_csv_into_staging_table.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "lineup.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "lineup_csv_file_path": csv, "table_suffix": suffix},
                source.id,
            )


def import_frcs_lineup(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_lineup_staging_into_db.sql"
    source = FrcsSource()
    for week_id, event_number in _pull_target_weeks(kwargs):
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


def load_stage_frcs_races_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_race_csvs_into_staging_tables.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
        base = source.storage_path_week(series.db_name, year, event_number)
        race_csv = os.path.join(base, "race-results.csv")
        loop_csv = os.path.join(base, "loop-data.csv")
        if os.path.isfile(race_csv) and os.path.isfile(loop_csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {
                    "week_id": week_id,
                    "race_results_csv_file_path": race_csv,
                    "race_loop_data_csv_file_path": loop_csv,
                    "table_suffix": suffix,
                },
                source.id,
            )


def import_frcs_races(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_race_staging_into_db.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
        base = source.storage_path_week(series.db_name, year, event_number)
        race_csv = os.path.join(base, "race-results.csv")
        loop_csv = os.path.join(base, "loop-data.csv")
        if os.path.isfile(race_csv) and os.path.isfile(loop_csv):
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


def load_stage_frcs_penalties_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_penalties_csv_into_staging_table.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "penalties.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "penalties_csv_file_path": csv, "table_suffix": suffix},
                source.id,
            )


def import_frcs_penalties(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_penalties_staging_into_db.sql"
    source = FrcsSource()
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "penalties.csv")
        stg = f"stg_frcs_penalties_{week_id}_{suffix}"
        if not os.path.isfile(csv) and not _nascar_staging_table_exists(hook, stg):
            continue
        _execute_templated_sql_file(
            hook,
            sql,
            {"week_id": week_id, "source_name": source.id, "table_suffix": suffix},
            source.id,
        )


def load_stage_frcs_pitstops_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = (
        _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_pit_stops_detail_csv_into_staging_table.sql"
    )
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(
            source.storage_path_week(series.db_name, year, event_number), "pit-stops-detail.csv"
        )
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "pit_stops_detail_csv_file_path": csv, "table_suffix": suffix},
                source.id,
            )


def import_frcs_pitstops(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_pit_stops_detail_staging_into_db.sql"
    source = FrcsSource()
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(
            source.storage_path_week(series.db_name, year, event_number), "pit-stops-detail.csv"
        )
        stg = f"stg_frcs_pit_stops_detail_{week_id}_{suffix}"
        if not os.path.isfile(csv) and not _nascar_staging_table_exists(hook, stg):
            continue
        _execute_templated_sql_file(
            hook,
            sql,
            {"week_id": week_id, "source_name": source.id, "table_suffix": suffix},
            source.id,
        )


def load_stage_frcs_draftkings_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = (
        _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_draftkings_csv_into_staging_table.sql"
    )
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "draftkings.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "draftkings_csv_file_path": csv, "table_suffix": suffix},
                source.id,
            )


def import_frcs_draftkings(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_draftkings_staging_into_db.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
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


def load_stage_frcs_fanduel_csvs(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = (
        _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_fanduel_csv_into_staging_table.sql"
    )
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "fanduel.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook,
                stage_sql,
                {"week_id": week_id, "fanduel_csv_file_path": csv, "table_suffix": suffix},
                source.id,
            )


def import_frcs_fanduel(**kwargs) -> None:
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    suffix = _importer_table_suffix(kwargs)
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_fanduel_staging_into_db.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
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


def cleanup_importer_staging_and_temp_tables(**kwargs: Any) -> None:
    """
    Drop importer staging/temp tables for **this DAG run** only, identified by
    ``table_suffix`` (``_importer_table_suffix`` / schedule load anchor).
    """
    suffix = _importer_table_suffix(kwargs)
    if not _TABLE_SUFFIX_RE.fullmatch(suffix):
        raise ValueError(f"refusing cleanup: invalid table_suffix {suffix!r}")

    like_patterns = (
        "stg_frcs_%",
        "stg_cup_frcs_schedule_import_%",
        "stg_cup_racing_reference_schedule_import_%",
        "temp_entrylist_%",
        "temp_practice_%",
        "temp_qualifying_%",
        "temp_race_%",
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
    rows = hook.get_records(list_sql, parameters=tuple(like_patterns))
    tail = f"_{suffix}"
    names: list[str] = []
    for row in rows:
        if not row:
            continue
        raw = row[0]
        name = raw if isinstance(raw, str) else str(raw)
        if not name.endswith(tail):
            continue
        if not any(name.startswith(p) for p in _IMPORTER_STAGING_PREFIXES):
            continue
        if not _TABLE_NAME_RE.match(name):
            _LOG.warning("Cup importer cleanup: skipping invalid table name %r", name)
            continue
        names.append(name)

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
    schedule: str | list[Asset],
    start_date: datetime,
    end_date: datetime | None,
    catchup: bool,
    doc_md: str,
) -> None:
    with DAG(
        dag_id=dag_id,
        start_date=start_date,
        end_date=end_date,
        schedule=schedule,
        catchup=catchup,
        max_active_runs=1,
        doc_md=doc_md,
    ) as dag:
        _build_cup_importer_task_graph(dag)


def _build_cup_importer_task_graph(dag: DAG) -> None:
    load_stage_frcs_schedule_csv_task = MySQLTemplateFileOperatorFromContext(
        dag=dag,
        task_id="load_stage_frcs_schedule_csv",
        sql_file_path=_sql_root() / "nascar.cup.scraper.importer" / "load_stage_frcs_schedule_csv.sql",
        mysql_conn_id="nascar_conn",
        parameters_factory=_importer_params_load_frcs_schedule,
    )

    load_stage_racing_reference_schedule_csv_task = MySQLTemplateFileOperatorFromContext(
        dag=dag,
        task_id="load_stage_racing_reference_schedule_csv",
        sql_file_path=_sql_root()
        / "nascar.cup.scraper.importer"
        / "load_racing_reference_schedule_data_into_temp_table.sql",
        mysql_conn_id="nascar_conn",
        parameters_factory=_importer_params_load_rr_schedule,
    )

    import_frcs_schedule_task = MySQLTemplateFileOperatorFromContext(
        dag=dag,
        task_id="import_frcs_schedule",
        sql_file_path=_sql_root() / "nascar.cup.scraper.importer" / "import_frcs_schedule.sql",
        mysql_conn_id="nascar_conn",
        parameters_factory=_importer_params_import_frcs_schedule,
    )

    load_stage_frcs_entrylist_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_frcs_entrylist_csvs",
        python_callable=load_stage_frcs_entrylist_csvs,
    )
    import_frcs_entrylist_task = PythonOperator(
        dag=dag,
        task_id="import_frcs_entrylist",
        python_callable=import_frcs_entrylist,
    )
    load_stage_frcs_practices_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_frcs_practices_csvs",
        python_callable=load_stage_frcs_practices_csvs,
    )
    import_frcs_practices_task = PythonOperator(
        dag=dag,
        task_id="import_frcs_practices",
        python_callable=import_frcs_practices,
    )
    load_stage_frcs_qualifyings_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_frcs_qualifyings_csvs",
        python_callable=load_stage_frcs_qualifyings_csvs,
    )
    import_frcs_qualifyings_task = PythonOperator(
        dag=dag,
        task_id="import_frcs_qualifyings",
        python_callable=import_frcs_qualifyings,
    )
    load_stage_frcs_lineup_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_frcs_lineup_csvs",
        python_callable=load_stage_frcs_lineup_csvs,
    )
    import_frcs_lineup_task = PythonOperator(
        dag=dag,
        task_id="import_frcs_lineup",
        python_callable=import_frcs_lineup,
    )
    load_stage_frcs_races_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_frcs_races_csvs",
        python_callable=load_stage_frcs_races_csvs,
    )
    import_frcs_races_task = PythonOperator(
        dag=dag,
        task_id="import_frcs_races",
        python_callable=import_frcs_races,
    )
    load_stage_frcs_penalties_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_frcs_penalties_csvs",
        python_callable=load_stage_frcs_penalties_csvs,
    )
    import_frcs_penalties_task = PythonOperator(
        dag=dag,
        task_id="import_frcs_penalties",
        python_callable=import_frcs_penalties,
    )
    load_stage_frcs_pitstops_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_frcs_pitstops_csvs",
        python_callable=load_stage_frcs_pitstops_csvs,
    )
    import_frcs_pitstops_task = PythonOperator(
        dag=dag,
        task_id="import_frcs_pitstops",
        python_callable=import_frcs_pitstops,
    )
    load_stage_frcs_draftkings_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_frcs_draftkings_csvs",
        python_callable=load_stage_frcs_draftkings_csvs,
    )
    import_frcs_draftkings_task = PythonOperator(
        dag=dag,
        task_id="import_frcs_draftkings",
        python_callable=import_frcs_draftkings,
    )
    load_stage_frcs_fanduel_csvs_task = PythonOperator(
        dag=dag,
        task_id="load_stage_frcs_fanduel_csvs",
        python_callable=load_stage_frcs_fanduel_csvs,
    )
    import_frcs_fanduel_task = PythonOperator(
        dag=dag,
        task_id="import_frcs_fanduel",
        python_callable=import_frcs_fanduel,
    )

    cleanup_importer_staging_task = PythonOperator(
        dag=dag,
        task_id="cleanup_importer_staging_and_temp_tables",
        python_callable=cleanup_importer_staging_and_temp_tables,
    )

    (
        load_stage_frcs_schedule_csv_task
        >> import_frcs_schedule_task
        >> load_stage_frcs_entrylist_csvs_task
        >> import_frcs_entrylist_task
        >> load_stage_frcs_practices_csvs_task
        >> import_frcs_practices_task
        >> load_stage_frcs_qualifyings_csvs_task
        >> import_frcs_qualifyings_task
        >> load_stage_frcs_lineup_csvs_task
        >> import_frcs_lineup_task
        >> load_stage_frcs_races_csvs_task
        >> import_frcs_races_task
        >> load_stage_frcs_penalties_csvs_task
        >> import_frcs_penalties_task
        >> load_stage_frcs_pitstops_csvs_task
        >> import_frcs_pitstops_task
        >> load_stage_frcs_draftkings_csvs_task
        >> import_frcs_draftkings_task
        >> load_stage_frcs_fanduel_csvs_task
        >> import_frcs_fanduel_task
        >> load_stage_racing_reference_schedule_csv_task
        >> cleanup_importer_staging_task
    )


_register_cup_importer_dag(
    dag_id=DAG_ID,
    schedule=[
        NASCAR_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
        NASCAR_FRCS_CUP_RACE_WEEK_ASSET,
    ],
    start_date=datetime(2025, 7, 11),
    end_date=None,
    catchup=False,
    doc_md=__doc__ or "",
)

_register_cup_importer_dag(
    dag_id=f"backfill.{DAG_ID}",
    schedule=BACKFILL_YEARLY_SCHEDULE,
    start_date=BACKFILL_START_DATE,
    end_date=BACKFILL_END_DATE,
    catchup=True,
    doc_md=(
        "Yearly backfill of the Cup importer (2010–2025), same cron as other "
        "``backfill.*`` Cup DAGs (Dec 31). Run after backfill schedule and "
        "race-week DAGs have populated CSVs for that ``data_interval_start.year``, "
        "or clear failed tasks and re-run. Uses the same load/import tasks as "
        "``nascar.cup.scraper.importer``."
    ),
)
