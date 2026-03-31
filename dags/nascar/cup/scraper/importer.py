"""
Top-level Cup scraper importer DAG.
"""

from __future__ import annotations

import glob
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from jinja2 import Template
from nascar.cup.scraper.frcs.race_week import NASCAR_FRCS_CUP_RACE_WEEK_ASSET
from nascar.cup.scraper.racing_reference.race_week import (
    NASCAR_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
)

from airflow_utils import MySQLTemplateFileOperator
from include.nascar.data_sources.frcs import FrcsSource
from include.nascar.data_sources.racing_reference import RacingReferenceSource
from include.nascar.data_sources.racing_reference_race_week import (
    _cup_series_schedule_year_from_context,
)
from include.nascar.enums import NascarSeries

DAG_ID = "nascar.cup.scraper.importer"


def _sql_root() -> Path:
    return Path(__file__).parents[4] / "sql"


def _render_sql(sql_file_path: Path, context: dict[str, object]) -> str:
    with open(sql_file_path, encoding="utf-8") as f:
        return Template(f.read()).render(context)


def _execute_templated_sql_file(
    hook: MySqlHook, sql_file_path: Path, context: dict[str, object], source_name: str
) -> None:
    rendered_sql = _render_sql(sql_file_path, {**context, "source_name": source_name})
    for stmt in [s.strip() for s in rendered_sql.split("---") if s.strip()]:
        hook.run(stmt)


def _target_weeks(hook: MySqlHook, *, series_id: int, year: int) -> list[tuple[int, int]]:
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


def load_stage_frcs_entrylist_csvs(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                hook, stage_sql, {"week_id": week_id, "entrylist_csv_file_path": csv}, source.id
            )


def import_frcs_entrylist(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                {"week_id": week_id, "series_id": series.series_id},
                source.id,
            )


def load_stage_frcs_practices_csvs(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                    {"week_id": week_id, "number": int(match.group(1)), "practice_csv_file_path": csv},
                    source.id,
                )


def import_frcs_practices(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                    },
                    source.id,
                )


def load_stage_frcs_qualifyings_csvs(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                hook, stage_sql, {"week_id": week_id, "qualifying_csv_file_path": csv}, source.id
            )


def import_frcs_qualifyings(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                {"week_id": week_id, "series_id": series.series_id},
                source.id,
            )


def load_stage_frcs_lineup_csvs(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_lineup_csv_into_staging_table.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "lineup.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook, stage_sql, {"week_id": week_id, "lineup_csv_file_path": csv}, source.id
            )


def import_frcs_lineup(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_lineup_staging_into_db.sql"
    source = FrcsSource()
    for week_id, _ in _pull_target_weeks(kwargs):
        _execute_templated_sql_file(
            hook,
            sql,
            {"week_id": week_id, "series_id": NascarSeries.CUP.series_id, "source_name": source.id},
            source.id,
        )


def load_stage_frcs_races_csvs(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                },
                source.id,
            )


def import_frcs_races(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                },
                source.id,
            )


def load_stage_frcs_penalties_csvs(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
    year = _context_year(kwargs)
    series = NascarSeries.CUP
    source = FrcsSource()
    stage_sql = _sql_root() / "nascar.cup.scraper.importer" / "load_frcs_penalties_csv_into_staging_table.sql"
    for week_id, event_number in _pull_target_weeks(kwargs):
        csv = os.path.join(source.storage_path_week(series.db_name, year, event_number), "penalties.csv")
        if os.path.isfile(csv):
            _execute_templated_sql_file(
                hook, stage_sql, {"week_id": week_id, "penalties_csv_file_path": csv}, source.id
            )


def import_frcs_penalties(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_penalties_staging_into_db.sql"
    source = FrcsSource()
    for week_id, _ in _pull_target_weeks(kwargs):
        _execute_templated_sql_file(
            hook, sql, {"week_id": week_id, "source_name": source.id}, source.id
        )


def load_stage_frcs_pitstops_csvs(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                hook, stage_sql, {"week_id": week_id, "pit_stops_detail_csv_file_path": csv}, source.id
            )


def import_frcs_pitstops(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_pit_stops_detail_staging_into_db.sql"
    source = FrcsSource()
    for week_id, _ in _pull_target_weeks(kwargs):
        _execute_templated_sql_file(
            hook, sql, {"week_id": week_id, "source_name": source.id}, source.id
        )


def load_stage_frcs_draftkings_csvs(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                hook, stage_sql, {"week_id": week_id, "draftkings_csv_file_path": csv}, source.id
            )


def import_frcs_draftkings(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_draftkings_staging_into_db.sql"
    for week_id, _ in _pull_target_weeks(kwargs):
        _execute_templated_sql_file(
            hook,
            sql,
            {"week_id": week_id, "source_name": source.id, "source_url": source.base_url},
            source.id,
        )


def load_stage_frcs_fanduel_csvs(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
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
                hook, stage_sql, {"week_id": week_id, "fanduel_csv_file_path": csv}, source.id
            )


def import_frcs_fanduel(**kwargs) -> None:
    hook = MySqlHook(mysql_conn_id="nascar_conn")
    source = FrcsSource()
    sql = _sql_root() / "nascar.cup.scraper.importer" / "import_frcs_fanduel_staging_into_db.sql"
    for week_id, _ in _pull_target_weeks(kwargs):
        _execute_templated_sql_file(
            hook,
            sql,
            {"week_id": week_id, "source_name": source.id, "source_url": source.base_url},
            source.id,
        )


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 7, 11),
    schedule=[
        NASCAR_RACING_REFERENCE_CUP_RACE_WEEK_ASSET,
        NASCAR_FRCS_CUP_RACE_WEEK_ASSET,
    ],
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    frcs_source = FrcsSource()
    racing_reference_source = RacingReferenceSource()

    load_stage_frcs_schedule_csv_task = MySQLTemplateFileOperator(
        task_id="load_stage_frcs_schedule_csv",
        sql_file_path=Path(__file__).parents[4]
        / "sql"
        / "nascar.cup.scraper.importer"
        / "load_stage_frcs_schedule_csv.sql",
        mysql_conn_id="nascar_conn",
        parameters={
            "schedule_csv_file_path": (
                f"{frcs_source.storage_path_series_year('cup', '{{ (data_interval_start or logical_date).year }}')}/schedule.csv"
            ),
            "table_suffix": "{{ (data_interval_start or logical_date).strftime('%Y%m%d%H%M') }}",
        },
    )

    load_stage_racing_reference_schedule_csv_task = MySQLTemplateFileOperator(
        task_id="load_stage_racing_reference_schedule_csv",
        sql_file_path=Path(__file__).parents[4]
        / "sql"
        / "nascar.cup.scraper.importer"
        / "load_racing_reference_schedule_data_into_temp_table.sql",
        mysql_conn_id="nascar_conn",
        parameters={
            "schedule_csv_file_path": (
                f"{racing_reference_source.storage_path_series_year('cup', '{{ (data_interval_start or logical_date).year }}')}/schedule.csv"
            ),
            "table_suffix": "{{ (data_interval_start or logical_date).strftime('%Y%m%d%H%M') }}",
        },
    )

    import_frcs_schedule_task = MySQLTemplateFileOperator(
        task_id="import_frcs_schedule",
        sql_file_path=Path(__file__).parents[4]
        / "sql"
        / "nascar.cup.scraper.importer"
        / "import_frcs_schedule.sql",
        mysql_conn_id="nascar_conn",
        parameters={
            "series_id": NascarSeries.CUP.series_id,
            "series_name": NascarSeries.CUP.db_name,
            "source_name": frcs_source.id,
            "source_url": frcs_source.base_url,
            "season_year": "{{ (data_interval_start or logical_date).year }}",
            "table_suffix": "{{ (data_interval_start or logical_date).strftime('%Y%m%d%H%M') }}",
        },
    )

    load_stage_frcs_entrylist_csvs_task = PythonOperator(
        task_id="load_stage_frcs_entrylist_csvs",
        python_callable=load_stage_frcs_entrylist_csvs,
    )
    import_frcs_entrylist_task = PythonOperator(
        task_id="import_frcs_entrylist",
        python_callable=import_frcs_entrylist,
    )
    load_stage_frcs_practices_csvs_task = PythonOperator(
        task_id="load_stage_frcs_practices_csvs",
        python_callable=load_stage_frcs_practices_csvs,
    )
    import_frcs_practices_task = PythonOperator(
        task_id="import_frcs_practices",
        python_callable=import_frcs_practices,
    )
    load_stage_frcs_qualifyings_csvs_task = PythonOperator(
        task_id="load_stage_frcs_qualifyings_csvs",
        python_callable=load_stage_frcs_qualifyings_csvs,
    )
    import_frcs_qualifyings_task = PythonOperator(
        task_id="import_frcs_qualifyings",
        python_callable=import_frcs_qualifyings,
    )
    load_stage_frcs_lineup_csvs_task = PythonOperator(
        task_id="load_stage_frcs_lineup_csvs",
        python_callable=load_stage_frcs_lineup_csvs,
    )
    import_frcs_lineup_task = PythonOperator(
        task_id="import_frcs_lineup",
        python_callable=import_frcs_lineup,
    )
    load_stage_frcs_races_csvs_task = PythonOperator(
        task_id="load_stage_frcs_races_csvs",
        python_callable=load_stage_frcs_races_csvs,
    )
    import_frcs_races_task = PythonOperator(
        task_id="import_frcs_races",
        python_callable=import_frcs_races,
    )
    load_stage_frcs_penalties_csvs_task = PythonOperator(
        task_id="load_stage_frcs_penalties_csvs",
        python_callable=load_stage_frcs_penalties_csvs,
    )
    import_frcs_penalties_task = PythonOperator(
        task_id="import_frcs_penalties",
        python_callable=import_frcs_penalties,
    )
    load_stage_frcs_pitstops_csvs_task = PythonOperator(
        task_id="load_stage_frcs_pitstops_csvs",
        python_callable=load_stage_frcs_pitstops_csvs,
    )
    import_frcs_pitstops_task = PythonOperator(
        task_id="import_frcs_pitstops",
        python_callable=import_frcs_pitstops,
    )
    load_stage_frcs_draftkings_csvs_task = PythonOperator(
        task_id="load_stage_frcs_draftkings_csvs",
        python_callable=load_stage_frcs_draftkings_csvs,
    )
    import_frcs_draftkings_task = PythonOperator(
        task_id="import_frcs_draftkings",
        python_callable=import_frcs_draftkings,
    )
    load_stage_frcs_fanduel_csvs_task = PythonOperator(
        task_id="load_stage_frcs_fanduel_csvs",
        python_callable=load_stage_frcs_fanduel_csvs,
    )
    import_frcs_fanduel_task = PythonOperator(
        task_id="import_frcs_fanduel",
        python_callable=import_frcs_fanduel,
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
    )
