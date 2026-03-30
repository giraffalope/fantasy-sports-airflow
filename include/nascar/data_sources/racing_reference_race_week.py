"""
Cup race-week HTML sync from ``schedule.csv`` (Racing Reference).

Each callable matches one DAG task: download missing artifacts, then convert where applicable.
"""

from __future__ import annotations

import glob
import logging
import os
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from include.nascar.data_sources.racing_reference import (
    RacingReferenceSource,
    _resolve_racing_reference_source,
    download_entrylist_html_if_missing,
    download_loop_data_html_if_missing,
    download_penalties_html_if_missing,
    download_practices_html_if_missing,
    download_qualifying_html_if_missing,
    download_race_results_html_if_missing,
)
from include.nascar.enums import NascarSeries
from include.utils.file import File

_RR_CUP = NascarSeries.CUP


def _parse_iso_datetime_string(s: str) -> datetime | None:
    """Parse ISO-8601 substrings embedded in Airflow ``run_id`` values."""
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _year_from_airflow_run_id(run_id: str | None) -> int | None:
    """
    Infer calendar year from Airflow ``run_id`` strings.

    - ``scheduled__<iso>`` — cron / periodic runs
    - ``asset_triggered__<iso>_<token>`` — asset-driven runs (no ``data_interval_start``)
    """
    if not run_id:
        return None

    if run_id.startswith("scheduled__"):
        payload = run_id[len("scheduled__") :]
        dt = _parse_iso_datetime_string(payload)
        return int(dt.year) if dt is not None else None

    if run_id.startswith("asset_triggered__"):
        rest = run_id[len("asset_triggered__") :]
        if "_" not in rest:
            return None
        # ``…+00:00_oaEL2K8L`` — split off trailing opaque token
        iso_candidate = rest.rsplit("_", 1)[0]
        dt = _parse_iso_datetime_string(iso_candidate)
        return int(dt.year) if dt is not None else None

    return None


def _year_from_airflow_scheduled_run_id(run_id: str | None) -> int | None:
    """Backward-compatible name for scheduled-run id parsing only."""
    if not run_id or not run_id.startswith("scheduled__"):
        return None
    return _year_from_airflow_run_id(run_id)


def _cup_series_schedule_year_from_context(context: dict[str, Any]) -> int:
    """
    Calendar year for ``…/cup/<year>/`` paths.

    Cron DAGs expose ``data_interval_start`` / ``logical_date``. Asset-triggered DAGs
    omit those; derive the season year from ``triggering_asset_events`` (producer
    ``source_run_id`` / ``timestamp``) or from ``dag_run`` fallbacks.
    """
    dis = context.get("data_interval_start")
    if dis is not None:
        return dis.year
    logical = context.get("logical_date")
    if logical is not None:
        return logical.year

    events_map = context.get("triggering_asset_events")
    if events_map is not None:
        for events in events_map.values():
            for event in events or ():
                src = getattr(event, "source_dag_run", None)
                if src is not None:
                    src_dis = getattr(src, "data_interval_start", None)
                    if src_dis is not None:
                        return src_dis.year
                    src_logical = getattr(src, "logical_date", None)
                    if src_logical is not None:
                        return src_logical.year
                y = _year_from_airflow_run_id(getattr(event, "source_run_id", None))
                if y is not None:
                    return y
                ts = getattr(event, "timestamp", None)
                if ts is not None:
                    return ts.year

    dag_run = context.get("dag_run")
    if dag_run is not None:
        y = _year_from_airflow_run_id(getattr(dag_run, "run_id", None))
        if y is not None:
            return y
        for attr in (
            "data_interval_start",
            "logical_date",
            "run_after",
            "start_date",
        ):
            dt = getattr(dag_run, attr, None)
            if dt is not None:
                return dt.year

    ti = context.get("ti")
    if ti is not None:
        dr = getattr(ti, "dag_run", None)
        if dr is not None:
            y = _year_from_airflow_run_id(getattr(dr, "run_id", None))
            if y is not None:
                return y

    raise ValueError(
        "Cannot resolve Cup season year: no data_interval_start, logical_date, "
        "triggering_asset_events (source_run_id/timestamp), dag_run.run_id "
        "(scheduled__/asset_triggered__), or dag_run/ti date fallbacks"
    )


def _schedule_csv_path(source: RacingReferenceSource, series_name: str, year: int) -> str:
    return f"{source.storage_path_series_year(series_name, year)}/schedule.csv"


def iter_week_numbers_with_url(
    source: RacingReferenceSource, series_name: str, year: int
) -> Iterator[tuple[int, str]]:
    path = _schedule_csv_path(source, series_name, year)
    if not os.path.isfile(path):
        logging.warning("Schedule CSV missing at %s; skipping race-week sync", path)
        return
    rows = File.read_csv(filename=path, as_dict=True)
    for row in rows:
        week_url = (row.get("week_url") or "").strip()
        if not week_url:
            continue
        try:
            week_number = int(row["week_number"])
        except (KeyError, TypeError, ValueError):
            logging.warning("Bad week_number in schedule row: %s", row)
            continue
        yield week_number, week_url


def cup_series_sync_weeks(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    for week_number, week_url in iter_week_numbers_with_url(src, _RR_CUP.db_name, year):
        download_race_results_html_if_missing(src, _RR_CUP.db_name, year, week_number, week_url)


def cup_series_sync_entrylists(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    conv = src.converters
    for week_number, _ in iter_week_numbers_with_url(src, _RR_CUP.db_name, year):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        download_entrylist_html_if_missing(src, _RR_CUP.db_name, year, week_number, race_html)
        entry_html = os.path.join(base, "entrylist.html")
        if os.path.isfile(entry_html):
            conv.convert_entrylist(entry_html, os.path.join(base, "entrylist.csv"))


def cup_series_sync_practices(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    conv = src.converters
    for week_number, _ in iter_week_numbers_with_url(src, _RR_CUP.db_name, year):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        download_practices_html_if_missing(src, _RR_CUP.db_name, year, week_number, race_html)
        paths = sorted(glob.glob(os.path.join(base, "practice-*.html")))
        if paths:
            conv.convert_practices(paths)


def cup_series_sync_qualifyings(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    conv = src.converters
    for week_number, _ in iter_week_numbers_with_url(src, _RR_CUP.db_name, year):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        qpath = download_qualifying_html_if_missing(src, _RR_CUP.db_name, year, week_number, race_html)
        conv.convert_qualifying(qpath)


def cup_series_sync_prerace_penalties(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    conv = src.converters
    for week_number, _ in iter_week_numbers_with_url(src, _RR_CUP.db_name, year):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        if not os.path.isfile(race_html):
            continue
        conv.convert_prerace_penalties(
            race_html, os.path.join(base, "pre-race-penalties.csv")
        )


def cup_series_sync_caution_flags(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    conv = src.converters
    for week_number, _ in iter_week_numbers_with_url(src, _RR_CUP.db_name, year):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        if not os.path.isfile(race_html):
            continue
        conv.convert_caution_flags(race_html, os.path.join(base, "caution-flags.csv"))


def cup_series_sync_race_results(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    conv = src.converters
    for week_number, _ in iter_week_numbers_with_url(src, _RR_CUP.db_name, year):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        if not os.path.isfile(race_html):
            continue
        conv.convert_race_results(race_html, os.path.join(base, "race-results.csv"))
        loop_path = download_loop_data_html_if_missing(
            src, _RR_CUP.db_name, year, week_number, race_html
        )
        conv.convert_loop_data(loop_path)


def cup_series_sync_penalties_and_lap_leaders(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    conv = src.converters
    for week_number, _ in iter_week_numbers_with_url(src, _RR_CUP.db_name, year):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        pen_path = download_penalties_html_if_missing(
            src, _RR_CUP.db_name, year, week_number, race_html
        )
        conv.convert_penalties(pen_path)
        if os.path.isfile(race_html):
            conv.convert_lap_leaders(race_html, os.path.join(base, "lap-leaders.csv"))
