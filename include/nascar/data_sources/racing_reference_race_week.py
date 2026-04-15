"""
Cup race-week HTML sync from ``schedule.csv`` (Racing Reference).

Each callable matches one DAG task: download missing artifacts, then convert where applicable.

Schedule rows are skipped when ``week_date`` is missing/unparseable or when that date
(start of day UTC) is **not** strictly before the DAG run anchor plus
``RACE_WEEK_SCRAPE_HORIZON_DAYS`` — so distant future races are not scraped on
every hourly/catchup run.

**Complete-week skip:** if ``<week_dir>/loop-data.csv`` exists **and has at least one data row**,
each task skips that week. ``loop-data.csv`` is written by the final DAG task
(``sync_race_results``), matching FRCS race-week behavior. DAGs whose ``dag_id`` starts with
``backfill.`` bypass this skip so existing HTML can be re-converted to CSV.
"""

from __future__ import annotations

import glob
import logging
import os
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

from include.nascar.data_sources.race_week_context import is_backfill_race_week_dag
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

RACE_WEEK_SCRAPE_HORIZON_DAYS = 14


def _rr_task_begin(task: str, year: int, run_anchor: datetime) -> None:
    logging.info(
        "Racing Reference race-week: %s starting year=%s run_anchor=%s",
        task,
        year,
        run_anchor.isoformat(),
    )


def _rr_task_end(task: str, year: int, weeks_processed: int) -> None:
    logging.info(
        "Racing Reference race-week: %s finished year=%s weeks_processed=%s",
        task,
        year,
        weeks_processed,
    )


def _rr_week_line(task: str, year: int, week_number: int, message: str) -> None:
    logging.info(
        "Racing Reference race-week: %s year=%s week=%02d — %s",
        task,
        year,
        week_number,
        message,
    )


def _rr_week_loop_data_csv_has_data_rows(week_dir: str) -> bool:
    path = os.path.join(week_dir, "loop-data.csv")
    if not os.path.isfile(path):
        return False
    try:
        rows = File.read_csv(path, as_dict=False)
    except Exception as exc:
        logging.warning(
            "Racing Reference race-week: loop-data.csv unreadable for skip check %s: %s",
            path,
            exc,
        )
        return False
    return len(rows) > 1


def _rr_week_skip_if_loop_csv_present(
    task: str,
    year: int,
    week_number: int,
    week_dir: str,
    context: dict[str, Any] | None = None,
) -> bool:
    if context is not None and is_backfill_race_week_dag(context):
        return False
    if not _rr_week_loop_data_csv_has_data_rows(week_dir):
        return False
    _rr_week_line(task, year, week_number, "skip (loop-data.csv has data rows)")
    return True


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


def _datetime_from_airflow_run_id(run_id: str | None) -> datetime | None:
    """Parse full datetime from ``scheduled__…`` / ``asset_triggered__…`` run ids."""
    if not run_id:
        return None
    if run_id.startswith("scheduled__"):
        return _parse_iso_datetime_string(run_id[len("scheduled__") :])
    if run_id.startswith("asset_triggered__"):
        rest = run_id[len("asset_triggered__") :]
        if "_" not in rest:
            return None
        iso_candidate = rest.rsplit("_", 1)[0]
        return _parse_iso_datetime_string(iso_candidate)
    return None


def _as_utc_datetime(dt: date | datetime) -> datetime:
    """
    Normalize to timezone-aware UTC for comparisons.

    Naive :class:`datetime` values are treated as UTC wall time. Calendar
    :class:`date` values map to midnight UTC. Re-applied after arithmetic so
    ``run_anchor + timedelta`` cannot leave a naive cutoff that would raise
    when compared to UTC-aware week starts.
    """
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    return datetime(dt.year, dt.month, dt.day, tzinfo=UTC)


def parse_schedule_week_date(raw: str | None) -> date | None:
    """Parse ``week_date`` from schedule CSV (``YYYY-MM-DD`` or a string that starts with it)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    head = s[:10]
    try:
        return date.fromisoformat(head)
    except ValueError:
        return None


def week_date_within_race_week_scrape_horizon(week_date: date, run_anchor: datetime) -> bool:
    """
    True if this race week should be scraped for the given DAG run.

    The week is included when the start of ``week_date`` in UTC is strictly before
    ``run_anchor`` (normalized to UTC) plus ``RACE_WEEK_SCRAPE_HORIZON_DAYS``.
    """
    run_utc = _as_utc_datetime(run_anchor)
    cutoff = _as_utc_datetime(run_utc + timedelta(days=RACE_WEEK_SCRAPE_HORIZON_DAYS))
    week_start = _as_utc_datetime(week_date)
    return week_start < cutoff


def race_week_run_anchor_from_context(context: dict[str, Any]) -> datetime:
    """
    DAG run instant for schedule filtering (mirrors year-resolution fallbacks in
    :func:`_cup_series_schedule_year_from_context`).
    """
    dis = context.get("data_interval_start")
    if dis is not None:
        return _as_utc_datetime(dis)
    logical = context.get("logical_date")
    if logical is not None:
        return _as_utc_datetime(logical)

    events_map = context.get("triggering_asset_events")
    if events_map is not None:
        for events in events_map.values():
            for event in events or ():
                src = getattr(event, "source_dag_run", None)
                if src is not None:
                    for attr in (
                        "data_interval_start",
                        "logical_date",
                        "run_after",
                        "start_date",
                    ):
                        dt = getattr(src, attr, None)
                        if dt is not None:
                            return _as_utc_datetime(dt)
                dt = _datetime_from_airflow_run_id(getattr(event, "source_run_id", None))
                if dt is not None:
                    return _as_utc_datetime(dt)
                ts = getattr(event, "timestamp", None)
                if ts is not None:
                    return _as_utc_datetime(ts)

    dag_run = context.get("dag_run")
    if dag_run is not None:
        for attr in (
            "data_interval_start",
            "logical_date",
            "run_after",
            "start_date",
        ):
            dt = getattr(dag_run, attr, None)
            if dt is not None:
                return _as_utc_datetime(dt)
        dt = _datetime_from_airflow_run_id(getattr(dag_run, "run_id", None))
        if dt is not None:
            return _as_utc_datetime(dt)

    ti = context.get("ti")
    if ti is not None:
        dr = getattr(ti, "dag_run", None)
        if dr is not None:
            dt = _datetime_from_airflow_run_id(getattr(dr, "run_id", None))
            if dt is not None:
                return _as_utc_datetime(dt)

    raise ValueError(
        "Cannot resolve race-week run anchor: no data_interval_start, logical_date, "
        "triggering_asset_events, or dag_run date/run_id fallbacks"
    )


def _year_from_airflow_scheduled_run_id(run_id: str | None) -> int | None:
    """Backward-compatible name for scheduled-run id parsing only."""
    if not run_id or not run_id.startswith("scheduled__"):
        return None
    return _year_from_airflow_run_id(run_id)


def _cup_series_schedule_year_from_context(context: dict[str, Any]) -> int:
    """
    Calendar year for ``…/cup/<year>/`` paths.

    Cron DAGs expose ``data_interval_start`` / ``logical_date`` (primary path when
    race-week runs on a timetable aligned with schedule). Fallbacks cover manual runs
    and legacy asset triggers: ``triggering_asset_events``, ``dag_run.run_id``
    (``scheduled__`` / ``asset_triggered__``), etc.
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
    source: RacingReferenceSource,
    series_name: str,
    year: int,
    *,
    run_anchor: datetime | None = None,
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
        if run_anchor is not None:
            wd = parse_schedule_week_date(row.get("week_date"))
            if wd is None:
                logging.warning("Skipping schedule row without parseable week_date: %s", row)
                continue
            if not week_date_within_race_week_scrape_horizon(wd, run_anchor):
                continue
        yield week_number, week_url


def cup_series_sync_weeks(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    task = cup_series_sync_weeks.__name__
    _rr_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, week_url in iter_week_numbers_with_url(
        src, _RR_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        if _rr_week_skip_if_loop_csv_present(task, year, week_number, base, context):
            continue
        weeks += 1
        _rr_week_line(task, year, week_number, "ensure race-results.html")
        download_race_results_html_if_missing(src, _RR_CUP.db_name, year, week_number, week_url)
    _rr_task_end(task, year, weeks)


def cup_series_sync_entrylists(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_entrylists.__name__
    _rr_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in iter_week_numbers_with_url(
        src, _RR_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        if _rr_week_skip_if_loop_csv_present(task, year, week_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _rr_week_line(task, year, week_number, "entry list HTML + CSV")
        download_entrylist_html_if_missing(src, _RR_CUP.db_name, year, week_number, race_html)
        entry_html = os.path.join(base, "entrylist.html")
        entry_csv = os.path.join(base, "entrylist.csv")
        if os.path.isfile(entry_html):
            logging.info(
                "Racing Reference race-week: converting entry list %s -> %s",
                entry_html,
                entry_csv,
            )
            conv.convert_entrylist(entry_html, entry_csv)
        else:
            logging.info(
                "Racing Reference race-week: %s year=%s week=%02d — no entrylist.html (skipped convert)",
                task,
                year,
                week_number,
            )
    _rr_task_end(task, year, weeks)


def cup_series_sync_practices(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_practices.__name__
    _rr_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in iter_week_numbers_with_url(
        src, _RR_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        if _rr_week_skip_if_loop_csv_present(task, year, week_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _rr_week_line(task, year, week_number, "practice HTML + CSV")
        download_practices_html_if_missing(src, _RR_CUP.db_name, year, week_number, race_html)
        paths = sorted(glob.glob(os.path.join(base, "practice-*.html")))
        if paths:
            logging.info("Racing Reference race-week: converting practice files %s", paths)
            conv.convert_practices(paths)
        else:
            logging.info(
                "Racing Reference race-week: %s year=%s week=%02d — no practice-*.html",
                task,
                year,
                week_number,
            )
    _rr_task_end(task, year, weeks)


def cup_series_sync_qualifyings(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_qualifyings.__name__
    _rr_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in iter_week_numbers_with_url(
        src, _RR_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        if _rr_week_skip_if_loop_csv_present(task, year, week_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _rr_week_line(task, year, week_number, "qualifying HTML + CSV")
        qpath = download_qualifying_html_if_missing(src, _RR_CUP.db_name, year, week_number, race_html)
        if qpath:
            logging.info("Racing Reference race-week: converting qualifying %s", qpath)
        conv.convert_qualifying(qpath)
    _rr_task_end(task, year, weeks)


def cup_series_sync_prerace_penalties(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_prerace_penalties.__name__
    _rr_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in iter_week_numbers_with_url(
        src, _RR_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        if _rr_week_skip_if_loop_csv_present(task, year, week_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        pre_csv = os.path.join(base, "pre-race-penalties.csv")
        if not os.path.isfile(race_html):
            logging.info(
                "Racing Reference race-week: %s year=%s week=%02d — skip (no %s)",
                task,
                year,
                week_number,
                race_html,
            )
            continue
        _rr_week_line(task, year, week_number, "pre-race penalties CSV")
        logging.info(
            "Racing Reference race-week: converting pre-race penalties %s -> %s",
            race_html,
            pre_csv,
        )
        conv.convert_prerace_penalties(race_html, pre_csv)
    _rr_task_end(task, year, weeks)


def cup_series_sync_caution_flags(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_caution_flags.__name__
    _rr_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in iter_week_numbers_with_url(
        src, _RR_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        if _rr_week_skip_if_loop_csv_present(task, year, week_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        caution_csv = os.path.join(base, "caution-flags.csv")
        if not os.path.isfile(race_html):
            logging.info(
                "Racing Reference race-week: %s year=%s week=%02d — skip (no %s)",
                task,
                year,
                week_number,
                race_html,
            )
            continue
        _rr_week_line(task, year, week_number, "caution flags CSV")
        logging.info(
            "Racing Reference race-week: converting caution flags %s -> %s",
            race_html,
            caution_csv,
        )
        conv.convert_caution_flags(race_html, caution_csv)
    _rr_task_end(task, year, weeks)


def cup_series_sync_race_results(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_race_results.__name__
    _rr_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in iter_week_numbers_with_url(
        src, _RR_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        if _rr_week_skip_if_loop_csv_present(task, year, week_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        race_csv = os.path.join(base, "race-results.csv")
        if not os.path.isfile(race_html):
            logging.info(
                "Racing Reference race-week: %s year=%s week=%02d — skip (no %s)",
                task,
                year,
                week_number,
                race_html,
            )
            continue
        _rr_week_line(task, year, week_number, "race results + loop data CSV")
        logging.info(
            "Racing Reference race-week: converting race results %s -> %s",
            race_html,
            race_csv,
        )
        conv.convert_race_results(race_html, race_csv)
        loop_path = download_loop_data_html_if_missing(
            src, _RR_CUP.db_name, year, week_number, race_html
        )
        if loop_path:
            logging.info("Racing Reference race-week: converting loop data %s", loop_path)
        conv.convert_loop_data(loop_path)
    _rr_task_end(task, year, weeks)


def cup_series_sync_penalties_and_lap_leaders(**context: Any) -> None:
    src = _resolve_racing_reference_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_penalties_and_lap_leaders.__name__
    _rr_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in iter_week_numbers_with_url(
        src, _RR_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_RR_CUP.db_name, year, week_number)
        if _rr_week_skip_if_loop_csv_present(task, year, week_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        lap_csv = os.path.join(base, "lap-leaders.csv")
        _rr_week_line(task, year, week_number, "penalties HTML + lap leaders CSV")
        pen_path = download_penalties_html_if_missing(
            src, _RR_CUP.db_name, year, week_number, race_html
        )
        if pen_path:
            logging.info(
                "Racing Reference race-week: converting penalties (source %s)",
                pen_path,
            )
        conv.convert_penalties(pen_path)
        if os.path.isfile(race_html):
            logging.info(
                "Racing Reference race-week: converting lap leaders %s -> %s",
                race_html,
                lap_csv,
            )
            conv.convert_lap_leaders(race_html, lap_csv)
        else:
            logging.info(
                "Racing Reference race-week: %s year=%s week=%02d — skip lap leaders (no %s)",
                task,
                year,
                week_number,
                race_html,
            )
    _rr_task_end(task, year, weeks)
