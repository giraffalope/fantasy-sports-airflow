"""
Cup race-week HTML sync from ``schedule.csv`` ([FRCS.pro](https://frcs.pro/)).

Each callable matches one DAG task: download missing artifacts, then convert where applicable.

Throttling: each task sleeps 0–5s at start (same spread as the DAG ``WaitOperator``,
but evaluated at run time), and successive weeks inside one task pause 2–5s so
multi-week runs do not rapid-fire FRCS. Downloads also use 2–5s jitter before each
HTTP fetch (see ``include.nascar.data_sources.frcs``).

Race weeks are limited by ``week_date`` vs. the DAG run anchor (same rules as Racing
Reference in ``racing_reference_race_week``).
"""

from __future__ import annotations

import glob
import logging
import os
import random
import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from include.nascar.data_sources.frcs import (
    FrcsSource,
    _resolve_frcs_source,
    download_draftkings_points_html_if_missing,
    download_entrylist_html_if_missing,
    download_fanduel_points_html_if_missing,
    download_lineup_html_if_missing,
    download_loop_data_html_if_missing,
    download_penalties_html_if_missing,
    download_pit_stall_selections_if_missing,
    download_pit_stops_html_if_missing,
    download_practices_html_if_missing,
    download_qualifying_html_if_missing,
    download_race_results_html_if_missing,
)
from include.nascar.data_sources.racing_reference_race_week import (
    _cup_series_schedule_year_from_context,
    parse_schedule_week_date,
    race_week_run_anchor_from_context,
    week_date_within_race_week_scrape_horizon,
)
from include.nascar.enums import NascarSeries
from include.utils.file import File

_FRCS_CUP = NascarSeries.CUP


def _frcs_task_begin(task: str, year: int, run_anchor: datetime) -> None:
    logging.info(
        "FRCS race-week: %s starting year=%s run_anchor=%s",
        task,
        year,
        run_anchor.isoformat(),
    )


def _frcs_task_end(task: str, year: int, weeks_processed: int) -> None:
    logging.info(
        "FRCS race-week: %s finished year=%s weeks_processed=%s",
        task,
        year,
        weeks_processed,
    )


def _frcs_week_line(task: str, year: int, week_number: int, message: str) -> None:
    logging.info(
        "FRCS race-week: %s year=%s week=%02d — %s",
        task,
        year,
        week_number,
        message,
    )


def _sleep_frcs_task_start_jitter() -> None:
    """Match ``WaitOperator(seconds=random.randint(0, 5))`` at task run time."""
    time.sleep(random.randint(0, 5))


def _iter_week_numbers_with_url_throttled(
    source: FrcsSource,
    series_name: str,
    year: int,
    *,
    run_anchor: datetime | None = None,
) -> Iterator[tuple[int, str]]:
    """Yield schedule rows with a random pause between weeks (2–5s)."""
    first = True
    for item in iter_week_numbers_with_url(source, series_name, year, run_anchor=run_anchor):
        if not first:
            time.sleep(random.randint(2, 5))
        first = False
        yield item


def _schedule_csv_path(source: FrcsSource, series_name: str, year: int) -> str:
    return f"{source.storage_path_series_year(series_name, year)}/schedule.csv"


def iter_week_numbers_with_url(
    source: FrcsSource,
    series_name: str,
    year: int,
    *,
    run_anchor: datetime | None = None,
) -> Iterator[tuple[int, str]]:
    path = _schedule_csv_path(source, series_name, year)
    if not os.path.isfile(path):
        logging.warning("Schedule CSV missing at %s; skipping FRCS race-week sync", path)
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
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    task = cup_series_sync_weeks.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, week_url in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        _frcs_week_line(task, year, week_number, "ensure race-results.html")
        download_race_results_html_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, week_url
        )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_entrylists(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_entrylists.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, week_number, "entry list HTML + CSV")
        download_entrylist_html_if_missing(src, _FRCS_CUP.db_name, year, week_number, race_html)
        entry_html = os.path.join(base, "entrylist.html")
        entry_csv = os.path.join(base, "entrylist.csv")
        if os.path.isfile(entry_html):
            logging.info("FRCS race-week: converting entry list %s -> %s", entry_html, entry_csv)
            conv.convert_entrylist(entry_html, entry_csv)
        else:
            logging.info(
                "FRCS race-week: %s year=%s week=%02d — no entrylist.html (skipped convert)",
                task,
                year,
                week_number,
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_practices(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_practices.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, week_number, "practice HTML + CSV")
        download_practices_html_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, race_html
        )
        paths = sorted(glob.glob(os.path.join(base, "practice-*.html")))
        if paths:
            logging.info("FRCS race-week: converting practice files %s", paths)
            conv.convert_practices(paths)
        else:
            logging.info(
                "FRCS race-week: %s year=%s week=%02d — no practice-*.html",
                task,
                year,
                week_number,
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_qualifyings(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_qualifyings.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, week_number, "qualifying HTML + CSV")
        qpath = download_qualifying_html_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, race_html
        )
        if qpath:
            logging.info("FRCS race-week: converting qualifying %s", qpath)
        conv.convert_qualifying(qpath)
    _frcs_task_end(task, year, weeks)


def cup_series_sync_lineups(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_lineups.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, week_number, "lineup HTML + CSV")
        lineup_path = download_lineup_html_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, race_html
        )
        if lineup_path:
            logging.info("FRCS race-week: converting lineup %s", lineup_path)
        conv.convert_lineup(lineup_path)
    _frcs_task_end(task, year, weeks)


def cup_series_sync_pit_stops(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_pit_stops.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, week_number, "pit stops HTML + CSV")
        pit_stops_path = download_pit_stops_html_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, race_html
        )
        if pit_stops_path:
            logging.info("FRCS race-week: converting pit stops %s", pit_stops_path)
        conv.convert_pit_stops(pit_stops_path)
    _frcs_task_end(task, year, weeks)


def cup_series_sync_draftkings_points(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_draftkings_points.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, week_number, "DraftKings points HTML + CSV")
        dk_path = download_draftkings_points_html_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, race_html
        )
        if dk_path:
            logging.info("FRCS race-week: converting DraftKings points %s", dk_path)
        conv.convert_draftkings_points(dk_path)
    _frcs_task_end(task, year, weeks)


def cup_series_sync_fanduel_points(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_fanduel_points.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, week_number, "FanDuel points HTML + CSV")
        fd_path = download_fanduel_points_html_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, race_html
        )
        if fd_path:
            logging.info("FRCS race-week: converting FanDuel points %s", fd_path)
        conv.convert_fanduel_points(fd_path)
    _frcs_task_end(task, year, weeks)


def cup_series_sync_pit_stall_selections(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    task = cup_series_sync_pit_stall_selections.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        stall_pdf = os.path.join(base, "pit-stall-selections.pdf")
        _frcs_week_line(task, year, week_number, "pit stall selections PDF")
        out = download_pit_stall_selections_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, race_html
        )
        if out and os.path.isfile(out):
            logging.info("FRCS race-week: pit stall selections ready %s", out)
        else:
            logging.info(
                "FRCS race-week: %s year=%s week=%02d — no PDF at %s",
                task,
                year,
                week_number,
                stall_pdf,
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_prerace_penalties(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_prerace_penalties.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        pre_csv = os.path.join(base, "pre-race-penalties.csv")
        if not os.path.isfile(race_html):
            logging.info(
                "FRCS race-week: %s year=%s week=%02d — skip (no %s)",
                task,
                year,
                week_number,
                race_html,
            )
            continue
        _frcs_week_line(task, year, week_number, "pre-race penalties CSV")
        logging.info("FRCS race-week: converting pre-race penalties %s -> %s", race_html, pre_csv)
        conv.convert_prerace_penalties(race_html, pre_csv)
    _frcs_task_end(task, year, weeks)


def cup_series_sync_race_results(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_race_results.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        race_csv = os.path.join(base, "race-results.csv")
        if not os.path.isfile(race_html):
            logging.info(
                "FRCS race-week: %s year=%s week=%02d — skip (no %s)",
                task,
                year,
                week_number,
                race_html,
            )
            continue
        _frcs_week_line(task, year, week_number, "race results + loop data CSV")
        logging.info("FRCS race-week: converting race results %s -> %s", race_html, race_csv)
        conv.convert_race_results(race_html, race_csv)
        loop_path = download_loop_data_html_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, race_html
        )
        if loop_path:
            logging.info("FRCS race-week: converting loop data %s", loop_path)
        conv.convert_loop_data(loop_path)
    _frcs_task_end(task, year, weeks)


def cup_series_sync_penalties(**context: Any) -> None:
    _sleep_frcs_task_start_jitter()
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_penalties.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for week_number, _ in _iter_week_numbers_with_url_throttled(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        weeks += 1
        base = src.storage_path_week(_FRCS_CUP.db_name, year, week_number)
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, week_number, "penalties from race-results HTML")
        pen_path = download_penalties_html_if_missing(
            src, _FRCS_CUP.db_name, year, week_number, race_html
        )
        if pen_path:
            logging.info("FRCS race-week: converting penalties (source %s)", pen_path)
        conv.convert_penalties(pen_path)
    _frcs_task_end(task, year, weeks)
