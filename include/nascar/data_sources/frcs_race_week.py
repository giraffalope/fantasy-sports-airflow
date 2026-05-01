"""
Cup race-week HTML sync from ``schedule.csv`` ([FRCS.pro](https://frcs.pro/)).

Each callable matches one DAG task: download missing artifacts, then convert where applicable.

Throttling: the Cup race-week DAG uses a ``WaitOperator`` for a coarse random delay before
these tasks run. Each HTTP fetch uses 2–5s jitter immediately before download (see
``include.nascar.data_sources.frcs`` ``_sleep_download_jitter``).

Schedule rows are filtered by ``week_date`` vs. the DAG run anchor (same rules as Racing
Reference in ``racing_reference_race_week``). Storage paths use ``event_number`` (row order
in ``schedule.csv``), not the official ``week_number`` from the site ``#`` column.

**Complete-week skip:** if ``<week_dir>/loop-data.csv`` exists **and has at least one data row**
(not header-only), every task skips that event week. ``loop-data.csv`` is produced by the
final task in the race-week DAG (``sync_race_results``), so it indicates a full pass already
finished for that week folder. DAGs whose ``dag_id`` starts with ``backfill.`` bypass this skip
so existing HTML can be re-converted to CSV.

**Incomplete loop CSV refresh:** if ``loop-data.csv`` is missing, unreadable, or header-only,
``sync_weeks`` removes all FRCS week scrape artifacts (race results, entry list, practices,
qualifying, lineup, pit stops, loop data, DraftKings/FanDuel HTML, pit-stall PDF) so the usual
``download_*_if_missing`` calls re-fetch from the site on that run.
"""

from __future__ import annotations

import glob
import logging
import os
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
from include.nascar.data_sources.race_week_context import is_backfill_race_week_dag
from include.nascar.data_sources.racing_reference_race_week import (
    _cup_series_schedule_year_from_context,
    parse_schedule_week_date,
    race_week_run_anchor_from_context,
    week_date_within_race_week_scrape_horizon,
)
from include.nascar.enums import NascarSeries
from include.utils.file import File

_FRCS_CUP = NascarSeries.CUP

# Week-local files downloaded from FRCS (not schedule.csv). Removed when loop-data.csv
# is incomplete so stale HTML/PDF is not kept across hourly runs.
_FRCS_WEEK_SCRAPE_ARTIFACTS: tuple[str, ...] = (
    "race-results.html",
    "entrylist.html",
    "qualifying.html",
    "lineup.html",
    "pit-stops.html",
    "loop-data.html",
    "draftkings.html",
    "fanduel.html",
    "pit-stall-selections.pdf",
)


def _frcs_purge_week_scrape_artifacts_if_loop_incomplete(
    task: str, year: int, event_number: int, week_dir: str
) -> None:
    """
    If the week is not complete (no data rows in loop-data.csv), delete cached scrape files
    so downstream ``download_*_if_missing`` re-downloads them.
    """
    if _frcs_week_loop_data_csv_has_data_rows(week_dir):
        return
    removed_any = False
    for name in _FRCS_WEEK_SCRAPE_ARTIFACTS:
        path = os.path.join(week_dir, name)
        if os.path.isfile(path):
            File.silent_remove(path)
            removed_any = True
    for path in glob.glob(os.path.join(week_dir, "practice-*.html")):
        File.silent_remove(path)
        removed_any = True
    if removed_any:
        _frcs_week_line(
            task,
            year,
            event_number,
            "purged week HTML/PDF (loop-data.csv missing or header-only; re-fetch)",
        )


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


def _frcs_week_line(task: str, year: int, event_number: int, message: str) -> None:
    logging.info(
        "FRCS race-week: %s year=%s event=%02d — %s",
        task,
        year,
        event_number,
        message,
    )


def _frcs_week_loop_data_csv_has_data_rows(week_dir: str) -> bool:
    path = os.path.join(week_dir, "loop-data.csv")
    if not os.path.isfile(path):
        return False
    try:
        rows = File.read_csv(path, as_dict=False)
    except Exception as exc:
        logging.warning(
            "FRCS race-week: loop-data.csv unreadable for skip check %s: %s",
            path,
            exc,
        )
        return False
    return len(rows) > 1


def _frcs_week_skip_if_loop_csv_present(
    task: str,
    year: int,
    event_number: int,
    week_dir: str,
    context: dict[str, Any] | None = None,
) -> bool:
    if context is not None and is_backfill_race_week_dag(context):
        return False
    if not _frcs_week_loop_data_csv_has_data_rows(week_dir):
        return False
    _frcs_week_line(task, year, event_number, "skip (loop-data.csv has data rows)")
    return True


def _frcs_remove_csv_if_no_data_rows(
    task: str, year: int, event_number: int, csv_path: str
) -> None:
    if not os.path.isfile(csv_path):
        return
    try:
        rows = File.read_csv(csv_path, as_dict=False)
    except Exception as exc:
        logging.warning(
            "FRCS race-week: unable to validate CSV rows for %s: %s",
            csv_path,
            exc,
        )
        return
    if len(rows) > 1:
        return
    File.silent_remove(csv_path)
    _frcs_week_line(task, year, event_number, f"removed empty CSV {csv_path}")


def _schedule_csv_path(source: FrcsSource, series_name: str, year: int) -> str:
    return f"{source.storage_path_series_year(series_name, year)}/schedule.csv"


def iter_week_numbers_with_url(
    source: FrcsSource,
    series_name: str,
    year: int,
    *,
    run_anchor: datetime | None = None,
) -> Iterator[tuple[int, str]]:
    """Yield ``(event_number, week_url)`` for folders ``…/<year>/<event_number:02d>/``."""
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
            event_number = int(row["event_number"])
        except (KeyError, TypeError, ValueError):
            logging.warning("Bad event_number in FRCS schedule row: %s", row)
            continue
        if run_anchor is not None:
            wd = parse_schedule_week_date(row.get("week_date"))
            if wd is None:
                logging.warning("Skipping schedule row without parseable week_date: %s", row)
                continue
            if not week_date_within_race_week_scrape_horizon(wd, run_anchor):
                continue
        yield event_number, week_url


def cup_series_sync_weeks(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    task = cup_series_sync_weeks.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, week_url in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        _frcs_purge_week_scrape_artifacts_if_loop_incomplete(
            task, year, event_number, base
        )
        _frcs_week_line(task, year, event_number, "ensure race-results.html")
        download_race_results_html_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, week_url
        )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_entrylists(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_entrylists.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, event_number, "entry list HTML + CSV")
        download_entrylist_html_if_missing(src, _FRCS_CUP.db_name, year, event_number, race_html)
        entry_html = os.path.join(base, "entrylist.html")
        entry_csv = os.path.join(base, "entrylist.csv")
        if os.path.isfile(entry_html):
            logging.info("FRCS race-week: converting entry list %s -> %s", entry_html, entry_csv)
            conv.convert_entrylist(entry_html, entry_csv)
            _frcs_remove_csv_if_no_data_rows(task, year, event_number, entry_csv)
        else:
            logging.info(
                "FRCS race-week: %s year=%s event=%02d — no entrylist.html (skipped convert)",
                task,
                year,
                event_number,
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_practices(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_practices.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, event_number, "practice HTML + CSV")
        download_practices_html_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, race_html
        )
        paths = sorted(glob.glob(os.path.join(base, "practice-*.html")))
        if paths:
            logging.info("FRCS race-week: converting practice files %s", paths)
            conv.convert_practices(paths)
            for path in paths:
                _frcs_remove_csv_if_no_data_rows(
                    task, year, event_number, path.replace(".html", ".csv")
                )
        else:
            logging.info(
                "FRCS race-week: %s year=%s event=%02d — no practice-*.html",
                task,
                year,
                event_number,
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_qualifyings(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_qualifyings.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, event_number, "qualifying HTML + CSV")
        qpath = download_qualifying_html_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, race_html
        )
        if qpath:
            logging.info("FRCS race-week: converting qualifying %s", qpath)
        conv.convert_qualifying(qpath)
        if qpath:
            _frcs_remove_csv_if_no_data_rows(
                task, year, event_number, qpath.replace(".html", ".csv")
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_lineups(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_lineups.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, event_number, "lineup HTML + CSV")
        lineup_path = download_lineup_html_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, race_html
        )
        if lineup_path:
            logging.info("FRCS race-week: converting lineup %s", lineup_path)
        conv.convert_lineup(lineup_path)
        if lineup_path:
            _frcs_remove_csv_if_no_data_rows(
                task, year, event_number, lineup_path.replace(".html", ".csv")
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_pit_stops(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_pit_stops.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, event_number, "pit stops HTML + CSV")
        pit_stops_path = download_pit_stops_html_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, race_html
        )
        if pit_stops_path:
            logging.info("FRCS race-week: converting pit stops %s", pit_stops_path)
        conv.convert_pit_stops(pit_stops_path)
        if pit_stops_path:
            pit_stops_base = pit_stops_path.replace(".html", "")
            _frcs_remove_csv_if_no_data_rows(
                task, year, event_number, f"{pit_stops_base}-summary.csv"
            )
            _frcs_remove_csv_if_no_data_rows(
                task, year, event_number, f"{pit_stops_base}-detail.csv"
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_draftkings(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_draftkings.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, event_number, "DraftKings points HTML + CSV")
        dk_path = download_draftkings_points_html_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, race_html
        )
        if dk_path:
            logging.info("FRCS race-week: converting DraftKings points %s", dk_path)
        conv.convert_draftkings_points(dk_path)
        if dk_path:
            _frcs_remove_csv_if_no_data_rows(
                task, year, event_number, dk_path.replace(".html", ".csv")
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_fanduel(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_fanduel.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, event_number, "FanDuel points HTML + CSV")
        fd_path = download_fanduel_points_html_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, race_html
        )
        if fd_path:
            logging.info("FRCS race-week: converting FanDuel points %s", fd_path)
        conv.convert_fanduel_points(fd_path)
        if fd_path:
            _frcs_remove_csv_if_no_data_rows(
                task, year, event_number, fd_path.replace(".html", ".csv")
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_pit_stall_selections(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    task = cup_series_sync_pit_stall_selections.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        stall_pdf = os.path.join(base, "pit-stall-selections.pdf")
        _frcs_week_line(task, year, event_number, "pit stall selections PDF")
        out = download_pit_stall_selections_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, race_html
        )
        if out and os.path.isfile(out):
            logging.info("FRCS race-week: pit stall selections ready %s", out)
        else:
            logging.info(
                "FRCS race-week: %s year=%s event=%02d — no PDF at %s",
                task,
                year,
                event_number,
                stall_pdf,
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_race_results(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_race_results.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        race_csv = os.path.join(base, "race-results.csv")
        if not os.path.isfile(race_html):
            logging.info(
                "FRCS race-week: %s year=%s event=%02d — skip (no %s)",
                task,
                year,
                event_number,
                race_html,
            )
            continue
        _frcs_week_line(task, year, event_number, "race results + loop data CSV")
        logging.info("FRCS race-week: converting race results %s -> %s", race_html, race_csv)
        conv.convert_race_results(race_html, race_csv)
        _frcs_remove_csv_if_no_data_rows(task, year, event_number, race_csv)
        loop_path = download_loop_data_html_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, race_html
        )
        if loop_path:
            logging.info("FRCS race-week: converting loop data %s", loop_path)
        conv.convert_loop_data(loop_path)
        if loop_path:
            _frcs_remove_csv_if_no_data_rows(
                task, year, event_number, loop_path.replace(".html", ".csv")
            )
    _frcs_task_end(task, year, weeks)


def cup_series_sync_penalties(**context: Any) -> None:
    src = _resolve_frcs_source(None)
    year = _cup_series_schedule_year_from_context(context)
    run_anchor = race_week_run_anchor_from_context(context)
    conv = src.converters
    task = cup_series_sync_penalties.__name__
    _frcs_task_begin(task, year, run_anchor)
    weeks = 0
    for event_number, _ in iter_week_numbers_with_url(
        src, _FRCS_CUP.db_name, year, run_anchor=run_anchor
    ):
        base = src.storage_path_week(_FRCS_CUP.db_name, year, event_number)
        if _frcs_week_skip_if_loop_csv_present(task, year, event_number, base, context):
            continue
        weeks += 1
        race_html = os.path.join(base, "race-results.html")
        _frcs_week_line(task, year, event_number, "penalties from race-results HTML")
        pen_path = download_penalties_html_if_missing(
            src, _FRCS_CUP.db_name, year, event_number, race_html
        )
        if pen_path:
            logging.info("FRCS race-week: converting penalties (source %s)", pen_path)
        conv.convert_penalties(pen_path)
        if pen_path:
            penalties_csv = (
                os.path.join(os.path.dirname(pen_path), "penalties.csv")
                if os.path.basename(pen_path).lower() == "race-results.html"
                else pen_path.replace(".html", ".csv")
            )
            _frcs_remove_csv_if_no_data_rows(task, year, event_number, penalties_csv)
    _frcs_task_end(task, year, weeks)
