"""frcs.pro — Cup schedule, race weekend pages (HTML + embedded Tabulator JSON)."""

from __future__ import annotations

import logging
import os
import random
import re
import time
from collections.abc import Callable
from typing import Any

from bs4 import BeautifulSoup

from include.nascar.data_sources.frcs_html_to_csv import FrcsHtmlConverters
from include.nascar.data_sources.html_parse import as_str, as_tag
from include.nascar.data_sources.race_week_context import is_backfill_race_week_dag
from include.nascar.data_sources.racing_reference import RACING_REFERENCE_USER_AGENTS
from include.nascar.enums import NascarSeries
from include.utils.scraper import Scraper

# Same rotating pool as Racing Reference — each download picks independently at random.
FRCS_USER_AGENTS = RACING_REFERENCE_USER_AGENTS


def _frcs_random_user_agent() -> str:
    return random.choice(RACING_REFERENCE_USER_AGENTS)


DEFAULT_NASCAR_SCRAPER_ROOT = "/home/airflow/scraper/nascar"

_PRACTICE_HREF_RE = re.compile(r"/races/practice/(\d+)/")


class FrcsSource:
    id = "frcs"
    domain = "frcs.pro"

    def __init__(self, scraper_root: str = DEFAULT_NASCAR_SCRAPER_ROOT):
        self._scraper_root = scraper_root.rstrip("/")
        self._converters = FrcsHtmlConverters()

    @property
    def base_url(self) -> str:
        return f"https://{self.domain}"

    def scraper_root_segment(self) -> str:
        return self.domain

    def storage_path_series_year(self, series_name: str, year: int) -> str:
        return f"{self._scraper_root}/{self.scraper_root_segment()}/{series_name.lower()}/{year}"

    def storage_path_week(self, series_name: str, year: int, event_number: int) -> str:
        """Week folder name from ``schedule.csv`` ``event_number`` (1-based row order)."""
        return f"{self.storage_path_series_year(series_name, year)}/{event_number:02d}"

    def schedule_url(self, year: int) -> str:
        return f"{self.base_url}/nascar/cup/races/schedule/{year}/"

    def resolve_href(self, href: str) -> str:
        href = href.strip()
        if href.startswith(("http://", "https://")):
            return href
        if href.startswith("//"):
            return "https:" + href
        if href.startswith("/"):
            return f"{self.base_url}{href}"
        return f"{self.base_url}/{href}"

    def resolve_page_url(self, href: str | None) -> str | None:
        if href is None:
            return None
        h = href.strip()
        if not h:
            return None
        return self.resolve_href(h)

    def _first_href_matching(self, soup: BeautifulSoup, needle: str) -> str | None:
        link = as_tag(soup.find("a", href=lambda h: h and needle in h))
        if link is None or not link.get("href"):
            return None
        return self.resolve_href(as_str(link.get("href")).strip())

    def extract_entrylist_url(self, soup: BeautifulSoup) -> str | None:
        return self._first_href_matching(soup, "/races/entrylist/")

    def extract_practice_results_url(self, soup: BeautifulSoup) -> str | None:
        return self._first_href_matching(soup, "/races/practice/1/")

    def extract_qualifying_url(self, soup: BeautifulSoup) -> str | None:
        return self._first_href_matching(soup, "/races/qualifying/")

    def extract_loopdata_url(self, soup: BeautifulSoup) -> str | None:
        u = self._first_href_matching(soup, "/races/loopdata/")
        if u:
            return u
        return self._first_href_matching(soup, "/races/loop-data/")

    def extract_penalties_url(self, soup: BeautifulSoup) -> str | None:
        return None

    def extract_lineup_url(self, soup: BeautifulSoup) -> str | None:
        return self._first_href_matching(soup, "/races/lineup/")

    def extract_pit_stops_url(self, soup: BeautifulSoup) -> str | None:
        return self._first_href_matching(soup, "/races/pit-stops/")

    def extract_draftkings_points_url(self, soup: BeautifulSoup) -> str | None:
        return self._first_href_matching(soup, "/dfs/draftkings/cup/race-fantasy-points/")

    def extract_fanduel_points_url(self, soup: BeautifulSoup) -> str | None:
        return self._first_href_matching(soup, "/dfs/fanduel/cup/race-fantasy-points/")

    def extract_pit_stall_selections_url(self, soup: BeautifulSoup) -> str | None:
        return self._first_href_matching(soup, "pit-stall-selections.pdf")

    def iter_practice_session_urls(
        self, practice_index_soup: BeautifulSoup
    ) -> list[tuple[int, str]]:
        out: list[tuple[int, str]] = []
        for a in practice_index_soup.find_all("a", href=True):
            h = str(a["href"])
            m = _PRACTICE_HREF_RE.search(h)
            if not m:
                continue
            n = int(m.group(1))
            url = self.resolve_href(h.strip())
            if (n, url) not in out:
                out.append((n, url))
        out.sort(key=lambda x: x[0])
        return out

    @property
    def converters(self) -> FrcsHtmlConverters:
        return self._converters


def _resolve_frcs_source(source: FrcsSource | None) -> FrcsSource:
    if source is not None:
        return source
    return FrcsSource()


def download_year_schedule_html(
    context: dict[str, Any],
    series_name: str,
    *,
    source: FrcsSource | None = None,
) -> None:
    src = _resolve_frcs_source(source)
    year = context["data_interval_start"].year
    base = src.storage_path_series_year(series_name, year)
    schedule_csv = os.path.join(base, "schedule.csv")
    if is_backfill_race_week_dag(context) and os.path.isfile(schedule_csv):
        logging.info(
            "FRCS: skip schedule HTML download (backfill; %s already exists)",
            schedule_csv,
        )
        return
    url = src.schedule_url(year)
    dest = os.path.join(base, "schedule.html")
    logging.info("FRCS: downloading schedule %s -> %s", url, dest)
    scraper = Scraper(base)
    _sleep_download_jitter()
    scraper.download(
        url=url,
        filename="schedule.html",
        user_agent=_frcs_random_user_agent(),
    )


def cup_series_download_year_schedule_html(**context: Any) -> None:
    download_year_schedule_html(context, NascarSeries.CUP.db_name)


def convert_year_schedule_html_to_csv(
    context: dict[str, Any],
    series_name: str,
    *,
    source: FrcsSource | None = None,
) -> None:
    src = _resolve_frcs_source(source)
    year = context["data_interval_start"].year
    base = src.storage_path_series_year(series_name, year)
    html_file_path = f"{base}/schedule.html"
    csv_file_path = f"{base}/schedule.csv"
    if is_backfill_race_week_dag(context) and os.path.isfile(csv_file_path):
        logging.info(
            "FRCS: skip schedule HTML→CSV (backfill; %s already exists)",
            csv_file_path,
        )
        return
    logging.info("FRCS: converting schedule %s -> %s", html_file_path, csv_file_path)
    src.converters.convert_schedule(html_file_path, csv_file_path)


def cup_series_convert_year_schedule_html_to_csv(**context: Any) -> None:
    convert_year_schedule_html_to_csv(context, NascarSeries.CUP.db_name)


def _sleep_download_jitter() -> None:
    time.sleep(random.randint(2, 5))


def download_race_results_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    week_url: str,
) -> None:
    week_path = source.storage_path_week(series_name, year, event_number)
    race_html = os.path.join(week_path, "race-results.html")
    if os.path.isfile(race_html):
        return
    resolved = source.resolve_page_url(week_url.strip())
    if not resolved:
        logging.warning(
            "Could not resolve week_url for %s year %s event %s; skipping race results download",
            series_name,
            year,
            event_number,
        )
        return
    scraper = Scraper(week_path)
    _sleep_download_jitter()
    logging.info("FRCS: downloading race-results %s -> %s", resolved, race_html)
    scraper.download(
        url=resolved,
        filename="race-results.html",
        user_agent=_frcs_random_user_agent(),
    )


def download_entrylist_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> None:
    if not os.path.isfile(race_results_html_path):
        return
    week_path = source.storage_path_week(series_name, year, event_number)
    entry_path = os.path.join(week_path, "entrylist.html")
    if os.path.isfile(entry_path):
        return
    with open(race_results_html_path, encoding="utf-8") as html_file:
        soup = BeautifulSoup(html_file, "html.parser")
    url = source.extract_entrylist_url(soup)
    if not url:
        logging.warning(
            "No entry list link for %s year %s event %s", series_name, year, event_number
        )
        return
    scraper = Scraper(week_path)
    _sleep_download_jitter()
    logging.info("FRCS: downloading entry list %s -> %s", url, entry_path)
    scraper.download(
        url=url,
        filename="entrylist.html",
        user_agent=_frcs_random_user_agent(),
    )


def download_practices_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> list[str]:
    week_path = source.storage_path_week(series_name, year, event_number)
    out: list[str] = []
    if not os.path.isfile(race_results_html_path):
        return out
    with open(race_results_html_path, encoding="utf-8") as html_file:
        soup_rr = BeautifulSoup(html_file, "html.parser")
    sessions = source.iter_practice_session_urls(soup_rr)
    if not sessions:
        return out
    for number, session_url in sessions:
        fn = f"practice-{number:02d}.html"
        p = os.path.join(week_path, fn)
        if not os.path.isfile(p):
            scraper = Scraper(week_path)
            _sleep_download_jitter()
            logging.info("FRCS: downloading practice session %s %s -> %s", number, session_url, p)
            scraper.download(
                url=session_url,
                filename=fn,
                user_agent=_frcs_random_user_agent(),
            )
        if os.path.isfile(p) and p not in out:
            out.append(p)
    return sorted(out)


def download_qualifying_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> str | None:
    week_path = source.storage_path_week(series_name, year, event_number)
    qpath = os.path.join(week_path, "qualifying.html")
    if os.path.isfile(qpath):
        return qpath
    if not os.path.isfile(race_results_html_path):
        return None
    with open(race_results_html_path, encoding="utf-8") as html_file:
        soup = BeautifulSoup(html_file, "html.parser")
    url = source.extract_qualifying_url(soup)
    if url is None:
        return None
    scraper = Scraper(week_path)
    _sleep_download_jitter()
    logging.info("FRCS: downloading qualifying %s -> %s", url, qpath)
    scraper.download(
        url=url,
        filename="qualifying.html",
        user_agent=_frcs_random_user_agent(),
    )
    return qpath if os.path.isfile(qpath) else None


def download_loop_data_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> str | None:
    week_path = source.storage_path_week(series_name, year, event_number)
    lpath = os.path.join(week_path, "loop-data.html")
    if os.path.isfile(lpath):
        return lpath
    if not os.path.isfile(race_results_html_path):
        return None
    with open(race_results_html_path, encoding="utf-8") as html_file:
        soup = BeautifulSoup(html_file, "html.parser")
    url = source.extract_loopdata_url(soup)
    if url is None:
        return None
    scraper = Scraper(week_path)
    _sleep_download_jitter()
    logging.info("FRCS: downloading loop data %s -> %s", url, lpath)
    scraper.download(
        url=url,
        filename="loop-data.html",
        user_agent=_frcs_random_user_agent(),
    )
    return lpath if os.path.isfile(lpath) else None


def download_penalties_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> str | None:
    _ = (source, series_name, year, event_number)
    if not os.path.isfile(race_results_html_path):
        return None
    return race_results_html_path


def _download_related_page_if_missing(
    source: FrcsSource,
    race_results_html_path: str,
    output_filename: str,
    series_name: str,
    year: int,
    event_number: int,
    *,
    url_getter: Callable[[BeautifulSoup], str | None],
    soft_fail_on_download_error: bool = False,
) -> str | None:
    if not os.path.isfile(race_results_html_path):
        return None
    week_path = source.storage_path_week(series_name, year, event_number)
    out_path = os.path.join(week_path, output_filename)
    if os.path.isfile(out_path):
        return out_path
    with open(race_results_html_path, encoding="utf-8") as html_file:
        soup = BeautifulSoup(html_file, "html.parser")
    url = url_getter(soup)
    if not url:
        logging.warning(
            "No %s link for %s year %s event %s",
            output_filename,
            series_name,
            year,
            event_number,
        )
        return None
    scraper = Scraper(week_path)
    _sleep_download_jitter()
    logging.info("FRCS: downloading %s %s -> %s", output_filename, url, out_path)
    try:
        scraper.download(
            url=url,
            filename=output_filename,
            user_agent=_frcs_random_user_agent(),
        )
    except Exception as exc:
        if soft_fail_on_download_error:
            logging.warning(
                "Skipping %s for %s year %s event %s (%s): %s",
                output_filename,
                series_name,
                year,
                event_number,
                url,
                exc,
            )
            return None
        raise
    return out_path if os.path.isfile(out_path) else None


def download_lineup_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> str | None:
    return _download_related_page_if_missing(
        source,
        race_results_html_path,
        "lineup.html",
        series_name,
        year,
        event_number,
        url_getter=source.extract_lineup_url,
    )


def download_pit_stops_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> str | None:
    return _download_related_page_if_missing(
        source,
        race_results_html_path,
        "pit-stops.html",
        series_name,
        year,
        event_number,
        url_getter=source.extract_pit_stops_url,
    )


def download_draftkings_points_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> str | None:
    return _download_related_page_if_missing(
        source,
        race_results_html_path,
        "draftkings.html",
        series_name,
        year,
        event_number,
        url_getter=source.extract_draftkings_points_url,
    )


def download_fanduel_points_html_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> str | None:
    return _download_related_page_if_missing(
        source,
        race_results_html_path,
        "fanduel.html",
        series_name,
        year,
        event_number,
        url_getter=source.extract_fanduel_points_url,
    )


def download_pit_stall_selections_if_missing(
    source: FrcsSource,
    series_name: str,
    year: int,
    event_number: int,
    race_results_html_path: str,
) -> str | None:
    return _download_related_page_if_missing(
        source,
        race_results_html_path,
        "pit-stall-selections.pdf",
        series_name,
        year,
        event_number,
        url_getter=source.extract_pit_stall_selections_url,
        soft_fail_on_download_error=True,
    )
