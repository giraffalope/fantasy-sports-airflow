"""www.racing-reference.info — schedule, results, and related pages."""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any, cast

from bs4 import BeautifulSoup

from include.nascar.data_sources.racing_reference_html_to_csv import RacingReferenceHtmlToCsv
from include.utils.scraper import Scraper

RACING_REFERENCE_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
]

# Default layout: /home/airflow/scraper/nascar/<host>/… — mount host scraper-data at /home/airflow/scraper
# so the nascar directory appears on the host under scraper-data/nascar/.
DEFAULT_NASCAR_SCRAPER_ROOT = "/home/airflow/scraper/nascar"


class RacingReferenceHtmlConverters:
    """Delegates to RacingReferenceHtmlToCsv (www.racing-reference.info markup)."""

    def convert_schedule(self, html_file_path: str, csv_file_path: str) -> None:
        RacingReferenceHtmlToCsv.convert_schedule(html_file_path, csv_file_path)

    def convert_race_results(self, html_file_path: str, csv_file_path: str) -> None:
        RacingReferenceHtmlToCsv.convert_race_results(html_file_path, csv_file_path)

    def convert_prerace_penalties(self, html_file_path: str, csv_file_path: str) -> None:
        RacingReferenceHtmlToCsv.convert_prerace_penalties(html_file_path, csv_file_path)

    def convert_caution_flags(self, html_file_path: str, csv_file_path: str) -> None:
        RacingReferenceHtmlToCsv.convert_caution_flags(html_file_path, csv_file_path)

    def convert_lap_leaders(self, html_file_path: str, csv_file_path: str) -> None:
        RacingReferenceHtmlToCsv.convert_lap_leaders(html_file_path, csv_file_path)

    def convert_entrylist(self, html_file_path: str, csv_file_path: str) -> None:
        RacingReferenceHtmlToCsv.convert_entrylist(html_file_path, csv_file_path)

    def convert_practices(self, paths: list[str]) -> None:
        RacingReferenceHtmlToCsv.convert_practices(paths)

    def convert_qualifying(self, path: str | None) -> None:
        RacingReferenceHtmlToCsv.convert_qualifying(path)

    def convert_loop_data(self, path: str | None) -> None:
        RacingReferenceHtmlToCsv.convert_loop_data(path)

    def convert_penalties(self, path: str | None) -> None:
        RacingReferenceHtmlToCsv.convert_penalties(path)


class RacingReferenceSource:
    id = "racing_reference"
    domain = "www.racing-reference.info"

    def __init__(self, scraper_root: str = DEFAULT_NASCAR_SCRAPER_ROOT):
        self._scraper_root = scraper_root.rstrip("/")
        self._converters = RacingReferenceHtmlConverters()

    @property
    def base_url(self) -> str:
        return f"https://{self.domain}"

    def scraper_root_segment(self) -> str:
        return self.domain

    def storage_path_series_year(self, series_name: str, year: int) -> str:
        return f"{self._scraper_root}/{self.scraper_root_segment()}/{series_name.lower()}/{year}"

    def storage_path_week(self, series_name: str, year: int, week_number: int) -> str:
        return f"{self.storage_path_series_year(series_name, year)}/{week_number:02d}"

    def schedule_url(self, year: int) -> str:
        return f"{self.base_url}/raceyear/{year}/W"

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

    def _find_first_href_containing(self, soup: BeautifulSoup, substring: str) -> str | None:
        table = soup.find("table", {"class": "tb"})
        if table is None:
            return None
        for row in table.find_all("tr"):
            for cell in row.find_all("td"):
                for a in cell.find_all("a", href=True):
                    if substring in a["href"]:
                        return self.resolve_href(a["href"])
        return None

    def extract_entrylist_url(self, soup: BeautifulSoup) -> str | None:
        return self._find_first_href_containing(soup, "entrylist")

    def extract_practice_results_url(self, soup: BeautifulSoup) -> str | None:
        return self._find_first_href_containing(soup, "practice-results")

    def extract_qualifying_url(self, soup: BeautifulSoup) -> str | None:
        return self._find_first_href_containing(soup, "qual-results")

    def extract_loopdata_url(self, soup: BeautifulSoup) -> str | None:
        return self._find_first_href_containing(soup, "loopdata")

    def extract_penalties_url(self, soup: BeautifulSoup) -> str | None:
        return self._find_first_href_containing(soup, "penalties")

    def iter_practice_session_urls(
        self, practice_index_soup: BeautifulSoup
    ) -> list[tuple[int, str]]:
        select = practice_index_soup.find("select", attrs={"name": "qnum"})
        if select is None:
            return []
        out: list[tuple[int, str]] = []
        options = select.find_all("option")[1:-1]
        for option in options:
            raw = option.get("value")
            if not raw:
                continue
            url = self.resolve_href(str(raw).strip())
            number = int(option.get_text().strip())
            out.append((number, url))
        return out

    @property
    def converters(self) -> RacingReferenceHtmlConverters:
        return self._converters


def _resolve_racing_reference_source(source: RacingReferenceSource | None) -> RacingReferenceSource:
    if source is not None:
        return source
    from include.nascar.data_sources import DEFAULT_NASCAR_DATA_SOURCE

    return cast(RacingReferenceSource, DEFAULT_NASCAR_DATA_SOURCE)


def download_year_schedule_html(
    context: dict[str, Any],
    series_name: str,
    *,
    source: RacingReferenceSource | None = None,
) -> None:
    """Download season schedule HTML into ``schedule.html`` under the series/year path."""
    src = _resolve_racing_reference_source(source)
    year = context["data_interval_start"].year
    scraper = Scraper(src.storage_path_series_year(series_name, year))
    scraper.download(
        url=src.schedule_url(year),
        filename="schedule.html",
        user_agent=random.choice(RACING_REFERENCE_USER_AGENTS),
    )


def cup_series_download_year_schedule_html(**context: Any) -> None:
    download_year_schedule_html(context, "Cup")


def convert_year_schedule_html_to_csv(
    context: dict[str, Any],
    series_name: str,
    *,
    source: RacingReferenceSource | None = None,
) -> None:
    """Convert ``schedule.html`` to ``schedule.csv`` for the data interval year."""
    src = _resolve_racing_reference_source(source)
    year = context["data_interval_start"].year
    base = src.storage_path_series_year(series_name, year)
    html_file_path = f"{base}/schedule.html"
    csv_file_path = f"{base}/schedule.csv"
    src.converters.convert_schedule(html_file_path, csv_file_path)


def cup_series_convert_year_schedule_html_to_csv(**context: Any) -> None:
    convert_year_schedule_html_to_csv(context, "Cup")


def _sleep_download_jitter() -> None:
    time.sleep(random.randint(2, 5))


def download_race_results_html_if_missing(
    source: RacingReferenceSource,
    series_name: str,
    year: int,
    week_number: int,
    week_url: str,
) -> None:
    """Fetch ``race-results.html`` from ``week_url`` when the file is absent."""
    week_path = source.storage_path_week(series_name, year, week_number)
    race_html = os.path.join(week_path, "race-results.html")
    if os.path.isfile(race_html):
        return
    resolved = source.resolve_page_url(week_url.strip())
    if not resolved:
        logging.warning(
            "Could not resolve week_url for %s year %s week %s; skipping race results download",
            series_name,
            year,
            week_number,
        )
        return
    scraper = Scraper(week_path)
    _sleep_download_jitter()
    scraper.download(
        url=resolved,
        filename="race-results.html",
        user_agent=random.choice(RACING_REFERENCE_USER_AGENTS),
    )


def download_entrylist_html_if_missing(
    source: RacingReferenceSource,
    series_name: str,
    year: int,
    week_number: int,
    race_results_html_path: str,
) -> None:
    if not os.path.isfile(race_results_html_path):
        return
    week_path = source.storage_path_week(series_name, year, week_number)
    entry_path = os.path.join(week_path, "entrylist.html")
    if os.path.isfile(entry_path):
        return
    with open(race_results_html_path, encoding="utf-8") as html_file:
        soup = BeautifulSoup(html_file, "html.parser")
    url = source.extract_entrylist_url(soup)
    if not url:
        logging.warning("No entry list link for %s year %s week %s", series_name, year, week_number)
        return
    scraper = Scraper(week_path)
    _sleep_download_jitter()
    scraper.download(
        url=url,
        filename="entrylist.html",
        user_agent=random.choice(RACING_REFERENCE_USER_AGENTS),
    )


def download_practices_html_if_missing(
    source: RacingReferenceSource,
    series_name: str,
    year: int,
    week_number: int,
    race_results_html_path: str,
) -> list[str]:
    """Ensure practice session HTML exists; return paths to all ``practice-*.html`` files."""
    week_path = source.storage_path_week(series_name, year, week_number)
    out: list[str] = []
    if not os.path.isfile(race_results_html_path):
        return out
    with open(race_results_html_path, encoding="utf-8") as html_file:
        soup_rr = BeautifulSoup(html_file, "html.parser")
    url = source.extract_practice_results_url(soup_rr)
    if url is None:
        return out
    first = os.path.join(week_path, "practice-01.html")
    if not os.path.isfile(first):
        scraper = Scraper(week_path)
        _sleep_download_jitter()
        scraper.download(
            url=url,
            filename="practice-01.html",
            user_agent=random.choice(RACING_REFERENCE_USER_AGENTS),
        )
    if not os.path.isfile(first):
        return out
    out.append(first)
    with open(first, encoding="utf-8") as html_file:
        soup_p = BeautifulSoup(html_file, "html.parser")
    for number, session_url in source.iter_practice_session_urls(soup_p):
        fn = f"practice-{number:02d}.html"
        p = os.path.join(week_path, fn)
        if not os.path.isfile(p):
            scraper = Scraper(week_path)
            _sleep_download_jitter()
            scraper.download(
                url=session_url,
                filename=fn,
                user_agent=random.choice(RACING_REFERENCE_USER_AGENTS),
            )
        if os.path.isfile(p) and p not in out:
            out.append(p)
    return out


def download_qualifying_html_if_missing(
    source: RacingReferenceSource,
    series_name: str,
    year: int,
    week_number: int,
    race_results_html_path: str,
) -> str | None:
    week_path = source.storage_path_week(series_name, year, week_number)
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
    scraper.download(
        url=url,
        filename="qualifying.html",
        user_agent=random.choice(RACING_REFERENCE_USER_AGENTS),
    )
    return qpath if os.path.isfile(qpath) else None


def download_loop_data_html_if_missing(
    source: RacingReferenceSource,
    series_name: str,
    year: int,
    week_number: int,
    race_results_html_path: str,
) -> str | None:
    week_path = source.storage_path_week(series_name, year, week_number)
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
    scraper.download(
        url=url,
        filename="loop-data.html",
        user_agent=random.choice(RACING_REFERENCE_USER_AGENTS),
    )
    return lpath if os.path.isfile(lpath) else None


def download_penalties_html_if_missing(
    source: RacingReferenceSource,
    series_name: str,
    year: int,
    week_number: int,
    race_results_html_path: str,
) -> str | None:
    week_path = source.storage_path_week(series_name, year, week_number)
    ppath = os.path.join(week_path, "penalties.html")
    if os.path.isfile(ppath):
        return ppath
    if not os.path.isfile(race_results_html_path):
        return None
    with open(race_results_html_path, encoding="utf-8") as html_file:
        soup = BeautifulSoup(html_file, "html.parser")
    url = source.extract_penalties_url(soup)
    if url is None:
        return None
    scraper = Scraper(week_path)
    _sleep_download_jitter()
    scraper.download(
        url=url,
        filename="penalties.html",
        user_agent=random.choice(RACING_REFERENCE_USER_AGENTS),
    )
    return ppath if os.path.isfile(ppath) else None
