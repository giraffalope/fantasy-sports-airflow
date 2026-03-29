"""
Pluggable NASCAR scrape sources.

A source describes where pages live (URLs, on-disk layout) and how HTML is turned
into the CSV shapes expected by the load SQL. Add a new site by implementing
NascarScrapeSource (and typically a matching HtmlConverters implementation).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from bs4 import BeautifulSoup


@runtime_checkable
class HtmlConverters(Protocol):
    """HTML → CSV steps used by the nascar scraper DAG."""

    def convert_schedule(self, html_file_path: str, csv_file_path: str) -> None: ...

    def convert_race_results(self, html_file_path: str, csv_file_path: str) -> None: ...

    def convert_prerace_penalties(self, html_file_path: str, csv_file_path: str) -> None: ...

    def convert_caution_flags(self, html_file_path: str, csv_file_path: str) -> None: ...

    def convert_lap_leaders(self, html_file_path: str, csv_file_path: str) -> None: ...

    def convert_entrylist(self, html_file_path: str, csv_file_path: str) -> None: ...

    def convert_practices(self, paths: list[str]) -> None: ...

    def convert_qualifying(self, path: str | None) -> None: ...

    def convert_loop_data(self, path: str | None) -> None: ...

    def convert_penalties(self, path: str | None) -> None: ...


@runtime_checkable
class NascarScrapeSource(Protocol):
    """
    One external site (or API) that provides schedule, results, and related pages.

    Implementations should keep site-specific URL rules and HTML link discovery here;
    parsing details can live in a dedicated HtmlConverters type.
    """

    @property
    def id(self) -> str:
        """Stable identifier, e.g. for logging or config (not necessarily the DB schema)."""
        ...

    @property
    def base_url(self) -> str: ...

    def scraper_root_segment(self) -> str:
        """Directory name under the NASCAR scraper root (e.g. hostname)."""
        ...

    def storage_path_series_year(self, series_name: str, year: int) -> str: ...

    def storage_path_week(self, series_name: str, year: int, week_number: int) -> str: ...

    def schedule_url(self, year: int) -> str: ...

    def resolve_href(self, href: str) -> str: ...

    def resolve_page_url(self, href: str | None) -> str | None:
        """Normalize a link from schedule/CSV (may be relative) to an absolute URL."""
        ...

    def extract_entrylist_url(self, soup: BeautifulSoup) -> str | None: ...

    def extract_practice_results_url(self, soup: BeautifulSoup) -> str | None: ...

    def extract_qualifying_url(self, soup: BeautifulSoup) -> str | None: ...

    def extract_loopdata_url(self, soup: BeautifulSoup) -> str | None: ...

    def extract_penalties_url(self, soup: BeautifulSoup) -> str | None: ...

    def iter_practice_session_urls(self, practice_index_soup: BeautifulSoup) -> list[tuple[int, str]]:
        """
        From the first practice-results page, return (session_number, absolute_url) pairs.
        Empty list if the page has no session selector.
        """
        ...

    @property
    def converters(self) -> HtmlConverters: ...
