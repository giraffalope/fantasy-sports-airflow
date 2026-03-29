"""www.racing-reference.info — schedule, results, and related pages."""

from __future__ import annotations

from bs4 import BeautifulSoup

from include.nascar.html_to_csv_converter import HtmlToCsvConverter

# Default layout: /home/airflow/scraper/nascar/<host>/… — mount host scraper-data at /home/airflow/scraper
# so the nascar directory appears on the host under scraper-data/nascar/.
DEFAULT_NASCAR_SCRAPER_ROOT = "/home/airflow/scraper/nascar"


class RacingReferenceHtmlConverters:
    """Delegates to HtmlToCsvConverter (Racing Reference markup)."""

    def convert_schedule(self, html_file_path: str, csv_file_path: str) -> None:
        HtmlToCsvConverter.convert_schedule(html_file_path, csv_file_path)

    def convert_race_results(self, html_file_path: str, csv_file_path: str) -> None:
        HtmlToCsvConverter.convert_race_results(html_file_path, csv_file_path)

    def convert_prerace_penalties(self, html_file_path: str, csv_file_path: str) -> None:
        HtmlToCsvConverter.convert_prerace_penalties(html_file_path, csv_file_path)

    def convert_caution_flags(self, html_file_path: str, csv_file_path: str) -> None:
        HtmlToCsvConverter.convert_caution_flags(html_file_path, csv_file_path)

    def convert_lap_leaders(self, html_file_path: str, csv_file_path: str) -> None:
        HtmlToCsvConverter.convert_lap_leaders(html_file_path, csv_file_path)

    def convert_entrylist(self, html_file_path: str, csv_file_path: str) -> None:
        HtmlToCsvConverter.convert_entrylist(html_file_path, csv_file_path)

    def convert_practices(self, paths: list[str]) -> None:
        HtmlToCsvConverter.convert_practices(paths)

    def convert_qualifying(self, path: str | None) -> None:
        HtmlToCsvConverter.convert_qualifying(path)

    def convert_loop_data(self, path: str | None) -> None:
        HtmlToCsvConverter.convert_loop_data(path)

    def convert_penalties(self, path: str | None) -> None:
        HtmlToCsvConverter.convert_penalties(path)


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
