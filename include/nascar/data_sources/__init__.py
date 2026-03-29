"""NASCAR scrape sources: register defaults and construct sources from config."""

from include.nascar.data_sources.base import HtmlConverters, NascarScrapeSource
from include.nascar.data_sources.racing_reference import (
    DEFAULT_NASCAR_SCRAPER_ROOT,
    RACING_REFERENCE_USER_AGENTS,
    RacingReferenceHtmlConverters,
    RacingReferenceSource,
    convert_year_schedule_html_to_csv,
    cup_series_convert_year_schedule_html_to_csv,
    cup_series_download_year_schedule_html,
    download_year_schedule_html,
)

# Single place to change which site the DAG uses (or read from env / Airflow Variable).
DEFAULT_NASCAR_DATA_SOURCE: NascarScrapeSource = RacingReferenceSource()

__all__ = [
    "DEFAULT_NASCAR_SCRAPER_ROOT",
    "DEFAULT_NASCAR_DATA_SOURCE",
    "HtmlConverters",
    "NascarScrapeSource",
    "RACING_REFERENCE_USER_AGENTS",
    "RacingReferenceHtmlConverters",
    "RacingReferenceSource",
    "convert_year_schedule_html_to_csv",
    "cup_series_convert_year_schedule_html_to_csv",
    "cup_series_download_year_schedule_html",
    "download_year_schedule_html",
]
