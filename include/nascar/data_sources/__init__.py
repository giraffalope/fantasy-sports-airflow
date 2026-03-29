"""NASCAR scrape sources: register defaults and construct sources from config."""

from include.nascar.data_sources.base import HtmlConverters, NascarScrapeSource
from include.nascar.data_sources.racing_reference import (
    DEFAULT_NASCAR_SCRAPER_ROOT,
    RacingReferenceHtmlConverters,
    RacingReferenceSource,
)

# Single place to change which site the DAG uses (or read from env / Airflow Variable).
DEFAULT_NASCAR_DATA_SOURCE: NascarScrapeSource = RacingReferenceSource()

__all__ = [
    "DEFAULT_NASCAR_SCRAPER_ROOT",
    "DEFAULT_NASCAR_DATA_SOURCE",
    "HtmlConverters",
    "NascarScrapeSource",
    "RacingReferenceHtmlConverters",
    "RacingReferenceSource",
]
