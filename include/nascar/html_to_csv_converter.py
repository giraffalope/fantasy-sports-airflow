"""Backward-compatible name for Racing Reference HTML parsers."""

from include.nascar.data_sources.racing_reference_html_to_csv import RacingReferenceHtmlToCsv

# Historical import path; implementations live in ``data_sources.racing_reference_html_to_csv``.
HtmlToCsvConverter = RacingReferenceHtmlToCsv

__all__ = ["HtmlToCsvConverter"]
