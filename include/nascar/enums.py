"""Domain enums for NASCAR scraping and database alignment."""

from __future__ import annotations

from enum import Enum
from typing import NamedTuple


class _NascarSeriesRow(NamedTuple):
    """``id`` must match ``nascar.series.id`` for ``db_name`` in your database."""

    id: int
    db_name: str


class NascarSeries(Enum):
    """Top-level NASCAR series used by scrapers and load SQL."""

    CUP = _NascarSeriesRow(1, "Cup")

    @property
    def series_id(self) -> int:
        return self.value.id

    @property
    def db_name(self) -> str:
        return self.value.db_name
