from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup


class CsvReader:
    def __init__(self, html: str):
        """
        Initialize with raw HTML content as a string.
        """
        self.html = html
        self.soup: BeautifulSoup | None = None
        self.data: list[Any] = []

    @staticmethod
    def insert_practice_data(csv_file_path):
        raise NotImplementedError(
            "insert_practice_data is not implemented; was incomplete (temporary table / SQL TBD). "
            f"csv_file_path={csv_file_path!r}"
        )
