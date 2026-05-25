"""Typing helpers for MySQL cursor objects used in transactional writes."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol


class DbCursor(Protocol):
    def execute(self, sql: str, params: Sequence[Any] | None = ...) -> None: ...

    def executemany(self, sql: str, params: Sequence[Sequence[Any]]) -> None: ...

    @property
    def lastrowid(self) -> int: ...
