"""Unit tests for backfill DAG registration helpers."""

from __future__ import annotations

import importlib

import pytest
from nascar.cup.scraper import backfill_assets


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, False),
        ("", False),
        ("0", False),
        ("false", False),
        ("FALSE", False),
        ("1", True),
        ("true", True),
        ("yes", True),
        ("on", True),
        ("ON", True),
    ],
)
def test_backfill_dags_enabled(
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
    expected: bool,
) -> None:
    if value is None:
        monkeypatch.delenv("FS_ENABLE_BACKFILL_DAGS", raising=False)
    else:
        monkeypatch.setenv("FS_ENABLE_BACKFILL_DAGS", value)
    assert backfill_assets.backfill_dags_enabled() is expected


def test_schedule_module_skips_backfill_dag_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FS_ENABLE_BACKFILL_DAGS", raising=False)
    import nascar.cup.scraper.frcs.schedule as schedule_module

    importlib.reload(schedule_module)
    try:
        assert schedule_module.backfill_dag is None
        assert schedule_module.dag is not None
    finally:
        monkeypatch.setenv("FS_ENABLE_BACKFILL_DAGS", "1")
        importlib.reload(schedule_module)
