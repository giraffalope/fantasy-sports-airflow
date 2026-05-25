"""Unit tests for snapshot_build environment helpers."""

from __future__ import annotations

from include.nascar.ml.snapshot_build import _extra_rebuild_week_ids_from_env


def test_extra_rebuild_week_ids_empty_when_unset(monkeypatch: object) -> None:
    monkeypatch.delenv("FS_ML_REBUILD_WEEK_IDS", raising=False)  # type: ignore[attr-defined]
    assert _extra_rebuild_week_ids_from_env() == set()


def test_extra_rebuild_week_ids_empty_when_blank(monkeypatch: object) -> None:
    monkeypatch.setenv("FS_ML_REBUILD_WEEK_IDS", "  ,  , ")  # type: ignore[attr-defined]
    assert _extra_rebuild_week_ids_from_env() == set()


def test_extra_rebuild_week_ids_parses_comma_list(monkeypatch: object) -> None:
    monkeypatch.setenv("FS_ML_REBUILD_WEEK_IDS", "1021, 1022 ,1023")  # type: ignore[attr-defined]
    assert _extra_rebuild_week_ids_from_env() == {1021, 1022, 1023}
