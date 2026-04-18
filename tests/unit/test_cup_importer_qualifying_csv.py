"""Tests for Cup importer qualifying CSV resolution (FRCS vs Racing Reference)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import nascar.cup.scraper.importer as importer_mod

from include.nascar.enums import NascarSeries


def test_cup_qualifying_csv_path_prefers_frcs() -> None:
    frcs_csv = "/scraper/frcs.pro/cup/2020/05/qualifying.csv"
    mock_frcs = MagicMock()
    mock_frcs.storage_path_week.return_value = "/scraper/frcs.pro/cup/2020/05"
    mock_rr = MagicMock()
    mock_rr.storage_path_week.return_value = "/scraper/www.racing-reference.info/cup/2020/05"

    def isfile(p: str) -> bool:
        return p == frcs_csv

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.os.path, "isfile", side_effect=isfile),
    ):
        path, src = importer_mod._cup_qualifying_csv_path(NascarSeries.CUP, 2020, 5, None)
    assert path == frcs_csv
    assert src == "frcs"


def test_cup_qualifying_csv_path_falls_back_to_racing_reference() -> None:
    rr_csv = "/scraper/www.racing-reference.info/cup/2010/01/qualifying.csv"
    mock_frcs = MagicMock()
    mock_frcs.storage_path_week.return_value = "/scraper/frcs.pro/cup/2010/01"
    mock_rr = MagicMock()
    mock_rr.storage_path_week.return_value = "/scraper/www.racing-reference.info/cup/2010/01"

    def isfile(p: str) -> bool:
        return p == rr_csv

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.os.path, "isfile", side_effect=isfile),
    ):
        path, src = importer_mod._cup_qualifying_csv_path(NascarSeries.CUP, 2010, 1, None)
    assert path == rr_csv
    assert src == "racing_reference"


def test_cup_qualifying_csv_path_rr_mapped_folder() -> None:
    mock_frcs = MagicMock()
    mock_rr = MagicMock()
    mock_frcs.storage_path_week.side_effect = lambda _s, y, n: f"/frcs/cup/{y}/{n:02d}"
    mock_rr.storage_path_week.side_effect = lambda _s, y, n: f"/rr/cup/{y}/{n:02d}"
    rr_csv = "/rr/cup/2010/06/qualifying.csv"

    def isfile(p: str) -> bool:
        return p == rr_csv

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.os.path, "isfile", side_effect=isfile),
    ):
        path, src = importer_mod._cup_qualifying_csv_path(NascarSeries.CUP, 2010, 5, 6)
    assert path == rr_csv
    assert src == "racing_reference"


def test_cup_qualifying_csv_path_none_when_both_missing() -> None:
    mock_frcs = MagicMock()
    mock_frcs.storage_path_week.return_value = "/scraper/frcs.pro/cup/2020/01"
    mock_rr = MagicMock()
    mock_rr.storage_path_week.return_value = "/scraper/www.racing-reference.info/cup/2020/01"

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.os.path, "isfile", return_value=False),
    ):
        path, src = importer_mod._cup_qualifying_csv_path(NascarSeries.CUP, 2020, 1, None)
    assert path is None
    assert src == ""
