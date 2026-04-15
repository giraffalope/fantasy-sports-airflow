"""Tests for Cup importer practice CSV resolution (FRCS vs Racing Reference, session union)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import nascar.cup.scraper.importer as importer_mod

from include.nascar.enums import NascarSeries


def test_cup_practice_session_numbers_unions_frcs_and_rr() -> None:
    mock_frcs = MagicMock()
    mock_rr = MagicMock()
    mock_frcs.storage_path_week.return_value = "/scraper/frcs.pro/cup/2020/05"
    mock_rr.storage_path_week.return_value = "/scraper/www.racing-reference.info/cup/2020/05"

    def glob_side_effect(pattern: str) -> list[str]:
        if "frcs.pro" in pattern:
            return [
                "/scraper/frcs.pro/cup/2020/05/practice-1.csv",
                "/scraper/frcs.pro/cup/2020/05/practice-3.csv",
            ]
        return [
            "/scraper/www.racing-reference.info/cup/2020/05/practice-2.csv",
            "/scraper/www.racing-reference.info/cup/2020/05/practice-3.csv",
        ]

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.glob, "glob", side_effect=glob_side_effect),
    ):
        nums = importer_mod._cup_practice_session_numbers(NascarSeries.CUP, 2020, 5)
    assert nums == [1, 2, 3]


def test_cup_practice_csv_for_session_prefers_frcs() -> None:
    frcs_p = "/scraper/frcs.pro/cup/2020/05/practice-1.csv"
    rr_p = "/scraper/www.racing-reference.info/cup/2020/05/practice-1.csv"
    mock_frcs = MagicMock()
    mock_frcs.storage_path_week.return_value = "/scraper/frcs.pro/cup/2020/05"
    mock_rr = MagicMock()
    mock_rr.storage_path_week.return_value = "/scraper/www.racing-reference.info/cup/2020/05"

    def isfile(p: str) -> bool:
        return p in (frcs_p, rr_p)

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.os.path, "isfile", side_effect=isfile),
    ):
        path, src = importer_mod._cup_practice_csv_for_session(NascarSeries.CUP, 2020, 5, 1)
    assert path == frcs_p
    assert src == "frcs"


def test_cup_practice_csv_for_session_falls_back_to_racing_reference() -> None:
    rr_p = "/scraper/www.racing-reference.info/cup/2010/01/practice-2.csv"
    mock_frcs = MagicMock()
    mock_frcs.storage_path_week.return_value = "/scraper/frcs.pro/cup/2010/01"
    mock_rr = MagicMock()
    mock_rr.storage_path_week.return_value = "/scraper/www.racing-reference.info/cup/2010/01"

    def isfile(p: str) -> bool:
        return p == rr_p

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.os.path, "isfile", side_effect=isfile),
    ):
        path, src = importer_mod._cup_practice_csv_for_session(NascarSeries.CUP, 2010, 1, 2)
    assert path == rr_p
    assert src == "racing_reference"


def test_cup_practice_csv_for_session_none_when_both_missing() -> None:
    mock_frcs = MagicMock()
    mock_frcs.storage_path_week.return_value = "/scraper/frcs.pro/cup/2020/01"
    mock_rr = MagicMock()
    mock_rr.storage_path_week.return_value = "/scraper/www.racing-reference.info/cup/2020/01"

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.os.path, "isfile", return_value=False),
    ):
        path, src = importer_mod._cup_practice_csv_for_session(NascarSeries.CUP, 2020, 1, 1)
    assert path is None
    assert src == ""
