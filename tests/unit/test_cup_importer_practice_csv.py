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
        nums = importer_mod._cup_practice_session_numbers(NascarSeries.CUP, 2020, 5, None)
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
        path, src = importer_mod._cup_practice_csv_for_session(NascarSeries.CUP, 2020, 5, 1, None)
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
        path, src = importer_mod._cup_practice_csv_for_session(NascarSeries.CUP, 2010, 1, 2, None)
    assert path == rr_p
    assert src == "racing_reference"


def test_cup_practice_session_numbers_rr_folder_differs_from_frcs_event() -> None:
    """RR ``storage_path_week`` uses mapped folder index (e.g. 06) while FRCS uses event 05."""
    mock_frcs = MagicMock()
    mock_rr = MagicMock()
    mock_frcs.storage_path_week.side_effect = lambda _s, y, n: f"/frcs/cup/{y}/{n:02d}"
    mock_rr.storage_path_week.side_effect = lambda _s, y, n: f"/rr/cup/{y}/{n:02d}"

    def glob_side_effect(pattern: str) -> list[str]:
        if "/frcs/" in pattern and "/2020/05" in pattern:
            return ["/frcs/cup/2020/05/practice-1.csv"]
        if "/rr/" in pattern and "/2020/06" in pattern:
            return ["/rr/cup/2020/06/practice-2.csv"]
        return []

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.glob, "glob", side_effect=glob_side_effect),
    ):
        nums = importer_mod._cup_practice_session_numbers(NascarSeries.CUP, 2020, 5, 6)
    assert nums == [1, 2]
    mock_rr.storage_path_week.assert_called()
    assert mock_rr.storage_path_week.call_args[0][2] == 6


def test_cup_practice_csv_for_session_rr_uses_mapped_folder() -> None:
    mock_frcs = MagicMock()
    mock_rr = MagicMock()
    mock_frcs.storage_path_week.side_effect = lambda _s, y, n: f"/frcs/cup/{y}/{n:02d}"
    mock_rr.storage_path_week.side_effect = lambda _s, y, n: f"/rr/cup/{y}/{n:02d}"
    rr_p = "/rr/cup/2020/06/practice-1.csv"

    def isfile(p: str) -> bool:
        return p == rr_p

    with (
        patch.object(importer_mod, "FrcsSource", return_value=mock_frcs),
        patch.object(importer_mod, "RacingReferenceSource", return_value=mock_rr),
        patch.object(importer_mod.os.path, "isfile", side_effect=isfile),
    ):
        path, src = importer_mod._cup_practice_csv_for_session(NascarSeries.CUP, 2020, 5, 1, 6)
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
        path, src = importer_mod._cup_practice_csv_for_session(NascarSeries.CUP, 2020, 1, 1, None)
    assert path is None
    assert src == ""
