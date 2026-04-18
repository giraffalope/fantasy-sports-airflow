from __future__ import annotations

from unittest.mock import MagicMock, patch

import nascar.cup.scraper.importer as importer_mod
import pytest

from include.nascar.data_sources.frcs import FrcsSource
from include.nascar.data_sources.racing_reference import RacingReferenceSource


class _DummyTI:
    def __init__(self, pull_map: dict[tuple[str | None, str | None], object] | None = None) -> None:
        self.pull_map = pull_map or {}
        self.pushed: dict[str, object] = {}

    def xcom_push(self, key: str, value: object) -> None:
        self.pushed[key] = value

    def xcom_pull(self, task_ids: str | None = None, key: str | None = None) -> object:
        return self.pull_map.get((task_ids, key))


def test_load_stage_practices_persists_staged_xcom_metadata() -> None:
    ti = _DummyTI()
    kwargs = {"ti": ti}
    execute_mock = MagicMock()

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_pull_target_weeks", return_value=[(101, 5, 6)]),
        patch.object(importer_mod, "_cup_practice_session_numbers", return_value=[1]),
        patch.object(
            importer_mod,
            "_cup_practice_csv_for_session",
            return_value=("/tmp/practice-1.csv", "racing_reference"),
        ),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        importer_mod.load_stage_frcs_practices_csvs(**kwargs)

    assert execute_mock.call_args.args[3] == RacingReferenceSource().id
    assert ti.pushed["importer_staged_practices"] == [
        {
            "week_id": 101,
            "event_number": 5,
            "number": 1,
            "source_id": RacingReferenceSource().id,
            "staged_table_name": "stg_frcs_practice_101_1_202601010000",
        }
    ]


def test_import_practices_uses_staged_xcom_and_not_filesystem() -> None:
    staged_rows = [
        {
            "week_id": 77,
            "event_number": 12,
            "number": 2,
            "source_id": RacingReferenceSource().id,
            "staged_table_name": "stg_frcs_practice_77_2_202601010000",
        }
    ]
    ti = _DummyTI({("load_stage_practices_csvs", "importer_staged_practices"): staged_rows})
    kwargs = {"ti": ti}
    execute_mock = MagicMock()

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_nascar_staging_table_exists", return_value=True),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
        patch.object(
            importer_mod, "_cup_practice_session_numbers", side_effect=AssertionError("should not be called")
        ),
    ):
        importer_mod.import_frcs_practices(**kwargs)

    assert execute_mock.call_count == 1
    assert execute_mock.call_args.args[3] == RacingReferenceSource().id


def test_import_practices_skips_when_temp_entrylist_missing() -> None:
    staged_rows = [
        {
            "week_id": 77,
            "event_number": 12,
            "number": 2,
            "source_id": RacingReferenceSource().id,
            "staged_table_name": "stg_frcs_practice_77_2_202601010000",
        }
    ]
    ti = _DummyTI({("load_stage_practices_csvs", "importer_staged_practices"): staged_rows})
    kwargs = {"ti": ti}
    execute_mock = MagicMock()

    def _table_exists(_hook: object, table_name: str) -> bool:
        return table_name == "stg_frcs_practice_77_2_202601010000"

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_nascar_staging_table_exists", side_effect=_table_exists),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        importer_mod.import_frcs_practices(**kwargs)

    assert execute_mock.call_count == 0


def test_load_stage_qualifyings_persists_staged_xcom_metadata() -> None:
    ti = _DummyTI()
    kwargs = {"ti": ti}
    execute_mock = MagicMock()

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_pull_target_weeks", return_value=[(555, 8, None)]),
        patch.object(
            importer_mod,
            "_cup_qualifying_csv_path",
            return_value=("/tmp/qualifying.csv", "frcs"),
        ),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        importer_mod.load_stage_frcs_qualifyings_csvs(**kwargs)

    assert execute_mock.call_args.args[3] == FrcsSource().id
    assert ti.pushed["importer_staged_qualifyings"] == [
        {
            "week_id": 555,
            "event_number": 8,
            "source_id": FrcsSource().id,
            "staged_table_name": "stg_frcs_qualifying_555_202601010000",
        }
    ]


def test_import_qualifyings_uses_staged_xcom_and_not_filesystem() -> None:
    staged_rows = [
        {
            "week_id": 19,
            "event_number": 4,
            "source_id": RacingReferenceSource().id,
            "staged_table_name": "stg_frcs_qualifying_19_202601010000",
        }
    ]
    ti = _DummyTI({("load_stage_qualifyings_csvs", "importer_staged_qualifyings"): staged_rows})
    kwargs = {"ti": ti}
    execute_mock = MagicMock()

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_nascar_staging_table_exists", return_value=True),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
        patch.object(
            importer_mod, "_cup_qualifying_data_source_id", side_effect=AssertionError("should not be called")
        ),
    ):
        importer_mod.import_frcs_qualifyings(**kwargs)

    assert execute_mock.call_count == 1
    assert execute_mock.call_args.args[3] == RacingReferenceSource().id


def test_import_qualifyings_skips_when_temp_entrylist_missing() -> None:
    staged_rows = [
        {
            "week_id": 19,
            "event_number": 4,
            "source_id": RacingReferenceSource().id,
            "staged_table_name": "stg_frcs_qualifying_19_202601010000",
        }
    ]
    ti = _DummyTI({("load_stage_qualifyings_csvs", "importer_staged_qualifyings"): staged_rows})
    kwargs = {"ti": ti}
    execute_mock = MagicMock()

    def _table_exists(_hook: object, table_name: str) -> bool:
        return table_name == "stg_frcs_qualifying_19_202601010000"

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_nascar_staging_table_exists", side_effect=_table_exists),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        importer_mod.import_frcs_qualifyings(**kwargs)

    assert execute_mock.call_count == 0


@pytest.mark.parametrize("source_id", [FrcsSource().id, RacingReferenceSource().id])
def test_import_practices_supports_frcs_and_racing_reference_staged_sources(source_id: str) -> None:
    staged_rows = [
        {
            "week_id": 88,
            "event_number": 9,
            "number": 1,
            "source_id": source_id,
            "staged_table_name": "stg_frcs_practice_88_1_202601010000",
        }
    ]
    ti = _DummyTI({("load_stage_practices_csvs", "importer_staged_practices"): staged_rows})
    kwargs = {"ti": ti}
    execute_mock = MagicMock()

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_nascar_staging_table_exists", return_value=True),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        importer_mod.import_frcs_practices(**kwargs)

    assert execute_mock.call_count == 1
    assert execute_mock.call_args.args[3] == source_id


@pytest.mark.parametrize("source_id", [FrcsSource().id, RacingReferenceSource().id])
def test_import_qualifyings_supports_frcs_and_racing_reference_staged_sources(source_id: str) -> None:
    staged_rows = [
        {
            "week_id": 88,
            "event_number": 9,
            "source_id": source_id,
            "staged_table_name": "stg_frcs_qualifying_88_202601010000",
        }
    ]
    ti = _DummyTI({("load_stage_qualifyings_csvs", "importer_staged_qualifyings"): staged_rows})
    kwargs = {"ti": ti}
    execute_mock = MagicMock()

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_nascar_staging_table_exists", return_value=True),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        importer_mod.import_frcs_qualifyings(**kwargs)

    assert execute_mock.call_count == 1
    assert execute_mock.call_args.args[3] == source_id


@pytest.mark.parametrize(
    ("fn_name", "stg_name"),
    [
        ("import_frcs_penalties", "stg_frcs_penalties_101_202601010000"),
        ("import_frcs_pitstops", "stg_frcs_pit_stops_detail_101_202601010000"),
        ("import_frcs_draftkings", "stg_frcs_draftkings_101_202601010000"),
        ("import_frcs_fanduel", "stg_frcs_fanduel_101_202601010000"),
        ("import_frcs_lineup", "stg_frcs_lineup_101_202601010000"),
    ],
)
def test_import_tasks_execute_from_staging_when_csv_missing(fn_name: str, stg_name: str) -> None:
    kwargs = {"ti": _DummyTI()}
    execute_mock = MagicMock()

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_pull_target_weeks", return_value=[(101, 7, None)]),
        patch.object(importer_mod.os.path, "isfile", return_value=False),
        patch.object(
            importer_mod,
            "_nascar_staging_table_exists",
            side_effect=lambda _h, table_name: table_name == stg_name,
        ),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        getattr(importer_mod, fn_name)(**kwargs)

    assert execute_mock.call_count == 1


def test_import_races_executes_when_both_required_csvs_exist() -> None:
    kwargs = {"ti": _DummyTI()}
    execute_mock = MagicMock()

    def _isfile(path: str) -> bool:
        return path.endswith("race-results.csv") or path.endswith("loop-data.csv")

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_pull_target_weeks", return_value=[(101, 7, None)]),
        patch.object(importer_mod.os.path, "isfile", side_effect=_isfile),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        importer_mod.import_frcs_races(**kwargs)

    assert execute_mock.call_count == 1


def test_import_races_skips_when_temp_entrylist_missing() -> None:
    kwargs = {"ti": _DummyTI()}
    execute_mock = MagicMock()

    def _isfile(path: str) -> bool:
        return path.endswith("race-results.csv") or path.endswith("loop-data.csv")

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_pull_target_weeks", return_value=[(101, 7, None)]),
        patch.object(importer_mod.os.path, "isfile", side_effect=_isfile),
        patch.object(importer_mod, "_nascar_staging_table_exists", return_value=False),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        importer_mod.import_frcs_races(**kwargs)

    assert execute_mock.call_count == 0


@pytest.mark.parametrize(
    "fn_name",
    [
        "load_stage_frcs_penalties_csvs",
        "load_stage_frcs_pitstops_csvs",
        "load_stage_frcs_draftkings_csvs",
        "load_stage_frcs_fanduel_csvs",
    ],
)
def test_load_stage_single_file_tasks_execute_when_csv_exists(fn_name: str) -> None:
    kwargs = {"ti": _DummyTI()}
    execute_mock = MagicMock()

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_pull_target_weeks", return_value=[(101, 7, None)]),
        patch.object(importer_mod.os.path, "isfile", return_value=True),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        getattr(importer_mod, fn_name)(**kwargs)

    assert execute_mock.call_count == 1


def test_load_stage_races_executes_when_both_csvs_exist() -> None:
    kwargs = {"ti": _DummyTI()}
    execute_mock = MagicMock()

    def _isfile(path: str) -> bool:
        return path.endswith("race-results.csv") or path.endswith("loop-data.csv")

    with (
        patch.object(importer_mod, "NascarMySqlHook", return_value=MagicMock()),
        patch.object(importer_mod, "_importer_table_suffix", return_value="202601010000"),
        patch.object(importer_mod, "_context_year", return_value=2020),
        patch.object(importer_mod, "_pull_target_weeks", return_value=[(101, 7, None)]),
        patch.object(importer_mod.os.path, "isfile", side_effect=_isfile),
        patch.object(importer_mod, "_execute_templated_sql_file", execute_mock),
        patch.object(importer_mod, "_emit_cup_importer_domain_event", MagicMock()),
    ):
        importer_mod.load_stage_frcs_races_csvs(**kwargs)

    assert execute_mock.call_count == 1
