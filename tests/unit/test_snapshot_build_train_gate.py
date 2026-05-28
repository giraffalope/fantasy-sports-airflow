"""Unit tests for the up-to-date skip gate in ``build_all_week_training_snapshots``.

The gate keys on ``ml_training_snapshots.source_pipeline_version`` matching the
``pipeline_version`` we're about to stamp. When every finished week already has
a current-version snapshot, the per-week builder is never called and the task
becomes a near-noop.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock, call, patch

for _name in (
    "airflow",
    "airflow.providers",
    "airflow.providers.mysql",
    "airflow.providers.mysql.hooks",
    "airflow.providers.mysql.hooks.mysql",
):
    sys.modules.setdefault(_name, MagicMock())

import include.nascar.ml.snapshot_build as snapshot_build  # noqa: E402
from include.nascar.ml.snapshot_build import (  # noqa: E402
    _train_weeks_with_current_version,
)
from include.nascar.ml.snapshot_config import (  # noqa: E402
    DRAFTKINGS_DFS_ID,
    GRAIN_CAR_DRIVER,
    SNAPSHOT_KIND_TRAIN,
)


_RULES_ID = 42


def _make_hook(
    *,
    finished_week_ids: set[int],
    already_built_week_ids: set[int],
) -> MagicMock:
    """Build a mock hook that answers both gate queries."""
    hook = MagicMock()

    def get_records(sql: str, parameters: Any = ()) -> list[tuple]:
        text = sql.lower()
        if "from nascar.dfs_scoring_rules" in text:
            return [(_RULES_ID, "{}")]
        if "from nascar.ml_training_snapshots" in text:
            return [(week_id,) for week_id in sorted(already_built_week_ids)]
        # FINISHED_AGG_WEEKS_SQL: joins agg_week_results -> weeks -> seasons.
        if (
            "from nascar.agg_week_results" in text
            and "join nascar.weeks" in text
            and "join nascar.seasons" in text
        ):
            return [(1, week_id) for week_id in sorted(finished_week_ids)]
        return []

    hook.get_records.side_effect = get_records
    return hook


def test_train_weeks_with_current_version_sql_uses_null_safe_equality() -> None:
    hook = MagicMock()
    hook.get_records.return_value = []

    result = _train_weeks_with_current_version(
        hook,
        series_id=1,
        rules_id=_RULES_ID,
        pipeline_version="abc123",
    )

    assert result == set()
    hook.get_records.assert_called_once()
    sql = hook.get_records.call_args.args[0]
    params = hook.get_records.call_args.kwargs.get("parameters") or (
        hook.get_records.call_args.args[1]
        if len(hook.get_records.call_args.args) > 1
        else ()
    )
    assert "source_pipeline_version <=> %s" in sql
    assert "deleted_at IS NULL" in sql
    assert params == (1, GRAIN_CAR_DRIVER, SNAPSHOT_KIND_TRAIN, _RULES_ID, "abc123")


def test_train_weeks_with_current_version_returns_int_set() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(100,), (200,), (300,)]

    result = _train_weeks_with_current_version(
        hook,
        series_id=1,
        rules_id=_RULES_ID,
        pipeline_version=None,
    )

    assert result == {100, 200, 300}
    params = hook.get_records.call_args.kwargs.get("parameters") or (
        hook.get_records.call_args.args[1]
    )
    assert params[-1] is None  # the NULL on the python side


@patch("include.nascar.ml.snapshot_build.build_car_driver_training_snapshot")
@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_skips_when_all_versions_match(
    mock_hook_cls: MagicMock,
    mock_build: MagicMock,
    monkeypatch: Any,
) -> None:
    monkeypatch.delenv("FS_ML_REBUILD_WEEK_IDS", raising=False)
    mock_hook_cls.return_value = _make_hook(
        finished_week_ids={1, 2, 3},
        already_built_week_ids={1, 2, 3},
    )

    result = snapshot_build.build_all_week_training_snapshots(pipeline_version="abc")

    assert result == []
    mock_build.assert_not_called()


@patch("include.nascar.ml.snapshot_build.build_car_driver_training_snapshot")
@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_rebuilds_only_new_weeks(
    mock_hook_cls: MagicMock,
    mock_build: MagicMock,
    monkeypatch: Any,
) -> None:
    monkeypatch.delenv("FS_ML_REBUILD_WEEK_IDS", raising=False)
    mock_hook_cls.return_value = _make_hook(
        finished_week_ids={1, 2, 3, 4},
        already_built_week_ids={1, 2},
    )
    mock_build.side_effect = [101, 102]

    result = snapshot_build.build_all_week_training_snapshots(pipeline_version="abc")

    assert result == [101, 102]
    assert mock_build.call_count == 2
    rebuilt_weeks = sorted(
        c.args[1] for c in mock_build.call_args_list
    )
    assert rebuilt_weeks == [3, 4]
    for c in mock_build.call_args_list:
        assert c.kwargs.get("kind") == SNAPSHOT_KIND_TRAIN
        assert c.kwargs.get("pipeline_version") == "abc"


@patch("include.nascar.ml.snapshot_build.build_car_driver_training_snapshot")
@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_force_rebuild_overrides_version_match(
    mock_hook_cls: MagicMock,
    mock_build: MagicMock,
    monkeypatch: Any,
) -> None:
    monkeypatch.setenv("FS_ML_REBUILD_WEEK_IDS", "1")
    mock_hook_cls.return_value = _make_hook(
        finished_week_ids={1, 2},
        already_built_week_ids={1, 2},
    )
    mock_build.return_value = 999

    result = snapshot_build.build_all_week_training_snapshots(pipeline_version="abc")

    assert result == [999]
    mock_build.assert_called_once()
    rebuilt_week = mock_build.call_args.args[1]
    assert rebuilt_week == 1


@patch("include.nascar.ml.snapshot_build.build_car_driver_training_snapshot")
@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_null_pipeline_version_matches_null_snapshot(
    mock_hook_cls: MagicMock,
    mock_build: MagicMock,
    monkeypatch: Any,
) -> None:
    """Dev envs without FS_PIPELINE_VERSION/GIT_COMMIT should still see a hit.

    Validates that the helper passes ``None`` through to the SQL layer unchanged
    (so MariaDB's NULL-safe ``<=>`` matches rows whose ``source_pipeline_version``
    is also NULL) and that the gate then short-circuits the entire loop.
    """
    monkeypatch.delenv("FS_ML_REBUILD_WEEK_IDS", raising=False)
    hook = _make_hook(
        finished_week_ids={5, 6},
        already_built_week_ids={5, 6},
    )
    mock_hook_cls.return_value = hook

    result = snapshot_build.build_all_week_training_snapshots(pipeline_version=None)

    assert result == []
    mock_build.assert_not_called()

    train_query_calls = [
        c for c in hook.get_records.mock_calls
        if "ml_training_snapshots" in str(c.args[0]).lower()
    ]
    assert len(train_query_calls) == 1
    params = train_query_calls[0].kwargs.get("parameters") or (
        train_query_calls[0].args[1]
        if len(train_query_calls[0].args) > 1
        else ()
    )
    assert params[-1] is None


@patch("include.nascar.ml.snapshot_build.build_car_driver_training_snapshot")
@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_no_active_rules_returns_empty_without_querying_gate(
    mock_hook_cls: MagicMock,
    mock_build: MagicMock,
    monkeypatch: Any,
) -> None:
    """Pre-existing no-rules guard must still short-circuit before the new gate."""
    monkeypatch.delenv("FS_ML_REBUILD_WEEK_IDS", raising=False)
    hook = MagicMock()

    def get_records(sql: str, parameters: Any = ()) -> list[tuple]:
        if "from nascar.dfs_scoring_rules" in sql.lower():
            return []
        raise AssertionError(f"unexpected query before rules check: {sql}")

    hook.get_records.side_effect = get_records
    mock_hook_cls.return_value = hook

    result = snapshot_build.build_all_week_training_snapshots(pipeline_version="abc")

    assert result == []
    mock_build.assert_not_called()


# Silence unused-import lint for `call` (kept for ergonomic future test edits).
_ = call
