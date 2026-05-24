"""Unit tests for the ``kind='predict'`` branch of ``build_car_driver_training_snapshot``."""

from __future__ import annotations

import json
import sys
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

for _name in (
    "airflow",
    "airflow.providers",
    "airflow.providers.mysql",
    "airflow.providers.mysql.hooks",
    "airflow.providers.mysql.hooks.mysql",
):
    sys.modules.setdefault(_name, MagicMock())

import include.nascar.ml.snapshot_build as snapshot_build
from include.nascar.ml.snapshot_config import (
    SNAPSHOT_KIND_PREDICT,
    SNAPSHOT_KIND_TRAIN,
)

_DK_RULES = {
    "version": 1,
    "components": [
        {
            "name": "finish",
            "source": "r_finish_position",
            "type": "finish_table",
            "table": {},
            "formula": {"base": 41, "subtract_position": True, "min": 0},
        },
        {
            "name": "fastest_laps",
            "source": "r_total_fastest_laps",
            "type": "linear",
            "multiplier": 0.45,
        },
        {
            "name": "laps_led",
            "source": "r_total_laps_led",
            "type": "linear",
            "multiplier": 0.25,
        },
    ],
}


def _anchor_tuple(*, finish_position: int | None, start_position: int = 5) -> tuple:
    payload = [
        1,            # id
        1,            # series_id
        2025,         # season_id
        100,          # week_id
        7,            # track_id
        9,            # race_id
        101,          # car_driver_id
        201,          # driver_id
        301,          # car_id
        401,          # team_id
        501,          # manufacturer_id
        601,          # car_generation_id
        701,          # car_generation_model_id
        start_position,  # r_start_position
        finish_position,  # r_finish_position
        None,         # r_place_diff
    ]
    payload.extend([None] * 21)
    assert len(payload) == 37
    return tuple(payload)


@pytest.fixture(autouse=True)
def _clear_metric_columns_cache():
    snapshot_build._METRIC_COLUMNS_CACHE.clear()
    yield
    snapshot_build._METRIC_COLUMNS_CACHE.clear()


def _make_hook(anchor_rows: list[tuple]) -> MagicMock:
    hook = MagicMock()

    def get_records(sql: str, parameters: Any = ()) -> list[tuple]:
        text = sql.lower()
        if "from nascar.dfs_scoring_rules" in text:
            return [(1, json.dumps(_DK_RULES))]
        if "information_schema" in text:
            return []
        if "from nascar.agg_week_results awr" in text:
            return anchor_rows
        if "select id from nascar.ml_training_snapshots" in text:
            return [(42,)]
        return []

    hook.get_records.side_effect = get_records

    conn_cm = MagicMock()
    cursor_cm = MagicMock()
    cursor = MagicMock()
    cursor.description = None
    cursor.fetchall.return_value = []
    cursor.nextset.return_value = False
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = False
    conn = MagicMock()
    conn.cursor.return_value = cursor_cm
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    hook.get_conn.return_value = conn_cm

    return hook


@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_predict_kind_inserts_rows_but_skips_labels(mock_hook_cls: MagicMock) -> None:
    hook = _make_hook([_anchor_tuple(finish_position=None)])
    mock_hook_cls.return_value = hook

    snapshot_id = snapshot_build.build_car_driver_training_snapshot(
        1,
        100,
        kind=SNAPSHOT_KIND_PREDICT,
    )

    assert snapshot_id == 42

    run_sqls = [str(call.args[0]) for call in hook.run.mock_calls]
    assert any("INSERT INTO nascar.ml_training_snapshots" in s for s in run_sqls)
    assert any("INSERT INTO nascar.ml_training_rows" in s for s in run_sqls)
    assert not any("INSERT INTO nascar.ml_labels" in s for s in run_sqls)


@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_predict_kind_uses_predict_anchor_filter(mock_hook_cls: MagicMock) -> None:
    hook = _make_hook([_anchor_tuple(finish_position=None)])
    mock_hook_cls.return_value = hook

    snapshot_build.build_car_driver_training_snapshot(
        1,
        100,
        kind=SNAPSHOT_KIND_PREDICT,
    )

    anchor_sqls = [
        str(call.args[0])
        for call in hook.get_records.mock_calls
        if "FROM nascar.agg_week_results" in str(call.args[0])
    ]
    assert anchor_sqls, "expected at least one anchor query"
    anchor_sql = anchor_sqls[0]
    assert "r_start_position IS NOT NULL" in anchor_sql
    assert "r_finish_position IS NULL" in anchor_sql


@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_train_kind_still_writes_labels(mock_hook_cls: MagicMock) -> None:
    hook = _make_hook([_anchor_tuple(finish_position=12)])
    mock_hook_cls.return_value = hook

    snapshot_build.build_car_driver_training_snapshot(
        1,
        100,
        kind=SNAPSHOT_KIND_TRAIN,
    )

    run_sqls = [str(call.args[0]) for call in hook.run.mock_calls]
    assert any("INSERT INTO nascar.ml_labels" in s for s in run_sqls)


@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_unknown_kind_rejected(mock_hook_cls: MagicMock) -> None:
    mock_hook_cls.return_value = _make_hook([])
    with pytest.raises(ValueError):
        snapshot_build.build_car_driver_training_snapshot(1, 100, kind="bogus")


@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_predict_kind_persists_kind_column(mock_hook_cls: MagicMock) -> None:
    hook = _make_hook([_anchor_tuple(finish_position=None)])
    mock_hook_cls.return_value = hook

    snapshot_build.build_car_driver_training_snapshot(
        1,
        100,
        kind=SNAPSHOT_KIND_PREDICT,
    )

    snapshot_inserts = [
        call
        for call in hook.run.mock_calls
        if "INSERT INTO nascar.ml_training_snapshots" in str(call.args[0])
    ]
    assert snapshot_inserts, "expected ml_training_snapshots insert"
    params = snapshot_inserts[0].kwargs.get("parameters") or snapshot_inserts[0].args[1]
    assert SNAPSHOT_KIND_PREDICT in params


@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_rebuild_same_week_hard_deletes_then_reinserts(mock_hook_cls: MagicMock) -> None:
    """Reruns must DELETE the prior snapshot; soft-delete would violate the unique key."""
    hook = _make_hook([_anchor_tuple(finish_position=12)])
    mock_hook_cls.return_value = hook

    snapshot_build.build_car_driver_training_snapshot(
        1,
        100,
        kind=SNAPSHOT_KIND_TRAIN,
    )
    snapshot_build.build_car_driver_training_snapshot(
        1,
        100,
        kind=SNAPSHOT_KIND_TRAIN,
    )

    run_sqls = [str(call.args[0]) for call in hook.run.mock_calls]
    delete_sqls = [
        s for s in run_sqls if "DELETE FROM nascar.ml_training_snapshots" in s
    ]
    assert len(delete_sqls) == 2
    assert not any("SET deleted_at" in s for s in run_sqls)

    snapshot_inserts = [
        s for s in run_sqls if "INSERT INTO nascar.ml_training_snapshots" in s
    ]
    assert len(snapshot_inserts) == 2
