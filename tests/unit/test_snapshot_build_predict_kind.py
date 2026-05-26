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

import include.nascar.ml.snapshot_build as snapshot_build  # noqa: E402
from include.nascar.ml.snapshot_config import (  # noqa: E402
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


def _anchor_tuple(
    *,
    finish_position: int | None,
    start_position: int = 5,
    car_status: str | None = "RUNNING",
) -> tuple:
    payload = [
        1,  # id
        1,  # series_id
        2025,  # season_id
        100,  # week_id
        7,  # track_id
        9,  # race_id
        101,  # car_driver_id
        201,  # driver_id
        301,  # car_id
        401,  # team_id
        501,  # manufacturer_id
        601,  # car_generation_id
        701,  # car_generation_model_id
        start_position,  # r_start_position
        finish_position,  # r_finish_position
        None,  # r_place_diff
    ]
    # 21 numeric anchor metrics between ``r_place_diff`` and ``r_car_status``
    # (laps led / running rates / pitstops / etc.). D3 added ``r_car_status``
    # as the 38th column so the DNF label derivation has a status to read.
    payload.extend([None] * 21)
    payload.append(car_status)  # r_car_status (D3 label source)
    assert len(payload) == 38
    return tuple(payload)


@pytest.fixture(autouse=True)
def _clear_metric_columns_cache():
    from include.nascar.ml import season_features

    snapshot_build._METRIC_COLUMNS_CACHE.clear()
    season_features.clear_tracks_columns_cache()
    yield
    snapshot_build._METRIC_COLUMNS_CACHE.clear()
    season_features.clear_tracks_columns_cache()


def _make_hook(anchor_rows: list[tuple]) -> MagicMock:
    hook = MagicMock()

    def get_records(sql: str, parameters: Any = ()) -> list[tuple]:
        text = sql.lower()
        if "from nascar.dfs_scoring_rules" in text:
            return [(1, json.dumps(_DK_RULES))]
        if "information_schema" in text:
            return []
        # C1 driver-form prior-race query: same `FROM nascar.agg_week_results
        # awr` prefix as the anchor query, distinguished by the join to weeks.
        if "join nascar.weeks" in text and "from nascar.agg_week_results awr" in text:
            return []
        if "from nascar.agg_week_results awr" in text:
            return anchor_rows
        # C4 season context + track attribute lookups
        if "from nascar.weeks" in text or "from nascar.seasons" in text:
            return []
        if "from nascar.tracks" in text:
            return []
        return []

    hook.get_records.side_effect = get_records
    hook.run_insert_returning_id.return_value = 42

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
    hook.run_insert_returning_id.assert_called_once()
    insert_sql = str(hook.run_insert_returning_id.call_args.args[0])
    assert "INSERT INTO nascar.ml_training_snapshots" in insert_sql

    run_sqls = [str(call.args[0]) for call in hook.run.mock_calls]
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
def test_train_kind_requires_both_endpoints_in_anchor_filter(
    mock_hook_cls: MagicMock,
) -> None:
    """B4 invariant: labeled train anchors must have both start & finish populated.

    Pre-fix, the train anchor filter only required ``r_finish_position IS
    NOT NULL``. Historical ``agg_week_results`` rows with a finish but no
    start (a lineup-import data-quality state) would then trip
    :class:`MissingStartPositionError` inside
    :func:`compute_dfs_points_from_targets` once Phase B4 hardened that
    function. This regression test pins both filters at the SQL level so
    the assertion only fires on genuine plumbing bugs, never on
    legitimate data gaps.
    """
    hook = _make_hook([_anchor_tuple(finish_position=12)])
    mock_hook_cls.return_value = hook

    snapshot_build.build_car_driver_training_snapshot(
        1,
        100,
        kind=SNAPSHOT_KIND_TRAIN,
    )

    anchor_sqls = [
        str(call.args[0])
        for call in hook.get_records.mock_calls
        if "FROM nascar.agg_week_results" in str(call.args[0])
        and "JOIN nascar.weeks" not in str(call.args[0])
    ]
    assert anchor_sqls, "expected at least one anchor query"
    anchor_sql = anchor_sqls[0]
    assert "r_start_position IS NOT NULL" in anchor_sql
    assert "r_finish_position IS NOT NULL" in anchor_sql
    assert "r_finish_position IS NULL" not in anchor_sql


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

    hook.run_insert_returning_id.assert_called_once()
    params = (
        hook.run_insert_returning_id.call_args.kwargs.get("parameters")
        or (hook.run_insert_returning_id.call_args.args[1])
    )
    assert SNAPSHOT_KIND_PREDICT in params


@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_new_feature_families_present_in_feature_keys(mock_hook_cls: MagicMock) -> None:
    """C5: ``df_*``, ``qf_*``, ``season_*``, ``track_attr_*`` flow through feature_keys."""
    hook = _make_hook([_anchor_tuple(finish_position=12)])
    mock_hook_cls.return_value = hook

    snapshot_build.build_car_driver_training_snapshot(
        1,
        100,
        kind=SNAPSHOT_KIND_TRAIN,
    )

    hook.run_insert_returning_id.assert_called_once()
    params = (
        hook.run_insert_returning_id.call_args.kwargs.get("parameters")
        or hook.run_insert_returning_id.call_args.args[1]
    )
    feature_keys_json = params[6]
    assert isinstance(feature_keys_json, str)
    feature_keys = set(json.loads(feature_keys_json))

    # C1 driver-form family
    assert "df_last5_avg_finish" in feature_keys
    assert "df_ytd_avg_finish" in feature_keys
    assert "df_streak_top10" in feature_keys
    # C2 qualifying / field-strength
    assert "qf_pole_gap_seconds" in feature_keys
    assert "qf_field_strength_ytd_avg_finish" in feature_keys
    # C4 season + track attributes
    assert "season_week_number" in feature_keys
    assert "season_is_playoffs" in feature_keys
    assert "track_attr_length_miles" in feature_keys


@patch("include.nascar.ml.snapshot_build.NascarMySqlHook")
def test_new_feature_families_present_in_features_json(mock_hook_cls: MagicMock) -> None:
    """C5: every row's ``features_json`` carries the new keys (None when no data)."""
    hook = _make_hook([_anchor_tuple(finish_position=12)])
    mock_hook_cls.return_value = hook

    snapshot_build.build_car_driver_training_snapshot(
        1,
        100,
        kind=SNAPSHOT_KIND_TRAIN,
    )

    row_inserts: list[tuple[Any, ...]] = []
    for call in hook.run.mock_calls:
        sql = str(call.args[0])
        if "INSERT INTO nascar.ml_training_rows" not in sql:
            continue
        params = call.kwargs.get("parameters") or (
            call.args[1] if len(call.args) > 1 else ()
        )
        row_inserts.append(params)
    assert row_inserts, "expected at least one ml_training_rows insert"
    features_json = json.loads(row_inserts[0][-1])
    for key in (
        "df_last5_avg_finish",
        "qf_pole_gap_seconds",
        "season_week_number",
        "track_attr_length_miles",
    ):
        assert key in features_json


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
    delete_sqls = [s for s in run_sqls if "DELETE FROM nascar.ml_training_snapshots" in s]
    assert len(delete_sqls) == 2
    assert not any("SET deleted_at" in s for s in run_sqls)

    assert hook.run_insert_returning_id.call_count == 2
