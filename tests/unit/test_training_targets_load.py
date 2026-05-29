"""Unit tests for cohort ``load_training_frame`` SQL and train-task loading."""

from __future__ import annotations

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

from include.nascar.ml.snapshot_config import SNAPSHOT_KIND_TRAIN  # noqa: E402
from include.nascar.ml.training_targets import load_training_frame  # noqa: E402


def _import_train_task():
    from include.nascar.ml import train_task

    return train_task


def _sample_db_row() -> tuple:
    return (
        1,  # ml_training_snapshot_id
        1,  # series_id
        100,  # week_id
        5,  # track_id
        3,  # track_type_id
        10,  # car_driver_id
        20,  # driver_id
        12,  # r_start_position
        '{"r_start_position": 12}',  # features_json
        '{"r_finish_position": 5.0}',  # targets_json
        42.5,  # dfs_points
    )


class TestLoadTrainingFrameFiltered:
    def test_filtered_load_without_track_type_returns_full_series_frame(self) -> None:
        hook = MagicMock()
        hook.get_records.return_value = [_sample_db_row()]

        with patch(
            "include.nascar.ml.training_targets.NascarMySqlHook",
            return_value=hook,
        ):
            frame = load_training_frame(
                dfs_scoring_rules_id=7,
                series_id=1,
                mysql_conn_id="nascar_conn",
            )

        assert len(frame) == 1
        assert frame[0]["track_id"] == 5
        assert frame[0]["track_type_id"] == 3
        assert "dfs_points_breakdown" not in frame[0]

        call = hook.get_records.call_args
        sql = call.args[0]
        params = call.kwargs.get("parameters", call.args[1] if len(call.args) > 1 else ())
        assert " IN (" not in sql
        assert "nascar.tracks_track_types" in sql
        assert "tt.track_type_id = %s" not in sql
        assert "s.kind = %s" in sql
        assert "s.dfs_scoring_rules_id = %s" in sql
        assert "s.series_id = %s" in sql
        assert params == (SNAPSHOT_KIND_TRAIN, 7, 1)
        # Same invariant as the cohort branch — placeholder count must match
        # the params tuple length so positional binding works.
        assert sql.count("%s") == len(params)

    def test_filtered_load_track_type_pulls_sibling_union(self) -> None:
        hook = MagicMock()
        hook.get_records.return_value = []

        with patch(
            "include.nascar.ml.training_targets.NascarMySqlHook",
            return_value=hook,
        ):
            load_training_frame(
                dfs_scoring_rules_id=3,
                series_id=2,
                track_type_id=9,
            )

        call = hook.get_records.call_args
        sql = call.args[0]
        params = call.kwargs.get("parameters", call.args[1] if len(call.args) > 1 else ())
        # Inner join on the bridge restricted to the cohort id; no per-snapshot ``s.track_id`` filter.
        assert "JOIN nascar.tracks_track_types tt" in sql
        assert "tt.track_type_id = %s" in sql
        assert "s.track_id = %s" not in sql
        assert "recent_weeks" not in sql  # max_weeks knob is retired
        # The JOIN placeholder is the FIRST ``%s`` in the rendered SQL, so the
        # cohort id must come first in the params tuple — drivers bind by
        # position, not by name.
        assert params == (9, SNAPSHOT_KIND_TRAIN, 3, 2)

    def test_filtered_load_placeholder_count_matches_params(self) -> None:
        """Regression guard: a prior bug appended track_type_id LAST while the
        JOIN placeholder rendered FIRST, so MariaDB matched
        ``tt.track_type_id = 'train'`` and every cohort load returned 0 rows.
        Lock in: placeholder count == params length, and the cohort id is the
        first param when ``track_type_id`` is supplied (since the JOIN ``%s``
        comes before the WHERE ``%s``s).
        """
        hook = MagicMock()
        hook.get_records.return_value = []

        with patch(
            "include.nascar.ml.training_targets.NascarMySqlHook",
            return_value=hook,
        ):
            load_training_frame(
                dfs_scoring_rules_id=3,
                series_id=2,
                track_type_id=9,
            )

        call = hook.get_records.call_args
        sql = call.args[0]
        params = call.kwargs.get("parameters", call.args[1] if len(call.args) > 1 else ())
        assert sql.count("%s") == len(params)
        join_idx = sql.index("tt.track_type_id = %s")
        where_idx = sql.index("WHERE")
        assert join_idx < where_idx
        assert params[0] == 9

    def test_filtered_load_rejects_legacy_track_id_kwarg(self) -> None:
        with pytest.raises(TypeError):
            load_training_frame(
                dfs_scoring_rules_id=7,
                series_id=1,
                track_id=42,  # type: ignore[call-arg]
            )

    def test_snapshot_id_list_still_uses_in_clause(self) -> None:
        hook = MagicMock()
        hook.get_records.return_value = []

        with patch(
            "include.nascar.ml.training_targets.NascarMySqlHook",
            return_value=hook,
        ):
            load_training_frame([1, 2, 3])

        call = hook.get_records.call_args
        sql = call.args[0]
        params = call.kwargs.get("parameters", call.args[1] if len(call.args) > 1 else ())
        assert " IN (%s, %s, %s)" in sql
        assert params == (1, 2, 3)
        assert "dfs_points_breakdown" not in sql


class TestTrainModelsIfNeededCohortLoads:
    def test_cohort_load_is_invoked_with_track_type_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from include.nascar.cup.current_week import SeriesCurrentWeekTrackContext

        train_task = _import_train_task()
        hook = MagicMock()
        # Order of get_records calls in train_models_if_needed_task:
        # 1. _active_rules
        # 2. _distinct_series_ids
        # 3. _feature_keys_from_snapshot_metadata
        # 4. track_type_ids_for_track (inside _enumerate_cohorts)
        # 5. cohort_distinct_weeks
        # 6. _cohort_covered_week
        # 7. cohort_latest_train_week
        # 8. sibling_track_ids_for_type
        hook.get_records.side_effect = [
            [(7, '{"version": 1, "components": []}')],  # _active_rules
            [(1,)],  # _distinct_series_ids
            [('["r_start_position", "cd_pt_finish_avg"]',)],  # feature_keys
            [(3,)],  # track_type_ids_for_track for current track
            [(5,)],  # cohort_distinct_weeks
            [(None,)],  # _cohort_covered_week (not yet covered)
            [(200,)],  # cohort_latest_train_week
            [(10,), (20,)],  # sibling_track_ids_for_type
        ]

        load_calls: list[dict[str, Any]] = []
        ctx = SeriesCurrentWeekTrackContext(series_id=1, track_id=10, track_type_id=3)

        def _fake_load(**kwargs: Any) -> list[dict]:
            load_calls.append(dict(kwargs))
            return [
                {"ml_training_snapshot_id": 1, "week_id": 100, "track_id": 10},
                {"ml_training_snapshot_id": 2, "week_id": 101, "track_id": 20},
            ]

        monkeypatch.setattr(train_task, "NascarMySqlHook", lambda **_: hook)
        monkeypatch.setattr(
            train_task,
            "resolve_series_current_week",
            lambda h, *, series_id: MagicMock(),
        )
        monkeypatch.setattr(train_task, "_latest_train_week_for_retrain_gate", lambda h, r: 200)
        monkeypatch.setattr(
            train_task,
            "resolve_current_week_track_context",
            lambda h, *, series_id: ctx,
        )
        monkeypatch.setattr(train_task, "latest_train_week_for_retrain", lambda h, **k: 200)
        monkeypatch.setattr(train_task, "load_training_frame", _fake_load)
        monkeypatch.setattr(
            train_task,
            "_train_cohort",
            lambda **kw: {
                "trained_models": {},
                "track_type_id": kw["track_type_id"],
                "cohort_n_weeks": kw["cohort_n_weeks"],
            },
        )
        monkeypatch.delenv("FS_ML_MIN_COHORT_WEEKS", raising=False)
        # Force the in-process branch so the load_training_frame /
        # _train_cohort monkeypatches above are actually exercised.
        # With FS_ML_TRAIN_COHORT_ISOLATION enabled (the default in
        # production), the task forks a child via billiard and the
        # child re-imports train_task from scratch — bypassing the
        # parent-side monkeypatches and breaking this assertion.
        monkeypatch.setenv("FS_ML_TRAIN_COHORT_ISOLATION", "0")

        result = train_task.train_models_if_needed_task()

        assert result.get("skipped") is None
        assert len(load_calls) == 1
        assert load_calls[0]["dfs_scoring_rules_id"] == 7
        assert load_calls[0]["series_id"] == 1
        assert load_calls[0]["track_type_id"] == 3
        assert "track_id" not in load_calls[0]
        assert "ml_training_snapshot_ids" not in load_calls[0]
