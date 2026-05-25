"""Unit tests for scoped ``load_training_frame`` SQL and train-task loading."""

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
        1,
        1,
        100,
        5,
        3,
        10,
        20,
        12,
        '{"r_start_position": 12}',
        '{"r_finish_position": 5.0}',
        42.5,
        7,
    )


class TestLoadTrainingFrameFiltered:
    def test_filtered_load_uses_metadata_filters_not_in_list(self) -> None:
        hook = MagicMock()
        hook.get_records.return_value = [_sample_db_row()]

        with patch(
            "include.nascar.ml.training_targets.NascarMySqlHook",
            return_value=hook,
        ):
            frame = load_training_frame(
                dfs_scoring_rules_id=7,
                series_id=1,
                track_id=42,
                mysql_conn_id="nascar_conn",
            )

        assert len(frame) == 1
        assert frame[0]["track_id"] == 5
        assert "dfs_points_breakdown" not in frame[0]
        assert "target_keys" not in frame[0]

        call = hook.get_records.call_args
        sql = call.args[0]
        params = call.kwargs.get("parameters", call.args[1] if len(call.args) > 1 else ())
        assert " IN (" not in sql
        assert "s.kind = %s" in sql
        assert "s.dfs_scoring_rules_id = %s" in sql
        assert "s.series_id = %s" in sql
        assert "s.track_id = %s" in sql
        assert params == (SNAPSHOT_KIND_TRAIN, 7, 1, 42)

    def test_filtered_load_track_type_and_max_weeks(self) -> None:
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
                max_weeks=52,
            )

        call = hook.get_records.call_args
        sql = call.args[0]
        params = call.kwargs.get("parameters", call.args[1] if len(call.args) > 1 else ())
        assert "tt.track_type_id = %s" in sql
        assert "recent_weeks" in sql
        assert "LIMIT %s" in sql
        assert params[-1] == 52

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


class TestTrainModelsIfNeededScopedLoads:
    def test_scoped_loads_not_monolithic_snapshot_list(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from include.nascar.cup.current_week import SeriesCurrentWeekTrackContext

        train_task = _import_train_task()
        hook = MagicMock()
        hook.get_records.side_effect = [
            [(7, '{"version": 1, "components": []}')],
            [(1,)],
            [('["r_start_position", "cd_pt_finish_avg"]',)],
        ]

        load_calls: list[dict[str, Any]] = []
        ctx = SeriesCurrentWeekTrackContext(series_id=1, track_id=10, track_type_id=2)

        def _fake_load(**kwargs: Any) -> list[dict]:
            load_calls.append(dict(kwargs))
            return [{"ml_training_snapshot_id": 1, "week_id": 100}]

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
        monkeypatch.setattr(
            train_task,
            "_predict_routing_chain",
            lambda *a, **k: [train_task.ScopeSpec(train_task.SCOPE_TRACK, 10, None)],
        )
        monkeypatch.setattr(train_task, "_predict_ready", lambda *a, **k: False)
        monkeypatch.setattr(
            train_task,
            "_scopes_needing_refresh",
            lambda *a, **k: [train_task.ScopeSpec(train_task.SCOPE_TRACK, 10, None)],
        )
        monkeypatch.setattr(train_task, "load_training_frame", _fake_load)
        monkeypatch.setattr(
            train_task,
            "_train_scope",
            lambda **kw: {"trained_models": {}, "scope": kw.get("scope")},
        )
        monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "0")

        result = train_task.train_models_if_needed_task()

        assert result.get("skipped") is None
        assert len(load_calls) == 1
        assert load_calls[0]["dfs_scoring_rules_id"] == 7
        assert load_calls[0]["series_id"] == 1
        assert load_calls[0]["track_id"] == 10
        assert "ml_training_snapshot_ids" not in load_calls[0]
