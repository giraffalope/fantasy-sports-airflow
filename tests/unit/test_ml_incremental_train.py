"""Unit tests for the cohort train-task gating helpers."""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

for _name in (
    "airflow",
    "airflow.providers",
    "airflow.providers.mysql",
    "airflow.providers.mysql.hooks",
    "airflow.providers.mysql.hooks.mysql",
):
    sys.modules.setdefault(_name, MagicMock())

from include.nascar.cup.current_week import (  # noqa: E402
    _CURRENT_WEEK_TRACK_CONTEXT_SQL,
    SeriesCurrentWeekTrackContext,
    resolve_current_week_track_context,
)
from include.nascar.ml.train_task import (  # noqa: E402
    train_models_if_needed_task,
)


def test_current_week_track_context_sql_shape() -> None:
    assert "v_series_current_week" in _CURRENT_WEEK_TRACK_CONTEXT_SQL
    assert "MIN(tt.track_type_id)" in _CURRENT_WEEK_TRACK_CONTEXT_SQL


def test_resolve_current_week_track_context_returns_dataclass() -> None:
    hook = MagicMock()
    hook.get_records.return_value = [(5, 2)]
    ctx = resolve_current_week_track_context(hook, series_id=1)
    assert ctx is not None
    assert ctx.series_id == 1
    assert ctx.track_id == 5
    assert ctx.track_type_id == 2


class TestTrainModelsIfNeededCohort:
    def _ctx(self) -> SeriesCurrentWeekTrackContext:
        return SeriesCurrentWeekTrackContext(series_id=1, track_id=10, track_type_id=2)

    def _setup_hook(self) -> MagicMock:
        hook = MagicMock()
        hook.get_records.side_effect = [
            [(7, '{"version": 1, "components": []}')],  # _active_rules
            [(1,)],  # _distinct_series_ids
            [('["r_start_position", "cd_pt_finish_avg"]',)],  # feature_keys
            [(2,), (3,)],  # track_type_ids_for_track
        ]
        return hook

    def test_cold_cohort_below_min_weeks_is_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from include.nascar.ml import train_task

        hook = self._setup_hook()
        monkeypatch.setattr(train_task, "NascarMySqlHook", lambda **_: hook)
        monkeypatch.setattr(
            train_task, "resolve_series_current_week", lambda h, *, series_id: MagicMock()
        )
        monkeypatch.setattr(train_task, "_latest_train_week_for_retrain_gate", lambda h, r: 200)
        monkeypatch.setattr(
            train_task,
            "resolve_current_week_track_context",
            lambda h, *, series_id: self._ctx(),
        )
        monkeypatch.setattr(train_task, "latest_train_week_for_retrain", lambda h, **k: 200)
        monkeypatch.setattr(train_task, "cohort_distinct_weeks", lambda *a, **k: 1)

        train_calls: list[Any] = []
        monkeypatch.setattr(
            train_task,
            "_train_cohort",
            lambda **kw: train_calls.append(kw) or {"trained_models": {}},
        )
        monkeypatch.setenv("FS_ML_MIN_COHORT_WEEKS", "4")

        result = train_models_if_needed_task()

        assert result["skipped"] == "insufficient_cohort_weeks_all_cohorts"
        assert train_calls == []
        cold_types = {entry["track_type_id"] for entry in result["cold_cohorts"]}
        assert cold_types == {2, 3}

    def test_fresh_cohorts_skip_training_with_predict_ready(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from include.nascar.ml import train_task

        hook = self._setup_hook()
        monkeypatch.setattr(train_task, "NascarMySqlHook", lambda **_: hook)
        monkeypatch.setattr(
            train_task, "resolve_series_current_week", lambda h, *, series_id: MagicMock()
        )
        monkeypatch.setattr(train_task, "_latest_train_week_for_retrain_gate", lambda h, r: 200)
        monkeypatch.setattr(
            train_task,
            "resolve_current_week_track_context",
            lambda h, *, series_id: self._ctx(),
        )
        monkeypatch.setattr(train_task, "latest_train_week_for_retrain", lambda h, **k: 200)
        monkeypatch.setattr(train_task, "cohort_distinct_weeks", lambda *a, **k: 8)
        monkeypatch.setattr(train_task, "_cohort_covered_week", lambda *a, **k: 200)
        monkeypatch.setattr(train_task, "cohort_latest_train_week", lambda *a, **k: 200)

        train_calls: list[Any] = []
        monkeypatch.setattr(
            train_task,
            "_train_cohort",
            lambda **kw: train_calls.append(kw) or {"trained_models": {}},
        )

        result = train_models_if_needed_task()

        assert result["skipped"] == "predict_ready"
        assert train_calls == []

    def test_cohort_fresh_when_covered_matches_cohort_latest_not_series(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: series-wide latest week must not force retrain for other cohorts."""
        from include.nascar.ml import train_task

        hook = self._setup_hook()
        monkeypatch.setattr(train_task, "NascarMySqlHook", lambda **_: hook)
        monkeypatch.setattr(
            train_task, "resolve_series_current_week", lambda h, *, series_id: MagicMock()
        )
        monkeypatch.setattr(train_task, "_latest_train_week_for_retrain_gate", lambda h, r: 200)
        monkeypatch.setattr(
            train_task,
            "resolve_current_week_track_context",
            lambda h, *, series_id: self._ctx(),
        )
        monkeypatch.setattr(train_task, "latest_train_week_for_retrain", lambda h, **k: 200)
        monkeypatch.setattr(train_task, "cohort_distinct_weeks", lambda *a, **k: 8)
        monkeypatch.setattr(train_task, "_cohort_covered_week", lambda *a, **k: 150)
        monkeypatch.setattr(train_task, "cohort_latest_train_week", lambda *a, **k: 150)

        train_calls: list[Any] = []
        load_calls: list[Any] = []
        monkeypatch.setattr(
            train_task,
            "_train_cohort",
            lambda **kw: train_calls.append(kw) or {"trained_models": {}},
        )
        monkeypatch.setattr(
            train_task,
            "load_training_frame",
            lambda **kw: load_calls.append(kw) or [],
        )

        result = train_models_if_needed_task()

        assert result["skipped"] == "predict_ready"
        assert train_calls == []
        assert load_calls == []

    def test_stale_cohort_triggers_training(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from include.nascar.ml import train_task

        hook = self._setup_hook()
        load_calls: list[dict[str, Any]] = []
        train_calls: list[Any] = []

        monkeypatch.setattr(train_task, "NascarMySqlHook", lambda **_: hook)
        monkeypatch.setattr(
            train_task, "resolve_series_current_week", lambda h, *, series_id: MagicMock()
        )
        monkeypatch.setattr(train_task, "_latest_train_week_for_retrain_gate", lambda h, r: 200)
        monkeypatch.setattr(
            train_task,
            "resolve_current_week_track_context",
            lambda h, *, series_id: self._ctx(),
        )
        monkeypatch.setattr(train_task, "latest_train_week_for_retrain", lambda h, **k: 200)
        monkeypatch.setattr(train_task, "cohort_distinct_weeks", lambda *a, **k: 8)
        monkeypatch.setattr(train_task, "_cohort_covered_week", lambda *a, **k: 50)
        monkeypatch.setattr(train_task, "cohort_latest_train_week", lambda *a, **k: 200)
        monkeypatch.setattr(train_task, "sibling_track_ids_for_type", lambda *a, **k: [10])
        monkeypatch.setenv("FS_ML_TRAIN_COHORT_ISOLATION", "0")

        def _fake_load(**kw: Any) -> list[dict[str, Any]]:
            load_calls.append(dict(kw))
            return [{"ml_training_snapshot_id": 1, "week_id": 100, "track_id": 10}]

        monkeypatch.setattr(train_task, "load_training_frame", _fake_load)
        monkeypatch.setattr(
            train_task,
            "_train_cohort",
            lambda **kw: train_calls.append(kw) or {
                "trained_models": {"r_finish_position": 99},
                "track_type_id": kw["track_type_id"],
            },
        )

        result = train_models_if_needed_task()

        assert result.get("skipped") is None
        assert len(load_calls) == 2  # one per track_type_id (2, 3)
        assert {c["track_type_id"] for c in load_calls} == {2, 3}
        assert all("track_id" not in c for c in load_calls)
        assert len(train_calls) == 2
