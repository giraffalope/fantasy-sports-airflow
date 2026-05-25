"""Unit tests for incremental predict-minimal train refresh."""

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

from include.nascar.cup.current_week import (  # noqa: E402
    _CURRENT_WEEK_TRACK_CONTEXT_SQL,
    SeriesCurrentWeekTrackContext,
    resolve_current_week_track_context,
)
from include.nascar.ml.model_scope import SCOPE_GLOBAL, SCOPE_TRACK, SCOPE_TRACK_TYPE  # noqa: E402
from include.nascar.ml.train_task import (  # noqa: E402
    ScopeSpec,
    _predict_ready,
    _predict_routing_chain,
    _scopes_needing_refresh,
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


def test_predict_routing_chain_cold_track_omits_track_scope() -> None:
    hook = MagicMock()
    hook.get_records.return_value = []
    ctx = SeriesCurrentWeekTrackContext(series_id=1, track_id=10, track_type_id=3)
    with (
        patch(
            "include.nascar.ml.train_task._track_meets_threshold",
            return_value=False,
        ),
        patch(
            "include.nascar.ml.train_task._track_type_meets_threshold",
            return_value=True,
        ),
    ):
        chain = _predict_routing_chain(
            hook, 7, 1, ctx, track_threshold=3, type_threshold=3, enable_global=True
        )
    assert chain == [
        ScopeSpec(SCOPE_TRACK_TYPE, None, 3),
        ScopeSpec(SCOPE_GLOBAL, None, None),
    ]


def test_predict_ready_when_first_scope_current() -> None:
    hook = MagicMock()
    chain = [ScopeSpec(SCOPE_TRACK, 10, None)]
    with (
        patch(
            "include.nascar.ml.train_task._scope_model_exists",
            return_value=True,
        ),
        patch(
            "include.nascar.ml.train_task._scope_covered_week",
            return_value=200,
        ),
    ):
        assert _predict_ready(hook, 7, 1, chain, 200) is True


def test_scopes_needing_refresh_returns_first_stale_only() -> None:
    hook = MagicMock()
    chain = [
        ScopeSpec(SCOPE_TRACK, 10, None),
        ScopeSpec(SCOPE_TRACK_TYPE, None, 3),
    ]
    with (
        patch(
            "include.nascar.ml.train_task._scope_model_exists",
            return_value=True,
        ),
        patch(
            "include.nascar.ml.train_task._scope_covered_week",
            side_effect=[100, 50],
        ),
    ):
        needing = _scopes_needing_refresh(hook, 7, 1, chain, 200)
    assert needing == [ScopeSpec(SCOPE_TRACK, 10, None)]


class TestTrainModelsIncremental:
    def _ctx(self) -> SeriesCurrentWeekTrackContext:
        return SeriesCurrentWeekTrackContext(series_id=1, track_id=10, track_type_id=2)

    def _setup_hook(self) -> MagicMock:
        hook = MagicMock()
        hook.get_records.side_effect = [
            [(7, '{"version": 1, "components": []}')],
            [(1,)],
            [('["r_start_position", "cd_pt_finish_avg"]',)],
        ]
        return hook

    def test_predict_ready_skips_without_train_scope(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from include.nascar.ml import train_task

        hook = self._setup_hook()
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
            lambda h, *, series_id: self._ctx(),
        )
        monkeypatch.setattr(
            train_task,
            "latest_train_week_for_retrain",
            lambda h, **k: 200,
        )
        monkeypatch.setattr(
            train_task,
            "_predict_routing_chain",
            lambda *a, **k: [ScopeSpec(SCOPE_TRACK, 10, None)],
        )
        monkeypatch.setattr(train_task, "_predict_ready", lambda *a, **k: True)
        train_calls: list[Any] = []
        monkeypatch.setattr(
            train_task,
            "_train_scope",
            lambda **kw: train_calls.append(kw) or {"trained_models": {}},
        )

        result = train_models_if_needed_task()

        assert result.get("skipped") == "predict_ready"
        assert train_calls == []

    def test_stale_track_trains_one_scope_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from include.nascar.ml import train_task

        hook = self._setup_hook()
        load_calls: list[dict[str, Any]] = []
        train_calls: list[Any] = []

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
            lambda h, *, series_id: self._ctx(),
        )
        monkeypatch.setattr(
            train_task,
            "latest_train_week_for_retrain",
            lambda h, **k: 200,
        )
        monkeypatch.setattr(
            train_task,
            "_predict_routing_chain",
            lambda *a, **k: [
                ScopeSpec(SCOPE_TRACK, 10, None),
                ScopeSpec(SCOPE_TRACK_TYPE, None, 2),
                ScopeSpec(SCOPE_GLOBAL, None, None),
            ],
        )
        monkeypatch.setattr(train_task, "_predict_ready", lambda *a, **k: False)
        monkeypatch.setattr(
            train_task,
            "_scopes_needing_refresh",
            lambda *a, **k: [ScopeSpec(SCOPE_TRACK, 10, None)],
        )

        def _fake_load(**kw: Any) -> list[dict[str, Any]]:
            load_calls.append(dict(kw))
            return [{"ml_training_snapshot_id": 1, "week_id": 100}]

        monkeypatch.setattr(train_task, "load_training_frame", _fake_load)
        monkeypatch.setattr(
            train_task,
            "_train_scope",
            lambda **kw: train_calls.append(kw) or {"trained_models": {}},
        )
        monkeypatch.setenv("FS_ML_ENABLE_GLOBAL_FALLBACK", "0")

        result = train_models_if_needed_task()

        assert result.get("skipped") is None
        assert len(load_calls) == 1
        assert load_calls[0]["track_id"] == 10
        assert len(train_calls) == 1
        assert train_calls[0]["scope"] == SCOPE_TRACK
