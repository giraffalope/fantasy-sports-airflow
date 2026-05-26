"""Unit tests for train-task DB persist helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest

for _name in (
    "airflow",
    "airflow.providers",
    "airflow.providers.mysql",
    "airflow.providers.mysql.hooks",
    "airflow.providers.mysql.hooks.mysql",
):
    sys.modules.setdefault(_name, MagicMock())

from include.nascar.ml import train_task  # noqa: E402
from include.nascar.ml.xgb_train_pipeline import TrainedTargetModel  # noqa: E402


def test_insert_feature_set_uses_run_insert_returning_id() -> None:
    hook = MagicMock()
    hook.run_insert_returning_id.return_value = 99

    feature_set_id = train_task._insert_feature_set(
        hook,
        target="r_finish_position",
        snapshot_id=1279,
        selected_features=["cd_pt_finish_avg"],
        boruta_metadata={"boruta": {"n_features": 1}},
    )

    assert feature_set_id == 99
    hook.run_insert_returning_id.assert_called_once()
    hook.get_records.assert_not_called()
    sql = hook.run_insert_returning_id.call_args[0][0]
    assert "INSERT INTO nascar.ml_feature_sets" in sql


def test_insert_model_uses_run_insert_returning_id() -> None:
    hook = MagicMock()
    hook.run_insert_returning_id.return_value = 100

    model_id = train_task._insert_model(
        hook,
        feature_set_id=99,
        hyperparams={"target": "r_finish_position", "n_estimators": 100},
        artifact_uri=None,
        series_id=1,
        track_type_id=2,
        cohort_n_weeks=8,
        train_target_mean=6.125,
    )

    assert model_id == 100
    hook.run_insert_returning_id.assert_called_once()
    hook.get_records.assert_not_called()
    params = (
        hook.run_insert_returning_id.call_args.kwargs.get("parameters")
        or (hook.run_insert_returning_id.call_args[0][1])
    )
    assert params[0] == 99
    # D4 + E1 schema:
    #   params[0] feature_set_id
    #   params[1] algorithm ("xgboost")
    #   params[2] model_kind ("regression" default)
    #   params[3] series_id
    #   params[4] model_scope (SCOPE_TRACK_TYPE)
    #   params[5] track_id (None for cohort scope)
    #   params[6] track_type_id
    #   params[7] hyperparams_json
    #   params[8] artifact_uri
    #   params[9] cohort_n_weeks
    #   params[10] train_target_mean (B3)
    #   params[-1] holdout_residual_quantiles (E1)
    assert params[1] == "xgboost"
    assert params[2] == "regression"
    assert params[3] == 1
    assert params[4] == "track_type"
    assert params[5] is None
    assert params[6] == 2
    assert params[9] == 8
    assert params[10] == 6.125
    # E1: this caller didn't set ``holdout_residual_quantiles`` so the
    # last positional argument must be NULL on the DB side.
    assert params[-1] is None


def test_insert_model_omits_train_target_mean_when_none() -> None:
    """B3: ``train_target_mean=None`` persists NULL, not 0.0."""
    hook = MagicMock()
    hook.run_insert_returning_id.return_value = 101

    train_task._insert_model(
        hook,
        feature_set_id=42,
        hyperparams={"target": "r_total_laps_led"},
        artifact_uri=None,
        series_id=1,
        track_type_id=2,
        cohort_n_weeks=8,
        train_target_mean=None,
    )

    params = (
        hook.run_insert_returning_id.call_args.kwargs.get("parameters")
        or (hook.run_insert_returning_id.call_args[0][1])
    )
    # E1: train_target_mean now sits at params[10]; the last position is
    # ``holdout_residual_quantiles``. Both default to NULL when not set.
    assert params[10] is None
    assert params[-1] is None


def _trained_target(target: str) -> TrainedTargetModel:
    """Build a minimal ``TrainedTargetModel`` for atomic-persistence tests."""
    return TrainedTargetModel(
        target=target,
        selected_features=["f1"],
        hyperparams={"max_depth": 4},
        boruta_metadata={"n_features": 1},
        tuning_metadata={"tuning_skipped": True},
        holdout_mae=0.5,
        holdout_predictions=np.array([0.4, 0.6]),
        artifact_path=None,
        model=MagicMock(),
    )


def test_train_cohort_a1_atomic_per_target_persist(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A1: target N's exception must not soft-delete target N+1's prior model.

    Pre-A1 the loop soft-deleted *all* targets up-front; if any later persist
    raised, the cohort lost all active models. After A1, soft-delete and
    persist are interleaved per target, so only the in-progress target sees
    partial state.
    """
    targets = ("t_a", "t_b", "t_c")
    trained = [_trained_target(t) for t in targets]

    monkeypatch.setattr(
        train_task,
        "train_models_for_targets",
        lambda *args, **kwargs: trained,
    )
    monkeypatch.setattr(
        train_task,
        "predicted_dfs_mae_on_holdout",
        lambda *args, **kwargs: (None, 0),
    )
    monkeypatch.setattr(
        train_task,
        "holdout_dfs_eval_metrics",
        lambda *args, **kwargs: {"n": 0},
    )
    monkeypatch.setattr(
        train_task,
        "_holdout_track_eval",
        lambda *args, **kwargs: None,
    )

    call_order: list[tuple[str, str]] = []

    def _soft_delete(hook, *, series_id, track_type_id, target):  # noqa: ARG001
        call_order.append(("soft_delete", target))

    monkeypatch.setattr(
        train_task, "_soft_delete_prior_cohort_models", _soft_delete
    )

    next_model_id = iter([101, 102, 103])

    def _persist(hook, *, entry, **kwargs):  # noqa: ARG001
        call_order.append(("persist", entry.target))
        if entry.target == "t_b":
            raise RuntimeError("boom mid-target")
        return next(next_model_id)

    monkeypatch.setattr(train_task, "_persist_trained_model", _persist)

    rows = [
        {
            "week_id": 100 + i,
            "track_id": 1,
            "track_type_id": 2,
            "ml_training_snapshot_id": 9000 + i,
            "features": {"f1": float(i)},
            "targets": {t: 0.0 for t in targets},
        }
        for i in range(8)
    ]

    with pytest.raises(RuntimeError, match="boom mid-target"):
        train_task._train_cohort(
            series_id=1,
            track_type_id=2,
            track_id_anchor=5,
            rows=rows,
            feature_keys=["f1"],
            cohort_n_weeks=8,
            holdout_weeks=1,
            dev_weeks=0,
            artifact_root=tmp_path,
            hook=MagicMock(),
            scoring_rules={"components": []},
            targets=targets,
        )

    assert call_order == [
        ("soft_delete", "t_a"),
        ("persist", "t_a"),
        ("soft_delete", "t_b"),
        ("persist", "t_b"),
    ], (
        "A1 contract violated: target t_c must not be soft-deleted when "
        "target t_b raises."
    )
    # Targets touched: only t_a and t_b. t_c's prior model stays active.
    touched_targets = {t for _, t in call_order}
    assert touched_targets == {"t_a", "t_b"}
    assert "t_c" not in touched_targets


def test_resolve_train_targets_uses_rules_components() -> None:
    """A5: dynamic targets come from ``target_keys_for_rules(scoring_rules)``."""
    scoring_rules = {
        "components": [
            {"source": "r_finish_position", "value_per_unit": -1.0},
            {"source": "r_total_laps_led", "value_per_unit": 0.25},
            # Derived place_diff component should NOT yield a target.
            {
                "source": "r_place_diff",
                "value_per_unit": 0.5,
                "fallback": {
                    "op": "subtract",
                    "from": ["r_start_position", "r_finish_position"],
                },
            },
        ]
    }
    targets = train_task._resolve_train_targets(scoring_rules)
    assert "r_finish_position" in targets
    assert "r_total_laps_led" in targets
    assert "r_place_diff" not in targets, (
        "derived place_diff must not become a trainable target"
    )


def test_resolve_train_targets_falls_back_when_rules_empty() -> None:
    """A5: when rules yield no targets, fall back to ``DEFAULT_TARGETS``.

    D3: the binary DNF target is always appended so the two-stage classifier
    head gets fit alongside the regression heads.
    """
    from include.nascar.ml.dnf import DNF_TARGET_KEY

    targets = train_task._resolve_train_targets({"components": []})
    assert "r_finish_position" in targets
    assert targets == (*train_task.DEFAULT_TARGETS, DNF_TARGET_KEY)


def test_resolve_train_targets_recovers_when_primary_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A5: if PRIMARY_TARGET drops out of rules, fall back to ``DEFAULT_TARGETS``.

    D3: ``r_is_dnf`` is appended on top of the recovered defaults.
    """
    from include.nascar.ml.dnf import DNF_TARGET_KEY

    rules = {
        "components": [
            {"source": "r_total_laps_led", "value_per_unit": 0.25},
        ]
    }
    with caplog.at_level("WARNING"):
        targets = train_task._resolve_train_targets(rules)

    assert targets == (*train_task.DEFAULT_TARGETS, DNF_TARGET_KEY)
    assert any("PRIMARY_TARGET" in r.message for r in caplog.records)
