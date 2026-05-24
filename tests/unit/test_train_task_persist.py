"""Unit tests for train-task DB persist helpers."""

from __future__ import annotations

import json
import sys
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

from include.nascar.ml import train_task


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
        scope="track",
        track_id=1,
        track_type_id=None,
    )

    assert model_id == 100
    hook.run_insert_returning_id.assert_called_once()
    hook.get_records.assert_not_called()
    params = hook.run_insert_returning_id.call_args.kwargs.get("parameters") or (
        hook.run_insert_returning_id.call_args[0][1]
    )
    assert params[0] == 99
