"""Smoke tests that Cup DAGs inherit shared retry defaults."""

from __future__ import annotations

import nascar.cup.aggregate as aggregate_dag
import nascar.cup.importer as importer_dag
import nascar.cup.machine_learning as ml_dag
import nascar.cup.machine_learning_backtest as ml_backtest_dag
import pytest
from nascar.cup.dag_defaults import CUP_DAG_DEFAULT_ARGS


def test_cup_dag_default_args_retries() -> None:
    assert CUP_DAG_DEFAULT_ARGS["retries"] == 2


@pytest.mark.parametrize(
    "dag",
    [
        importer_dag.dag,
        aggregate_dag.dag,
        ml_dag.dag,
        ml_backtest_dag.dag,
    ],
)
def test_core_cup_dags_use_shared_retries(dag) -> None:
    assert dag.default_args["retries"] == 2
