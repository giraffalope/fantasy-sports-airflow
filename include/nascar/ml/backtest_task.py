"""Airflow callable for walk-forward ML projection backtest."""

from __future__ import annotations

import logging

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.enums import NascarSeries
from include.nascar.ml.backtest import run_walk_forward_backtest

_LOG = logging.getLogger(__name__)


def run_ml_backtest_task(**_context: object) -> dict[str, object]:
    """Run walk-forward backtest over recent finished train weeks."""
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    result = run_walk_forward_backtest(hook, series_id=NascarSeries.CUP.series_id)
    return result
