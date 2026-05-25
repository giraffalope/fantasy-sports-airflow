"""Airflow callable for walk-forward ML projection backtest."""

from __future__ import annotations

import logging
import os

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.enums import NascarSeries
from include.nascar.ml.backtest import run_walk_forward_backtest

_LOG = logging.getLogger(__name__)


def run_ml_backtest_task(**_context: object) -> dict[str, object]:
    """Run walk-forward backtest when ``FS_ML_RUN_BACKTEST=1`` (skipped otherwise)."""
    if os.environ.get("FS_ML_RUN_BACKTEST", "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        _LOG.info("run_ml_backtest_task: skipped (FS_ML_RUN_BACKTEST not set)")
        return {"skipped": True, "reason": "FS_ML_RUN_BACKTEST not set"}

    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    result = run_walk_forward_backtest(hook, series_id=NascarSeries.CUP.series_id)
    return result
