"""Airflow callable for building ML training + prediction snapshots."""

from __future__ import annotations

import logging
import os

from include.nascar.ml.snapshot_build import (
    build_all_week_prediction_snapshots,
    build_all_week_training_snapshots,
)

_LOG = logging.getLogger(__name__)


def build_ml_training_snapshots_task(**_context: object) -> dict[str, list[int]]:
    """Materialize all train + predict ML snapshots for Cup. Returns ids by kind.

    ``train_ids`` lists only the snapshots that were (re)built on this run;
    weeks whose existing train snapshot already matches the current
    ``pipeline_version`` are skipped by the gate inside
    ``build_all_week_training_snapshots``.
    """
    version = os.environ.get("FS_PIPELINE_VERSION") or os.environ.get("GIT_COMMIT")
    train_ids = build_all_week_training_snapshots(pipeline_version=version)
    predict_ids = build_all_week_prediction_snapshots(pipeline_version=version)
    _LOG.info(
        "build_ml_training_snapshots_task: train_rebuilt=%d predict_built=%d pipeline_version=%s",
        len(train_ids),
        len(predict_ids),
        version,
    )
    return {"train": train_ids, "predict": predict_ids}
