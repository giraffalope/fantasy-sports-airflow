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
    """Materialize all train + predict ML snapshots for Cup. Returns ids by kind."""
    version = os.environ.get("FS_PIPELINE_VERSION") or os.environ.get("GIT_COMMIT")
    train_ids = build_all_week_training_snapshots(pipeline_version=version)
    predict_ids = build_all_week_prediction_snapshots(pipeline_version=version)
    _LOG.info(
        "build_ml_training_snapshots_task: train=%s predict=%s",
        len(train_ids),
        len(predict_ids),
    )
    return {"train": train_ids, "predict": predict_ids}
