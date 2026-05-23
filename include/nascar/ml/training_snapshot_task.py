"""Airflow callable for building ML training snapshots after aggregate loads."""

from __future__ import annotations

import os

from include.nascar.ml.snapshot_build import build_all_week_training_snapshots


def build_ml_training_snapshots_task(**_context: object) -> list[int]:
    version = os.environ.get("FS_PIPELINE_VERSION") or os.environ.get("GIT_COMMIT")
    return build_all_week_training_snapshots(pipeline_version=version)
