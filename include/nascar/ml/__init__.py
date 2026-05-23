"""NASCAR ML training snapshot and DFS scoring helpers."""

from __future__ import annotations

from include.nascar.ml.scoring import (
    build_scoring_row,
    compute_dfs_points,
    compute_dfs_points_from_targets,
    target_keys_for_rules,
    targets_from_agg_row,
)

__all__ = [
    "build_car_driver_training_snapshot",
    "build_scoring_row",
    "compute_dfs_points",
    "compute_dfs_points_from_targets",
    "load_training_frame",
    "predicted_dfs_points_for_rows",
    "target_keys_for_rules",
    "targets_from_agg_row",
]


def __getattr__(name: str):
    if name == "build_car_driver_training_snapshot":
        from include.nascar.ml.snapshot_build import build_car_driver_training_snapshot

        return build_car_driver_training_snapshot
    if name in ("load_training_frame", "predicted_dfs_points_for_rows"):
        from include.nascar.ml import training_targets

        return getattr(training_targets, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
