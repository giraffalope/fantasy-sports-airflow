"""Scope-aware helpers for per-track / per-track-type / global ML models.

``ml_models.model_scope`` is the routing discriminator the predict task uses to
pick the right artifact for an upcoming race week. This module owns:

- The canonical scope constants (``SCOPE_TRACK``, ``SCOPE_TRACK_TYPE``, ``SCOPE_GLOBAL``).
- The feature-key allowlist for each scope. Snapshot ``features_json`` contains
  per-track (``*_pt_*``) keys and per-track-type (``tt_<id>_*``) keys side by
  side; a track model should only see the per-track family, a track-type
  model should only see the column block for *its* type, and the global
  fallback gets everything.
- Min-week thresholds and a small grouping helper for ``train_task`` to slice
  ``load_training_frame`` output into per-scope sub-frames.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any

SCOPE_TRACK = "track"
SCOPE_TRACK_TYPE = "track_type"
SCOPE_TRACK_TYPE_BLEND = "track_type_blend"
SCOPE_GLOBAL = "global"
ALL_SCOPES: tuple[str, ...] = (SCOPE_TRACK, SCOPE_TRACK_TYPE, SCOPE_GLOBAL)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def min_track_weeks() -> int:
    """Minimum distinct finished weeks at a track before we train a track model."""
    return _env_int("FS_ML_MIN_TRACK_WEEKS", 3)


def min_track_type_weeks() -> int:
    """Minimum distinct finished weeks across a track type before we train its model."""
    return _env_int("FS_ML_MIN_TYPE_WEEKS", 3)


def global_fallback_enabled() -> bool:
    """Whether ``predict_upcoming_week`` may fall back to a global model."""
    return _env_flag("FS_ML_ENABLE_GLOBAL_FALLBACK", True)


def max_train_weeks() -> int:
    """
    Cap global-scope training to the N most recent distinct finished weeks.

    ``FS_ML_MAX_TRAIN_WEEKS=0`` (default) means no cap. Per-track and per-track-type
    loads are already bounded by venue/type and do not use this limit.
    """
    return _env_int("FS_ML_MAX_TRAIN_WEEKS", 0)


# Per-track aggregate sources expose features with a trailing ``_pt`` prefix
# component (see ``snapshot_config.PREV_TRACK_SOURCES``). The flattened track
# types families use ``tt_<track_type_id>_<metric>`` as the column suffix
# (see ``flatten_agg_week_prev_track_types_results`` stored procedure).
_PT_INFIX = "_pt_"
_TT_INFIX = "_tt_"


def _is_per_track_key(name: str) -> bool:
    return _PT_INFIX in name


def _is_per_track_type_key(name: str) -> bool:
    return _TT_INFIX in name


def _matches_track_type(name: str, track_type_id: int) -> bool:
    needle = f"{_TT_INFIX}{int(track_type_id)}_"
    return needle in name


def features_for_scope(
    feature_keys: Iterable[str],
    *,
    scope: str,
    track_type_id: int | None = None,
) -> list[str]:
    """
    Return the subset of ``feature_keys`` appropriate for ``scope``.

    - ``track``: only ``*_pt_*`` keys (history at the same physical track).
    - ``track_type``: only ``*_tt_<track_type_id>_*`` keys; non-track-type
      anchor columns (``r_start_position`` etc.) also pass through because they
      describe the upcoming race, not history.
    - ``global``: every key passes through (legacy behavior).

    Anchor / non-history columns are columns that contain neither the
    per-track nor the per-track-type infix; we keep those in every scope so
    pre-race signal like start position is always available.
    """
    keys = list(feature_keys)
    if scope == SCOPE_GLOBAL:
        return keys
    if scope == SCOPE_TRACK:
        return [
            name for name in keys if _is_per_track_key(name) or not _is_per_track_type_key(name)
        ]
    if scope == SCOPE_TRACK_TYPE:
        if track_type_id is None:
            return [name for name in keys if not _is_per_track_type_key(name)]
        return [
            name
            for name in keys
            if (_is_per_track_type_key(name) and _matches_track_type(name, track_type_id))
            or not (_is_per_track_type_key(name) or _is_per_track_key(name))
        ]
    raise ValueError(f"unknown model_scope: {scope!r}")


def group_frame_by_series(
    frame: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    """Bucket training rows by ``series_id`` (skips rows missing series_id).

    Train-task wraps the per-track / per-track-type / global loops in an outer
    series loop so models from different NASCAR series (Cup, Xfinity, Truck)
    never share weights or get cross-routed at predict time.
    """
    buckets: dict[int, list[dict[str, Any]]] = {}
    for row in frame:
        series_id = row.get("series_id")
        if series_id is None:
            continue
        buckets.setdefault(int(series_id), []).append(row)
    return buckets


def group_frame_by_track(
    frame: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    """Bucket training rows by ``track_id`` (skips rows missing track_id)."""
    buckets: dict[int, list[dict[str, Any]]] = {}
    for row in frame:
        track_id = row.get("track_id")
        if track_id is None:
            continue
        buckets.setdefault(int(track_id), []).append(row)
    return buckets


def group_frame_by_track_type(
    frame: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    """Bucket training rows by ``track_type_id`` (skips rows missing track_type_id)."""
    buckets: dict[int, list[dict[str, Any]]] = {}
    for row in frame:
        track_type_id = row.get("track_type_id")
        if track_type_id is None:
            continue
        buckets.setdefault(int(track_type_id), []).append(row)
    return buckets


def distinct_week_count(rows: list[dict[str, Any]]) -> int:
    return len({int(row["week_id"]) for row in rows if row.get("week_id") is not None})


def track_type_ids_for_track(hook: Any, track_id: int) -> list[int]:
    """Distinct ``track_type_id`` values mapped to ``track_id`` in ``tracks_track_types``."""
    rows = hook.get_records(
        """
        SELECT DISTINCT track_type_id
        FROM nascar.tracks_track_types
        WHERE track_id = %s
          AND deleted_at IS NULL
        ORDER BY track_type_id ASC
        """,
        parameters=(int(track_id),),
    )
    return [int(row[0]) for row in rows]


def track_meets_threshold(
    hook: Any,
    rules_id: int,
    series_id: int,
    track_id: int,
    min_weeks: int,
    *,
    train_kind: str,
) -> bool:
    """True when ``track_id`` has at least ``min_weeks`` distinct finished train snapshots."""
    rows = hook.get_records(
        """
        SELECT 1
        FROM nascar.ml_training_snapshots s
        WHERE s.kind = %s
          AND s.dfs_scoring_rules_id = %s
          AND s.series_id = %s
          AND s.track_id = %s
          AND s.deleted_at IS NULL
        GROUP BY s.track_id
        HAVING COUNT(DISTINCT s.week_id) >= %s
        LIMIT 1
        """,
        parameters=(train_kind, rules_id, series_id, track_id, min_weeks),
    )
    return bool(rows)


def is_cold_track(
    hook: Any,
    rules_id: int,
    series_id: int,
    track_id: int,
    *,
    train_kind: str,
) -> bool:
    """True when the venue lacks enough finished weeks for a track-scoped model."""
    return not track_meets_threshold(
        hook,
        rules_id,
        series_id,
        track_id,
        min_track_weeks(),
        train_kind=train_kind,
    )
