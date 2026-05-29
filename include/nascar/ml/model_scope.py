"""Scope-aware helpers for the cohort track-type ML design.

``ml_models.model_scope`` is the routing discriminator the predict task uses
to pick the right artifact for an upcoming race week. From the cohort design
onward, only ``model_scope = 'track_type'`` is trained: one XGBoost model per
``(series_id, track_type_id, target)`` fit on the union of train snapshots at
every sibling track sharing that ``track_type_id`` (see
``tracks_track_types``).

This module owns:

- The canonical scope constants.
- A strict ``*_pt_*`` + anchor-column feature allowlist (no ``*_tt_*`` keys).
- Sibling-track / cohort helpers the trainer and predict-time blend rely on.
- Environment knobs: ``FS_ML_MIN_COHORT_WEEKS``, ``FS_ML_BLEND_WEIGHTS``,
  ``FS_ML_HOLDOUT_WEEKS``, ``FS_ML_DEV_WEEKS``.

Legacy ``track`` / ``global`` constants are kept as string literals only so
audit tooling that joins on ``ml_models.model_scope`` keeps working; nothing
in this codebase trains or resolves them anymore.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable
from typing import Any

SCOPE_TRACK_TYPE = "track_type"
LEGACY_SCOPE_TRACK = "track"
LEGACY_SCOPE_GLOBAL = "global"
ALL_SCOPES: tuple[str, ...] = (SCOPE_TRACK_TYPE,)

VALID_BLEND_STRATEGIES: frozenset[str] = frozenset(
    {"inverse_mae", "sample_count", "equal", "spearman"}
)
DEFAULT_BLEND_STRATEGY = "inverse_mae"


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def min_cohort_weeks() -> int:
    """Minimum distinct finished ``week_id`` rows before a cohort is trained."""
    return _env_int("FS_ML_MIN_COHORT_WEEKS", 2)


def holdout_weeks() -> int:
    """Trailing distinct ``week_id`` rows reserved as the test holdout.

    The published ``mae`` / DFS metrics on ``ml_model_evaluations`` come from
    this slice; nothing in train, dev, or feature selection sees it.
    """
    return max(0, _env_int("FS_ML_HOLDOUT_WEEKS", 1))


def dev_weeks() -> int:
    """Trailing distinct ``week_id`` rows reserved as the dev slice.

    Sits between train and holdout in time order. Used for hyperparameter
    selection / early stopping so the holdout MAE stays clean. Default 1.
    Falls back to a two-way split when the cohort has too few weeks (see
    ``include.nascar.ml.xgb_train_pipeline.split_train_dev_holdout_masks``).
    """
    return max(0, _env_int("FS_ML_DEV_WEEKS", 1))


def blend_weight_strategy() -> str:
    """How predictions from multiple ``track_type`` cohorts get combined."""
    raw = (os.environ.get("FS_ML_BLEND_WEIGHTS") or DEFAULT_BLEND_STRATEGY).strip().lower()
    if raw not in VALID_BLEND_STRATEGIES:
        return DEFAULT_BLEND_STRATEGY
    return raw


def primary_rank_objective_enabled() -> bool:
    """D2: opt-in switch for fitting the primary head with ``rank:pairwise``.

    Defaults to ``False`` so the existing ``reg:absoluteerror`` primary
    behavior is preserved. Set ``FS_ML_PRIMARY_RANK_OBJECTIVE=1`` (or
    ``true`` / ``yes``) to switch the primary target to a pairwise
    ranker, fit with ``week_id`` as the ``qid`` group key so each race
    week's drivers are ranked against each other. Production should
    only flip this on after a champion/challenger cycle outside this
    plan validates the score-space change downstream.
    """
    raw = (os.environ.get("FS_ML_PRIMARY_RANK_OBJECTIVE") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


INTERVAL_METHOD_CONFORMAL = "conformal"
INTERVAL_METHOD_GLOBAL_MAE = "global_mae"
INTERVAL_METHOD_QUANTILE = "quantile"
VALID_INTERVAL_METHODS: frozenset[str] = frozenset(
    {INTERVAL_METHOD_CONFORMAL, INTERVAL_METHOD_GLOBAL_MAE, INTERVAL_METHOD_QUANTILE}
)
DEFAULT_INTERVAL_METHOD = INTERVAL_METHOD_CONFORMAL


def interval_method() -> str:
    """E1: per-driver interval method consumed by Monte Carlo / interval reporting.

    - ``conformal`` (default): pull per-cohort residual quantiles from
      ``ml_models.holdout_residual_quantiles`` and size per-driver σ off
      the (q90 - q10) / 2 half-width.
    - ``global_mae``: legacy behavior — σ derived from the global MAE
      evaluation row via the Gaussian conversion (σ ≈ mae / 0.7979).
      Kept as a one-line rollback when the conformal path mis-calibrates.
    - ``quantile``: reserved for the v3 quantile-regression heads (one
      model per ``q ∈ {0.1, 0.5, 0.9}`` per target). Until those exist
      the helper falls back to ``conformal`` and logs a one-line warning.

    Unknown values silently fall back to the default so a stray typo on
    one worker doesn't tank predict / Monte Carlo.
    """
    raw = (os.environ.get("FS_ML_INTERVAL_METHOD") or DEFAULT_INTERVAL_METHOD).strip().lower()
    if raw not in VALID_INTERVAL_METHODS:
        return DEFAULT_INTERVAL_METHOD
    return raw


_PT_INFIX = "_pt_"
_TT_INFIX = "_tt_"


def _is_per_track_key(name: str) -> bool:
    return _PT_INFIX in name


def _is_per_track_type_key(name: str) -> bool:
    return _TT_INFIX in name


def _cohort_tt_pattern(track_type_id: int) -> re.Pattern[str]:
    """Match ``*_tt_<track_type_id>_*`` while leaving ``_tt_<other>_`` alone.

    The trailing ``_`` is required so a cohort ``track_type_id=3`` doesn't
    also strip ``_tt_30_*`` etc.
    """
    return re.compile(rf"_tt_{int(track_type_id)}_")


def features_for_scope(
    feature_keys: Iterable[str],
    *,
    scope: str = SCOPE_TRACK_TYPE,
    track_type_id: int | None = None,
) -> list[str]:
    """Return the subset of ``feature_keys`` allowed for one cohort fit.

    Selective ``*_tt_*`` rule (C3): keep per-track (``*_pt_*``) keys, all
    anchor / non-history columns (``r_start_position``, ``q_*``, ``pt_*``,
    ``pf_*``, ``df_*``, ``qf_*``, ``season_*``, ``track_attr_*``, ...), and
    every per-track-type (``*_tt_<other_type>_*``) key except the cohort's
    own ``_tt_<track_type_id>_*`` columns. The cohort's own track-type
    aggregates are constant within the training frame by construction (all
    rows share that ``track_type_id``) and add only noise; opposing-type
    aggregates carry signal about how a driver fares on different track
    geometries and are kept.

    When ``track_type_id`` is ``None`` the function falls back to the legacy
    behavior and strips every ``*_tt_*`` key. That path is preserved so
    callers that iterate the union of all cohort features keep working.
    """
    if scope != SCOPE_TRACK_TYPE:
        raise ValueError(
            f"features_for_scope: unsupported scope {scope!r}; "
            f"only {SCOPE_TRACK_TYPE!r} is trained under the cohort design"
        )
    if track_type_id is None:
        return [name for name in feature_keys if not _is_per_track_type_key(name)]

    cohort_pattern = _cohort_tt_pattern(track_type_id)
    return [name for name in feature_keys if not cohort_pattern.search(name)]


def group_frame_by_series(
    frame: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    """Bucket training rows by ``series_id`` (skips rows missing ``series_id``)."""
    buckets: dict[int, list[dict[str, Any]]] = {}
    for row in frame:
        series_id = row.get("series_id")
        if series_id is None:
            continue
        buckets.setdefault(int(series_id), []).append(row)
    return buckets


def group_frame_by_track_type(
    frame: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    """Bucket training rows by ``track_type_id``."""
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


def sibling_track_ids_for_type(
    hook: Any,
    *,
    series_id: int,
    rules_id: int,
    track_type_id: int,
) -> list[int]:
    """Tracks that share ``track_type_id`` and have at least one active train snapshot.

    Empty when the cohort is not yet populated for ``(series_id, rules_id)``.
    """
    rows = hook.get_records(
        """
        SELECT DISTINCT s.track_id
        FROM nascar.tracks_track_types tt
        JOIN nascar.ml_training_snapshots s
            ON s.track_id = tt.track_id
           AND s.series_id = %s
           AND s.dfs_scoring_rules_id = %s
           AND s.kind = 'train'
           AND s.deleted_at IS NULL
        WHERE tt.track_type_id = %s
          AND tt.deleted_at IS NULL
        ORDER BY s.track_id ASC
        """,
        parameters=(int(series_id), int(rules_id), int(track_type_id)),
    )
    return [int(row[0]) for row in rows]


def cohort_distinct_weeks(
    hook: Any,
    *,
    series_id: int,
    rules_id: int,
    track_type_id: int,
) -> int:
    """Distinct finished ``week_id`` rows in the cohort's union of train snapshots."""
    rows = hook.get_records(
        """
        SELECT COUNT(DISTINCT s.week_id)
        FROM nascar.tracks_track_types tt
        JOIN nascar.ml_training_snapshots s
            ON s.track_id = tt.track_id
           AND s.series_id = %s
           AND s.dfs_scoring_rules_id = %s
           AND s.kind = 'train'
           AND s.deleted_at IS NULL
        WHERE tt.track_type_id = %s
          AND tt.deleted_at IS NULL
        """,
        parameters=(int(series_id), int(rules_id), int(track_type_id)),
    )
    if not rows or rows[0][0] is None:
        return 0
    return int(rows[0][0])


def cohort_latest_train_week(
    hook: Any,
    *,
    series_id: int,
    rules_id: int,
    track_type_id: int,
) -> int | None:
    """Latest ``week_id`` present in this cohort's union of train snapshots."""
    rows = hook.get_records(
        """
        SELECT MAX(s.week_id)
        FROM nascar.tracks_track_types tt
        JOIN nascar.ml_training_snapshots s
            ON s.track_id = tt.track_id
           AND s.series_id = %s
           AND s.dfs_scoring_rules_id = %s
           AND s.kind = 'train'
           AND s.deleted_at IS NULL
        WHERE tt.track_type_id = %s
          AND tt.deleted_at IS NULL
        """,
        parameters=(int(series_id), int(rules_id), int(track_type_id)),
    )
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])
