"""Load multi-output training labels and apply scoring rules to predictions."""

from __future__ import annotations

import json
from typing import Any

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.ml.scoring import (
    compute_dfs_points_from_targets,
    target_keys_for_rules,
)
from include.nascar.ml.snapshot_config import SNAPSHOT_KIND_TRAIN

__all__ = [
    "load_training_frame",
    "predicted_dfs_points_for_rows",
    "target_keys_for_rules",
]


def _parse_json_object(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    raise TypeError(f"Expected JSON object, got {type(raw)!r}")


def _frame_from_records(rows: list[tuple]) -> list[dict[str, Any]]:
    frame: list[dict[str, Any]] = []
    for raw in rows:
        (
            snapshot_id,
            series_id,
            week_id,
            track_id,
            track_type_id,
            car_driver_id,
            driver_id,
            r_start_position,
            features_json,
            targets_json,
            dfs_points,
        ) = raw[:11]
        frame.append(
            {
                "ml_training_snapshot_id": int(snapshot_id),
                "series_id": int(series_id),
                "week_id": int(week_id),
                "track_id": int(track_id),
                "track_type_id": int(track_type_id) if track_type_id is not None else None,
                "car_driver_id": int(car_driver_id),
                "driver_id": int(driver_id),
                "r_start_position": r_start_position,
                "features": _parse_json_object(features_json),
                "targets": _parse_json_object(targets_json),
                "dfs_points": float(dfs_points),
            }
        )
    return frame


def _load_training_frame_by_snapshot_ids(
    ml_training_snapshot_ids: list[int],
    *,
    mysql_conn_id: str,
) -> list[dict[str, Any]]:
    hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)
    placeholders = ", ".join(["%s"] * len(ml_training_snapshot_ids))
    sql = f"""
        SELECT
            r.ml_training_snapshot_id,
            s.series_id,
            s.week_id,
            s.track_id,
            tt.track_type_id,
            r.car_driver_id,
            r.driver_id,
            r.r_start_position,
            r.features_json,
            l.targets_json,
            l.dfs_points
        FROM nascar.ml_training_rows r
        JOIN nascar.ml_labels l
            ON l.ml_training_snapshot_id = r.ml_training_snapshot_id
           AND l.car_driver_id = r.car_driver_id
           AND l.deleted_at IS NULL
        JOIN nascar.ml_training_snapshots s
            ON s.id = r.ml_training_snapshot_id
           AND s.deleted_at IS NULL
        LEFT JOIN (
            SELECT track_id, MIN(track_type_id) AS track_type_id
            FROM nascar.tracks_track_types
            WHERE deleted_at IS NULL
            GROUP BY track_id
        ) tt ON tt.track_id = s.track_id
        WHERE r.ml_training_snapshot_id IN ({placeholders})
          AND r.deleted_at IS NULL
        ORDER BY s.week_id ASC, r.ml_training_snapshot_id ASC, r.car_driver_id ASC
    """
    rows = hook.get_records(sql, parameters=tuple(ml_training_snapshot_ids))
    return _frame_from_records(rows)


def _load_training_frame_filtered(
    *,
    dfs_scoring_rules_id: int,
    series_id: int,
    track_id: int | None = None,
    track_type_id: int | None = None,
    max_weeks: int = 0,
    mysql_conn_id: str,
) -> list[dict[str, Any]]:
    hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)
    filters = [
        "s.kind = %s",
        "s.dfs_scoring_rules_id = %s",
        "s.deleted_at IS NULL",
        "r.deleted_at IS NULL",
        "l.deleted_at IS NULL",
        "s.series_id = %s",
    ]
    params: list[Any] = [
        SNAPSHOT_KIND_TRAIN,
        dfs_scoring_rules_id,
        series_id,
    ]

    if track_id is not None:
        filters.append("s.track_id = %s")
        params.append(track_id)

    if track_type_id is not None:
        filters.append("tt.track_type_id = %s")
        params.append(track_type_id)

    where_clause = " AND ".join(filters)
    recent_weeks_cte = ""
    recent_weeks_join = ""
    if max_weeks > 0:
        recent_weeks_cte = f"""
        recent_weeks AS (
            SELECT DISTINCT s2.week_id
            FROM nascar.ml_training_snapshots s2
            JOIN nascar.ml_training_rows r2
                ON r2.ml_training_snapshot_id = s2.id
               AND r2.deleted_at IS NULL
            JOIN nascar.ml_labels l2
                ON l2.ml_training_snapshot_id = r2.ml_training_snapshot_id
               AND l2.car_driver_id = r2.car_driver_id
               AND l2.deleted_at IS NULL
            LEFT JOIN (
                SELECT track_id, MIN(track_type_id) AS track_type_id
                FROM nascar.tracks_track_types
                WHERE deleted_at IS NULL
                GROUP BY track_id
            ) tt2 ON tt2.track_id = s2.track_id
            WHERE {
            " AND ".join(
                f.replace("s.", "s2.")
                .replace("r.", "r2.")
                .replace("l.", "l2.")
                .replace("tt.", "tt2.")
                for f in filters
            )
        }
            ORDER BY s2.week_id DESC
            LIMIT %s
        ),
        """
        params.append(max_weeks)
        recent_weeks_join = " AND s.week_id IN (SELECT week_id FROM recent_weeks)"

    sql = f"""
        WITH
        {recent_weeks_cte}
        base AS (
            SELECT
                r.ml_training_snapshot_id,
                s.series_id,
                s.week_id,
                s.track_id,
                tt.track_type_id,
                r.car_driver_id,
                r.driver_id,
                r.r_start_position,
                r.features_json,
                l.targets_json,
                l.dfs_points
            FROM nascar.ml_training_rows r
            JOIN nascar.ml_labels l
                ON l.ml_training_snapshot_id = r.ml_training_snapshot_id
               AND l.car_driver_id = r.car_driver_id
               AND l.deleted_at IS NULL
            JOIN nascar.ml_training_snapshots s
                ON s.id = r.ml_training_snapshot_id
               AND s.deleted_at IS NULL
            LEFT JOIN (
                SELECT track_id, MIN(track_type_id) AS track_type_id
                FROM nascar.tracks_track_types
                WHERE deleted_at IS NULL
                GROUP BY track_id
            ) tt ON tt.track_id = s.track_id
            WHERE {where_clause}
              {recent_weeks_join}
        )
        SELECT *
        FROM base
        ORDER BY week_id ASC, ml_training_snapshot_id ASC, car_driver_id ASC
    """
    rows = hook.get_records(sql, parameters=tuple(params))
    return _frame_from_records(rows)


def load_training_frame(
    ml_training_snapshot_ids: list[int] | None = None,
    *,
    dfs_scoring_rules_id: int | None = None,
    series_id: int | None = None,
    track_id: int | None = None,
    track_type_id: int | None = None,
    max_weeks: int = 0,
    mysql_conn_id: str = "nascar_conn",
) -> list[dict[str, Any]]:
    """
    Load rows for model training: features, pre-race start, label targets, dfs_points.

    Legacy: ``load_training_frame([snapshot_id, ...])`` loads exactly those snapshots.

    Scoped: ``load_training_frame(dfs_scoring_rules_id=..., series_id=..., track_id=...)``
    (and optional ``track_type_id``, ``max_weeks``) loads matching train snapshots via
    SQL filters without building a huge ``IN (...)`` list.

    Output is sorted by ``week_id`` for chronological ``TimeSeriesSplit``.
    """
    if ml_training_snapshot_ids is not None:
        if not ml_training_snapshot_ids:
            return []
        return _load_training_frame_by_snapshot_ids(
            ml_training_snapshot_ids,
            mysql_conn_id=mysql_conn_id,
        )

    if dfs_scoring_rules_id is not None and series_id is not None:
        return _load_training_frame_filtered(
            dfs_scoring_rules_id=dfs_scoring_rules_id,
            series_id=series_id,
            track_id=track_id,
            track_type_id=track_type_id,
            max_weeks=max_weeks,
            mysql_conn_id=mysql_conn_id,
        )

    raise TypeError(
        "load_training_frame requires either ml_training_snapshot_ids or "
        "(dfs_scoring_rules_id and series_id)"
    )


def predicted_dfs_points_for_rows(
    rows: list[dict[str, Any]],
    predicted_targets: list[dict[str, float]],
    scoring_rules: dict[str, Any] | str,
) -> list[dict[str, Any]]:
    """
    For each row, apply scoring rules to predicted targets + ``r_start_position``.

    Returns list of dicts with predicted_targets, predicted_dfs_points_breakdown,
    predicted_dfs_points.
    """
    if len(rows) != len(predicted_targets):
        raise ValueError("rows and predicted_targets must have the same length")

    results: list[dict[str, Any]] = []
    for row, targets in zip(rows, predicted_targets, strict=True):
        total, breakdown = compute_dfs_points_from_targets(
            row.get("r_start_position"),
            targets,
            scoring_rules,
        )
        results.append(
            {
                "predicted_targets": targets,
                "predicted_dfs_points_breakdown": breakdown,
                "predicted_dfs_points": float(total),
            }
        )
    return results
