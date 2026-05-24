"""Build ml_training_snapshots, ml_training_rows, and ml_labels for a race week.

Two snapshot kinds are materialized:

- ``train`` snapshots include both features and labels for finished races
  (``r_finish_position IS NOT NULL``).
- ``predict`` snapshots include features only for upcoming races with known
  starting positions (``r_start_position IS NOT NULL AND r_finish_position IS NULL``).
  No ``ml_labels`` rows are written for predict snapshots.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import time, timedelta
from decimal import Decimal
from typing import Any

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.ml.scoring import (
    compute_dfs_points_from_targets,
    target_keys_for_rules,
    targets_from_agg_row,
)
from include.nascar.ml.snapshot_config import (
    AUDIT_COLUMNS,
    DRAFTKINGS_DFS_ID,
    GRAIN_CAR_DRIVER,
    GRAIN_COLUMNS,
    PREV_TRACK_SOURCES,
    PREV_TRACK_TYPES_SOURCES,
    SNAPSHOT_KIND_PREDICT,
    SNAPSHOT_KIND_TRAIN,
    TIME_COLUMN_SUFFIXES,
    PrevTrackSource,
    PrevTrackTypesSource,
)

_VALID_KINDS = frozenset({SNAPSHOT_KIND_TRAIN, SNAPSHOT_KIND_PREDICT})

_PREDICT_WEEKS_SQL = """
SELECT DISTINCT s.series_id, w.id AS week_id
FROM nascar.agg_week_results awr
JOIN nascar.weeks w ON awr.week_id = w.id AND w.deleted_at IS NULL
JOIN nascar.seasons s ON w.season_id = s.id AND s.deleted_at IS NULL
WHERE awr.deleted_at IS NULL
  AND awr.r_start_position IS NOT NULL
  AND awr.r_finish_position IS NULL
  AND s.series_id = %s
ORDER BY w.id
"""

_LOG = logging.getLogger(__name__)

_METRIC_COLUMNS_CACHE: dict[str, list[str]] = {}


def time_value_to_seconds(value: Any) -> float | None:
    """Convert TIME/timedelta/decimal race values to seconds for features_json."""
    if value is None:
        return None
    if isinstance(value, timedelta):
        return round(value.total_seconds(), 3)
    if isinstance(value, time):
        return round(value.hour * 3600 + value.minute * 60 + value.second + value.microsecond / 1e6, 3)
    if isinstance(value, Decimal):
        return round(float(value), 3)
    if isinstance(value, (int, float)):
        return round(float(value), 3)
    if isinstance(value, str):
        parts = value.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = parts
            sec_parts = seconds.split(".")
            sec = float(sec_parts[0])
            micro = float(f"0.{sec_parts[1]}") if len(sec_parts) > 1 else 0.0
            return round(int(hours) * 3600 + int(minutes) * 60 + sec + micro, 3)
    return None


def normalize_feature_value(column_name: str, value: Any) -> float | None:
    if value is None:
        return None
    if column_name.endswith(TIME_COLUMN_SUFFIXES):
        return time_value_to_seconds(value)
    if isinstance(value, Decimal):
        return round(float(value), 3)
    if isinstance(value, (int, float)):
        return round(float(value), 3)
    return None


def _metric_columns(hook: NascarMySqlHook, table_name: str) -> list[str]:
    if table_name in _METRIC_COLUMNS_CACHE:
        return _METRIC_COLUMNS_CACHE[table_name]

    rows = hook.get_records(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = 'nascar' AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        parameters=(table_name,),
    )
    metrics: list[str] = []
    for (column_name,) in rows:
        if column_name in AUDIT_COLUMNS or column_name in GRAIN_COLUMNS:
            continue
        metrics.append(column_name)

    _METRIC_COLUMNS_CACHE[table_name] = metrics
    return metrics


def _prefixed_key(prefix: str, column_name: str) -> str:
    return f"{prefix}_{column_name}"


def _row_to_prefixed_features(
    row: dict[str, Any],
    metric_columns: list[str],
    prefix: str,
) -> dict[str, float | None]:
    features: dict[str, float | None] = {}
    for column in metric_columns:
        key = _prefixed_key(prefix, column)
        features[key] = normalize_feature_value(column, row.get(column))
    return features


def _fetch_flattened_track_types(
    hook: NascarMySqlHook,
    series_id: int,
    week_id: int,
    source: PrevTrackTypesSource,
) -> dict[int, dict[str, Any]]:
    sql = (
        "CALL nascar.flatten_agg_week_prev_track_types_results(%s, %s, %s)"
    )
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, (series_id, week_id, source.table_name))
            description = cursor.description
            if not description:
                return {}
            column_names = [col[0] for col in description]
            raw_rows = cursor.fetchall()
            while cursor.nextset():
                pass

    join_column = source.join_column
    keyed: dict[int, dict[str, Any]] = {}
    for raw in raw_rows:
        row = dict(zip(column_names, raw, strict=True))
        entity_id = row.get(join_column)
        if entity_id is not None:
            keyed[int(entity_id)] = row
    return keyed


def _fetch_prev_track(
    hook: NascarMySqlHook,
    series_id: int,
    week_id: int,
    track_id: int,
    source: PrevTrackSource,
    metric_columns: list[str],
) -> dict[int, dict[str, Any]]:
    if not metric_columns:
        return {}

    select_list = ", ".join(f"`{column}`" for column in metric_columns)
    sql = f"""
        SELECT `{source.join_column}`, {select_list}
        FROM nascar.`{source.table_name}`
        WHERE series_id = %s
          AND week_id = %s
          AND track_id = %s
          AND deleted_at IS NULL
    """
    rows = hook.get_records(sql, parameters=(series_id, week_id, track_id))
    keyed: dict[int, dict[str, Any]] = {}
    for raw in rows:
        row = dict(zip([source.join_column, *metric_columns], raw, strict=True))
        entity_id = row.pop(source.join_column)
        if entity_id is not None:
            keyed[int(entity_id)] = row
    return keyed


def _feature_schema_hash(feature_keys: list[str]) -> str:
    canonical = json.dumps(feature_keys, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _active_rules(hook: NascarMySqlHook, dfs_id: int) -> tuple[int, dict[str, Any]] | None:
    rows = hook.get_records(
        """
        SELECT id, rules
        FROM nascar.dfs_scoring_rules
        WHERE dfs_id = %s AND deleted_at IS NULL
        ORDER BY version DESC, id DESC
        LIMIT 1
        """,
        parameters=(dfs_id,),
    )
    if not rows:
        return None
    rules_id, rules_raw = rows[0]
    if isinstance(rules_raw, (bytes, bytearray)):
        rules_raw = rules_raw.decode("utf-8")
    if isinstance(rules_raw, str):
        rules = json.loads(rules_raw)
    else:
        rules = rules_raw
    return int(rules_id), rules


def _load_scoring_rules(hook: NascarMySqlHook, dfs_id: int) -> tuple[int, dict[str, Any]]:
    rules = _active_rules(hook, dfs_id)
    if rules is None:
        raise ValueError(f"No dfs_scoring_rules found for dfs_id={dfs_id}")
    return rules


def _delete_existing_snapshot(
    hook: NascarMySqlHook,
    *,
    series_id: int,
    week_id: int,
    rules_id: int,
    kind: str,
) -> None:
    """Remove any prior snapshot for this logical key before rebuild.

    Physical delete is required: the unique key on
    ``(series_id, week_id, grain, kind, dfs_scoring_rules_id)`` does not
    include ``deleted_at``, so soft-delete would still block INSERT.
    Child rows cascade via FK (rows, labels, model links, predictions).
    """
    hook.run(
        """
        DELETE FROM nascar.ml_training_snapshots
        WHERE series_id = %s
          AND week_id = %s
          AND grain = %s
          AND kind = %s
          AND dfs_scoring_rules_id = %s
        """,
        parameters=(series_id, week_id, GRAIN_CAR_DRIVER, kind, rules_id),
    )


def _insert_snapshot(
    hook: NascarMySqlHook,
    *,
    series_id: int,
    season_id: int,
    week_id: int,
    track_id: int,
    feature_keys: list[str],
    target_keys: list[str],
    dfs_id: int,
    rules_id: int,
    kind: str,
    pipeline_version: str | None,
) -> int:
    feature_column_count = len(feature_keys)
    feature_schema_hash = _feature_schema_hash(feature_keys)
    target_column_count = len(target_keys)
    target_schema_hash = _feature_schema_hash(target_keys)
    hook.run(
        """
        INSERT INTO nascar.ml_training_snapshots (
            series_id,
            season_id,
            week_id,
            track_id,
            grain,
            kind,
            feature_keys,
            feature_column_count,
            feature_schema_hash,
            target_keys,
            target_column_count,
            target_schema_hash,
            dfs_id,
            dfs_scoring_rules_id,
            source_pipeline_version
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        parameters=(
            series_id,
            season_id,
            week_id,
            track_id,
            GRAIN_CAR_DRIVER,
            kind,
            json.dumps(feature_keys),
            feature_column_count,
            feature_schema_hash,
            json.dumps(target_keys),
            target_column_count,
            target_schema_hash,
            dfs_id,
            rules_id,
            pipeline_version,
        ),
    )
    snapshot_rows = hook.get_records(
        """
        SELECT id FROM nascar.ml_training_snapshots
        WHERE series_id = %s AND week_id = %s AND grain = %s
          AND kind = %s AND dfs_scoring_rules_id = %s AND deleted_at IS NULL
        ORDER BY id DESC LIMIT 1
        """,
        parameters=(series_id, week_id, GRAIN_CAR_DRIVER, kind, rules_id),
    )
    return int(snapshot_rows[0][0])


def build_car_driver_training_snapshot(
    series_id: int,
    week_id: int,
    *,
    kind: str = SNAPSHOT_KIND_TRAIN,
    mysql_conn_id: str = "nascar_conn",
    dfs_id: int = DRAFTKINGS_DFS_ID,
    pipeline_version: str | None = None,
) -> int | None:
    """
    Materialize snapshot + rows (+ labels when ``kind='train'``) for one week.

    Returns snapshot id or None when no rows match. ``kind='predict'`` skips
    writing ``ml_labels`` and selects rows where the race has not yet finished
    but the starting grid is known.
    """
    if kind not in _VALID_KINDS:
        raise ValueError(f"Unknown snapshot kind: {kind!r}; expected one of {sorted(_VALID_KINDS)}")

    hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)
    rules = _active_rules(hook, dfs_id)
    if rules is None:
        _LOG.warning(
            "build_car_driver_training_snapshot: no active dfs_scoring_rules for dfs_id=%s",
            dfs_id,
        )
        return None
    rules_id, scoring_rules = rules
    label_target_keys = target_keys_for_rules(scoring_rules)

    if kind == SNAPSHOT_KIND_TRAIN:
        anchor_filter = "AND awr.r_finish_position IS NOT NULL"
    else:
        anchor_filter = "AND awr.r_start_position IS NOT NULL AND awr.r_finish_position IS NULL"

    anchor_sql = f"""
        SELECT
            awr.id,
            awr.series_id,
            awr.season_id,
            awr.week_id,
            awr.track_id,
            awr.race_id,
            awr.car_driver_id,
            awr.driver_id,
            awr.car_id,
            awr.team_id,
            awr.manufacturer_id,
            awr.car_generation_id,
            awr.car_generation_model_id,
            awr.r_start_position,
            awr.r_finish_position,
            awr.r_place_diff,
            awr.r_total_laps_completed,
            awr.r_total_laps_led,
            awr.r_best_position,
            awr.r_worst_position,
            awr.r_average_position,
            awr.r_green_flag_passes,
            awr.r_green_flag_times_passed,
            awr.r_green_flag_pass_diff,
            awr.r_quality_passes,
            awr.r_quality_passes_pct,
            awr.r_total_fastest_laps,
            awr.r_total_fastest_laps_pct,
            awr.r_total_top_15_laps,
            awr.r_total_top_15_laps_pct,
            awr.r_total_laps_completed_pct,
            awr.r_total_laps_led_pct,
            awr.r_driver_rating,
            awr.r_total_pitstops,
            awr.r_total_penalties,
            awr.r_total_race_laps,
            awr.r_total_race_miles
        FROM nascar.agg_week_results awr
        WHERE awr.series_id = %s
          AND awr.week_id = %s
          AND awr.deleted_at IS NULL
          {anchor_filter}
    """
    anchor_rows = hook.get_records(
        anchor_sql,
        parameters=(series_id, week_id),
    )
    if not anchor_rows:
        _LOG.info(
            "build_car_driver_training_snapshot: no agg_week_results for series_id=%s week_id=%s kind=%s",
            series_id,
            week_id,
            kind,
        )
        return None

    anchor_columns = [
        "id",
        "series_id",
        "season_id",
        "week_id",
        "track_id",
        "race_id",
        "car_driver_id",
        "driver_id",
        "car_id",
        "team_id",
        "manufacturer_id",
        "car_generation_id",
        "car_generation_model_id",
        "r_start_position",
        "r_finish_position",
        "r_place_diff",
        "r_total_laps_completed",
        "r_total_laps_led",
        "r_best_position",
        "r_worst_position",
        "r_average_position",
        "r_green_flag_passes",
        "r_green_flag_times_passed",
        "r_green_flag_pass_diff",
        "r_quality_passes",
        "r_quality_passes_pct",
        "r_total_fastest_laps",
        "r_total_fastest_laps_pct",
        "r_total_top_15_laps",
        "r_total_top_15_laps_pct",
        "r_total_laps_completed_pct",
        "r_total_laps_led_pct",
        "r_driver_rating",
        "r_total_pitstops",
        "r_total_penalties",
        "r_total_race_laps",
        "r_total_race_miles",
    ]
    anchors = [
        dict(zip(anchor_columns, raw, strict=True)) for raw in anchor_rows
    ]

    track_id = int(anchors[0]["track_id"])
    season_id = int(anchors[0]["season_id"])

    prev_track_metrics = {
        source.table_name: _metric_columns(hook, source.table_name)
        for source in PREV_TRACK_SOURCES
    }
    prev_track_types_metrics = {
        source.table_name: _metric_columns(hook, source.table_name)
        for source in PREV_TRACK_TYPES_SOURCES
    }

    prev_track_data = {
        source.table_name: _fetch_prev_track(
            hook, series_id, week_id, track_id, source, prev_track_metrics[source.table_name]
        )
        for source in PREV_TRACK_SOURCES
    }
    prev_track_types_data = {
        source.table_name: _fetch_flattened_track_types(hook, series_id, week_id, source)
        for source in PREV_TRACK_TYPES_SOURCES
    }

    training_rows: list[dict[str, Any]] = []
    label_rows: list[dict[str, Any]] = []
    all_feature_keys: set[str] = set()

    for anchor in anchors:
        features: dict[str, float | None] = {}
        entity_ids = {
            "car_driver_id": int(anchor["car_driver_id"]),
            "driver_id": int(anchor["driver_id"]),
            "car_id": int(anchor["car_id"]),
            "team_id": int(anchor["team_id"]) if anchor.get("team_id") is not None else None,
            "manufacturer_id": int(anchor["manufacturer_id"]),
            "car_generation_id": int(anchor["car_generation_id"]),
            "car_generation_model_id": int(anchor["car_generation_model_id"]),
        }

        for source in PREV_TRACK_SOURCES:
            join_id = entity_ids[source.join_column]
            if join_id is None:
                continue
            prev_row = prev_track_data[source.table_name].get(join_id)
            if not prev_row:
                continue
            prefixed = _row_to_prefixed_features(
                prev_row,
                prev_track_metrics[source.table_name],
                source.prefix,
            )
            features.update(prefixed)

        for source in PREV_TRACK_TYPES_SOURCES:
            join_id = entity_ids[source.join_column]
            if join_id is None:
                continue
            flat_row = prev_track_types_data[source.table_name].get(join_id)
            if not flat_row:
                continue
            for column, value in flat_row.items():
                if column in GRAIN_COLUMNS or column in AUDIT_COLUMNS:
                    continue
                key = _prefixed_key(source.prefix, column)
                features[key] = normalize_feature_value(column, value)

        all_feature_keys.update(features.keys())

        training_rows.append(
            {
                "car_driver_id": entity_ids["car_driver_id"],
                "driver_id": entity_ids["driver_id"],
                "car_id": entity_ids["car_id"],
                "team_id": entity_ids["team_id"],
                "manufacturer_id": entity_ids["manufacturer_id"],
                "car_generation_id": entity_ids["car_generation_id"],
                "car_generation_model_id": entity_ids["car_generation_model_id"],
                "r_start_position": anchor.get("r_start_position"),
                "features_json": features,
            }
        )

        if kind == SNAPSHOT_KIND_TRAIN:
            targets = targets_from_agg_row(anchor, scoring_rules)
            dfs_points, breakdown = compute_dfs_points_from_targets(
                anchor.get("r_start_position"),
                targets,
                scoring_rules,
            )
            label_rows.append(
                {
                    "car_driver_id": entity_ids["car_driver_id"],
                    "targets_json": targets,
                    "dfs_points": dfs_points,
                    "dfs_points_breakdown": breakdown,
                }
            )

    feature_keys = sorted(all_feature_keys)
    for row in training_rows:
        row_features = row["features_json"]
        for key in feature_keys:
            row_features.setdefault(key, None)

    _delete_existing_snapshot(
        hook,
        series_id=series_id,
        week_id=week_id,
        rules_id=rules_id,
        kind=kind,
    )
    snapshot_id = _insert_snapshot(
        hook,
        series_id=series_id,
        season_id=season_id,
        week_id=week_id,
        track_id=track_id,
        feature_keys=feature_keys,
        target_keys=label_target_keys,
        dfs_id=dfs_id,
        rules_id=rules_id,
        kind=kind,
        pipeline_version=pipeline_version,
    )

    for row in training_rows:
        hook.run(
            """
            INSERT INTO nascar.ml_training_rows (
                ml_training_snapshot_id,
                car_driver_id,
                driver_id,
                car_id,
                team_id,
                manufacturer_id,
                car_generation_id,
                car_generation_model_id,
                r_start_position,
                features_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                snapshot_id,
                row["car_driver_id"],
                row["driver_id"],
                row["car_id"],
                row["team_id"],
                row["manufacturer_id"],
                row["car_generation_id"],
                row["car_generation_model_id"],
                row["r_start_position"],
                json.dumps(row["features_json"]),
            ),
        )

    for label in label_rows:
        hook.run(
            """
            INSERT INTO nascar.ml_labels (
                ml_training_snapshot_id,
                car_driver_id,
                targets_json,
                dfs_points,
                dfs_points_breakdown
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            parameters=(
                snapshot_id,
                label["car_driver_id"],
                json.dumps(label["targets_json"]),
                label["dfs_points"],
                json.dumps(label["dfs_points_breakdown"]),
            ),
        )

    _LOG.info(
        "build_car_driver_training_snapshot: series_id=%s week_id=%s kind=%s snapshot_id=%s rows=%s features=%s targets=%s",
        series_id,
        week_id,
        kind,
        snapshot_id,
        len(training_rows),
        len(feature_keys),
        len(label_target_keys),
    )
    return snapshot_id


def build_all_week_training_snapshots(
    *,
    mysql_conn_id: str = "nascar_conn",
    pipeline_version: str | None = None,
) -> list[int]:
    """Build train snapshots for every Cup week with finished agg_week_results."""
    from include.nascar.cup.aggregate_load import FINISHED_AGG_WEEKS_SQL
    from include.nascar.enums import NascarSeries

    hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)
    if _active_rules(hook, DRAFTKINGS_DFS_ID) is None:
        _LOG.warning(
            "build_all_week_training_snapshots: no active dfs_scoring_rules for dfs_id=%s",
            DRAFTKINGS_DFS_ID,
        )
        return []

    series_id = NascarSeries.CUP.series_id
    weeks = hook.get_records(FINISHED_AGG_WEEKS_SQL, parameters=(series_id,))

    snapshot_ids: list[int] = []
    for _series_id, week_id in weeks:
        snapshot_id = build_car_driver_training_snapshot(
            series_id,
            int(week_id),
            kind=SNAPSHOT_KIND_TRAIN,
            mysql_conn_id=mysql_conn_id,
            pipeline_version=pipeline_version,
        )
        if snapshot_id is not None:
            snapshot_ids.append(snapshot_id)
    return snapshot_ids


def build_all_week_prediction_snapshots(
    *,
    mysql_conn_id: str = "nascar_conn",
    pipeline_version: str | None = None,
) -> list[int]:
    """Build predict snapshots for Cup weeks with known starting grids but no finish yet."""
    from include.nascar.enums import NascarSeries

    hook = NascarMySqlHook(mysql_conn_id=mysql_conn_id)
    if _active_rules(hook, DRAFTKINGS_DFS_ID) is None:
        _LOG.warning(
            "build_all_week_prediction_snapshots: no active dfs_scoring_rules for dfs_id=%s",
            DRAFTKINGS_DFS_ID,
        )
        return []

    series_id = NascarSeries.CUP.series_id
    weeks = hook.get_records(_PREDICT_WEEKS_SQL, parameters=(series_id,))

    snapshot_ids: list[int] = []
    for _series_id, week_id in weeks:
        snapshot_id = build_car_driver_training_snapshot(
            series_id,
            int(week_id),
            kind=SNAPSHOT_KIND_PREDICT,
            mysql_conn_id=mysql_conn_id,
            pipeline_version=pipeline_version,
        )
        if snapshot_id is not None:
            snapshot_ids.append(snapshot_id)
    return snapshot_ids
