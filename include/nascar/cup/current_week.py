"""Resolve each series' live week from ``v_series_current_week``."""

from __future__ import annotations

from dataclasses import dataclass

from airflow_utils.nascar_mysql_hook import NascarMySqlHook

_SERIES_CURRENT_WEEK_SQL = """
SELECT v.series_id,
       v.season_id,
       v.season_year,
       v.week_id,
       v.week_number,
       v.week_season_number
FROM nascar.v_series_current_week v
WHERE v.series_id = %s
LIMIT 1
"""

_WEEK_HAS_DFS_SALARIES_SQL = """
SELECT 1
FROM nascar.dfs_salaries ds
JOIN nascar.races r ON r.id = ds.race_id AND r.deleted_at IS NULL
WHERE r.week_id = %s
  AND ds.dfs_id = %s
  AND ds.deleted_at IS NULL
LIMIT 1
"""


@dataclass(frozen=True)
class SeriesCurrentWeek:
    series_id: int
    season_id: int
    season_year: int
    week_id: int
    week_number: int
    week_season_number: int | None


@dataclass(frozen=True)
class SeriesCurrentWeekTrackContext:
    series_id: int
    track_id: int
    track_type_id: int | None


_CURRENT_WEEK_TRACK_CONTEXT_SQL = """
SELECT w.track_id, MIN(tt.track_type_id) AS track_type_id
FROM nascar.v_series_current_week v
JOIN nascar.weeks w ON w.id = v.week_id AND w.deleted_at IS NULL
LEFT JOIN nascar.tracks_track_types tt
  ON tt.track_id = w.track_id AND tt.deleted_at IS NULL
WHERE v.series_id = %s
GROUP BY w.track_id
LIMIT 1
"""


def resolve_current_week_track_context(
    hook: NascarMySqlHook,
    *,
    series_id: int,
) -> SeriesCurrentWeekTrackContext | None:
    rows = hook.get_records(_CURRENT_WEEK_TRACK_CONTEXT_SQL, parameters=(series_id,))
    if not rows:
        return None
    track_id, track_type_id = rows[0]
    return SeriesCurrentWeekTrackContext(
        series_id=int(series_id),
        track_id=int(track_id),
        track_type_id=int(track_type_id) if track_type_id is not None else None,
    )


def resolve_series_current_week(
    hook: NascarMySqlHook,
    *,
    series_id: int,
) -> SeriesCurrentWeek | None:
    rows = hook.get_records(_SERIES_CURRENT_WEEK_SQL, parameters=(series_id,))
    if not rows:
        return None
    series_id_v, season_id, season_year, week_id, week_number, week_season_number = rows[0]
    return SeriesCurrentWeek(
        series_id=int(series_id_v),
        season_id=int(season_id),
        season_year=int(season_year),
        week_id=int(week_id),
        week_number=int(week_number),
        week_season_number=int(week_season_number) if week_season_number is not None else None,
    )


def week_has_dfs_salaries(
    hook: NascarMySqlHook,
    *,
    week_id: int,
    dfs_id: int,
) -> bool:
    rows = hook.get_records(_WEEK_HAS_DFS_SALARIES_SQL, parameters=(week_id, dfs_id))
    return bool(rows)


_WEEK_ELIGIBLE_FOR_PREDICT_SQL = """
SELECT 1
FROM nascar.agg_week_results awr
WHERE awr.week_id = %s
  AND awr.deleted_at IS NULL
  AND awr.r_start_position IS NOT NULL
  AND (
      awr.r_finish_position IS NULL
      OR NOT EXISTS (
          SELECT 1
          FROM nascar.ml_predictions mp
          JOIN nascar.ml_training_snapshots s
              ON s.id = mp.ml_training_snapshot_id
             AND s.deleted_at IS NULL
          WHERE s.week_id = awr.week_id
            AND s.kind = 'predict'
            AND mp.deleted_at IS NULL
      )
  )
LIMIT 1
"""

_LATEST_TRAIN_WEEK_FOR_RETRAIN_SQL = """
SELECT MAX(s.week_id)
FROM nascar.ml_training_snapshots s
WHERE s.kind = %s
  AND s.dfs_scoring_rules_id = %s
  AND s.deleted_at IS NULL
  AND s.series_id = %s
  AND (
      s.week_id IN (
          SELECT b.week_id
          FROM nascar.v_series_before_current_week b
          WHERE b.series_id = s.series_id
      )
      OR s.week_id = (
          SELECT c.week_id
          FROM nascar.v_series_current_week c
          WHERE c.series_id = s.series_id
      )
  )
"""


def week_eligible_for_predict_snapshot(
    hook: NascarMySqlHook,
    *,
    week_id: int,
) -> bool:
    """Return True when a ``kind='predict'`` snapshot should be built for ``week_id``.

    Eligible when the grid is known (``r_start_position IS NOT NULL``) AND
    either:

    - The race hasn't finished yet (``r_finish_position IS NULL``) — the
      original pre-race window where the predict pipeline was designed to
      score every hour.
    - The race **has** finished but no live ``ml_predictions`` exist for any
      ``kind='predict'`` snapshot in this week — the self-healing branch
      that lets a missed pre-race cycle catch up after results land.

    Once predictions are written for the current week post-race, this
    function returns False so the hourly schedule does not thrash the
    snapshot builder. The downstream predict / Monte Carlo / lineup tasks
    enforce symmetric "ran exactly once" guards once results are in.
    """
    rows = hook.get_records(_WEEK_ELIGIBLE_FOR_PREDICT_SQL, parameters=(week_id,))
    return bool(rows)


def latest_train_week_for_retrain(
    hook: NascarMySqlHook,
    *,
    rules_id: int,
    series_id: int,
    train_kind: str,
) -> int | None:
    rows = hook.get_records(
        _LATEST_TRAIN_WEEK_FOR_RETRAIN_SQL,
        parameters=(train_kind, rules_id, series_id),
    )
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])
