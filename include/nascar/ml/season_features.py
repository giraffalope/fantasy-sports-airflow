"""Season / time-of-year (``season_*``) and opportunistic track-attribute
(``track_attr_*``) feature families for ML snapshots (C4).

Season features come from ``nascar.weeks`` + ``nascar.seasons``:

- ``season_week_number`` is the 1-indexed race number within the season,
  ordered by ``scheduled_start`` then ``id`` so it is deterministic even if
  ``scheduled_start`` is identical across weeks.
- ``season_total_races`` is the count of weeks in the season (already
  scheduled). It is exposed indirectly through ``season_progress_pct``
  rather than as its own column; raw counts vary with schedule changes and
  add noise.
- ``season_progress_pct = season_week_number / season_total_races``.
- ``season_year`` exposes the calendar year, intended as a low-cardinality
  drift signal across seasons.
- ``season_is_playoffs`` / ``season_is_chase_round`` are heuristic 0/1
  flags. The current ``weeks.event_type`` ENUM only has
  ``REGULAR``/``OTHER`` so we cannot read these directly. The historical
  Cup format reserves the final 10 races for the playoffs and the final 4
  for the championship round, so we encode that until a richer schedule
  column appears. If/when ``weeks.event_type`` widens, refactor here only.

Track attributes are read from ``nascar.tracks`` and gated on column
presence so adding columns upstream (banking, surface, weather) wires in
automatically without further changes here. Today only ``length`` exists →
``track_attr_length_miles``.
"""

from __future__ import annotations

import logging
from typing import Any

from airflow_utils.nascar_mysql_hook import NascarMySqlHook

_LOG = logging.getLogger(__name__)

SEASON_FEATURE_KEYS: tuple[str, ...] = (
    "season_week_number",
    "season_progress_pct",
    "season_year",
    "season_is_playoffs",
    "season_is_chase_round",
)

TRACK_ATTRIBUTE_FEATURE_KEYS: tuple[str, ...] = ("track_attr_length_miles",)

_PLAYOFF_WINDOW = 10
_CHAMPIONSHIP_WINDOW = 4

_TRACKS_COLUMNS_CACHE: list[str] | None = None


def _tracks_columns(hook: NascarMySqlHook) -> list[str]:
    """Cached ``information_schema`` lookup for ``nascar.tracks``."""
    global _TRACKS_COLUMNS_CACHE
    if _TRACKS_COLUMNS_CACHE is not None:
        return _TRACKS_COLUMNS_CACHE
    rows = hook.get_records(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = 'nascar' AND TABLE_NAME = 'tracks'
        ORDER BY ORDINAL_POSITION
        """,
    )
    columns = [str(name) for (name,) in rows]
    _TRACKS_COLUMNS_CACHE = columns
    return columns


def clear_tracks_columns_cache() -> None:
    """Reset the ``information_schema`` cache (test helper)."""
    global _TRACKS_COLUMNS_CACHE
    _TRACKS_COLUMNS_CACHE = None


def fetch_season_context(
    hook: NascarMySqlHook,
    *,
    week_id: int,
) -> dict[str, float | None]:
    """Return per-snapshot ``season_*`` features for the anchor week.

    All keys are always present (``None`` when unavailable) so the snapshot's
    ``feature_keys`` set stays stable across builds.
    """
    base: dict[str, float | None] = dict.fromkeys(SEASON_FEATURE_KEYS)

    rows = hook.get_records(
        """
        WITH ordered AS (
            SELECT
                w.id,
                w.season_id,
                ROW_NUMBER() OVER (
                    PARTITION BY w.season_id
                    ORDER BY w.scheduled_start ASC, w.id ASC
                ) AS season_week_number,
                COUNT(*) OVER (PARTITION BY w.season_id) AS season_total_races
            FROM nascar.weeks w
            WHERE w.deleted_at IS NULL
        )
        SELECT
            ordered.season_week_number,
            ordered.season_total_races,
            s.year
        FROM ordered
        JOIN nascar.seasons s ON s.id = ordered.season_id
        WHERE ordered.id = %s
        """,
        parameters=(int(week_id),),
    )
    if not rows:
        return base

    week_number, total_races, year = rows[0]
    if week_number is not None:
        base["season_week_number"] = float(int(week_number))
    if year is not None:
        base["season_year"] = float(int(year))

    if (
        week_number is not None
        and total_races is not None
        and int(total_races) > 0
    ):
        base["season_progress_pct"] = round(
            float(int(week_number)) / float(int(total_races)), 4
        )
        weeks_remaining = int(total_races) - int(week_number)
        base["season_is_playoffs"] = (
            1.0 if weeks_remaining < _PLAYOFF_WINDOW else 0.0
        )
        base["season_is_chase_round"] = (
            1.0 if weeks_remaining < _CHAMPIONSHIP_WINDOW else 0.0
        )

    return base


def fetch_track_attributes(
    hook: NascarMySqlHook,
    *,
    track_id: int,
) -> dict[str, float | None]:
    """Return opportunistic ``track_attr_*`` features for the anchor track.

    Always returns every documented key; gating happens by setting the value
    to ``None`` when the underlying column is missing or NULL. New columns
    on ``nascar.tracks`` (e.g. banking, surface) light up automatically once
    listed in :data:`TRACK_ATTRIBUTE_FEATURE_KEYS` and surfaced here.
    """
    base: dict[str, float | None] = dict.fromkeys(TRACK_ATTRIBUTE_FEATURE_KEYS)

    columns = _tracks_columns(hook)
    if "length" not in columns:
        _LOG.info(
            "season_features: nascar.tracks has no 'length' column; skipping track_attr_length_miles"
        )
        return base

    rows = hook.get_records(
        "SELECT `length` FROM nascar.tracks WHERE id = %s AND deleted_at IS NULL",
        parameters=(int(track_id),),
    )
    if not rows:
        return base

    (length_value,) = rows[0]
    if length_value is not None:
        try:
            base["track_attr_length_miles"] = round(float(length_value), 4)
        except (TypeError, ValueError):
            base["track_attr_length_miles"] = None

    return base
