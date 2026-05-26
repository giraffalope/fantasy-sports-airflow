"""Driver recent-form (``df_*``) feature family for ML snapshots (C1).

Computed per ``driver_id`` strictly from prior races (``week_id <
current_week_id``) and joined into the per-row ``features_json`` at snapshot
build time. Designed so:

- Last-N windows (``df_last3``, ``df_last5``, ``df_last10``) use the most
  recent races regardless of season — i.e. they cross the season boundary
  cleanly.
- YTD windows (``df_ytd_*``) restrict to the snapshot's own ``season_id`` so
  a "year-to-date" interpretation actually holds.
- Streak windows (``df_streak_*``) walk back from the most recent prior race
  until the condition breaks.
- DNF is derived from ``r_car_status``: any non-``RUNNING`` value is treated
  as a DNF. The schema does not carry a clean ``r_status`` column, so the
  car-status proxy is the cleanest available signal.
- Drivers with no prior races get ``None`` for every ``df_*`` key. The
  downstream feature matrix builder median-fills those entries (see
  :mod:`include.nascar.ml.feature_matrix`).

There is no schema migration in C1; the new keys flow through
``ml_training_snapshots.feature_keys`` as JSON.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.ml.scoring import (
    compute_dfs_points,
    target_keys_for_rules,
)

_LOG = logging.getLogger(__name__)

DRIVER_FORM_FEATURE_KEYS: tuple[str, ...] = (
    "df_last3_avg_finish",
    "df_last5_avg_finish",
    "df_last10_avg_finish",
    "df_last5_top5_rate",
    "df_last5_top10_rate",
    "df_last5_dnf_rate",
    "df_last5_avg_dfs_points",
    "df_last5_avg_laps_led",
    "df_last5_avg_fastest_laps",
    "df_ytd_avg_finish",
    "df_ytd_top5_rate",
    "df_ytd_avg_dfs_points",
    "df_streak_top10",
    "df_streak_dnf",
)

_RUNNING_STATUS = "RUNNING"

_BASE_PRIOR_COLUMNS: tuple[str, ...] = (
    "driver_id",
    "season_id",
    "week_id",
    "r_start_position",
    "r_finish_position",
    "r_car_status",
    "r_total_laps_led",
    "r_total_fastest_laps",
    "r_place_diff",
)


def _prior_race_columns(scoring_rules: Mapping[str, Any] | str) -> list[str]:
    """Columns to select from ``agg_week_results`` for a prior-race row.

    Always includes the base set used for ``df_*`` aggregates and DNF detection,
    plus any target-key columns required to evaluate ``compute_dfs_points``
    under the active scoring rules.
    """
    keys = list(_BASE_PRIOR_COLUMNS)
    seen = set(keys)
    for key in target_keys_for_rules(scoring_rules):
        if key not in seen:
            keys.append(key)
            seen.add(key)
    return keys


def fetch_prior_race_rows(
    hook: NascarMySqlHook,
    *,
    series_id: int,
    week_id: int,
    driver_ids: Iterable[int],
    scoring_rules: Mapping[str, Any] | str,
) -> dict[int, list[dict[str, Any]]]:
    """Return prior finished races per ``driver_id``, oldest-first.

    Strict ``week_id < current_week_id`` bound enforces no leakage. Sorted by
    chronological race order (``season_id`` ASC, ``weeks.season_number``
    ASC). Drivers absent from the result map have no prior races.
    """
    driver_id_list = [int(d) for d in driver_ids if d is not None]
    if not driver_id_list:
        return {}

    column_list = _prior_race_columns(scoring_rules)
    select_list = ", ".join(f"awr.`{column}`" for column in column_list)
    placeholders = ", ".join(["%s"] * len(driver_id_list))
    sql = f"""
        SELECT {select_list}, w.season_number
        FROM nascar.agg_week_results awr
        JOIN nascar.weeks w ON w.id = awr.week_id
        WHERE awr.series_id = %s
          AND awr.week_id < %s
          AND awr.deleted_at IS NULL
          AND awr.r_finish_position IS NOT NULL
          AND awr.driver_id IN ({placeholders})
        ORDER BY awr.driver_id, w.season_id ASC, w.season_number ASC, awr.week_id ASC
    """
    rows = hook.get_records(
        sql,
        parameters=(int(series_id), int(week_id), *driver_id_list),
    )
    column_names = [*column_list, "season_number"]
    keyed: dict[int, list[dict[str, Any]]] = {}
    for raw in rows:
        record = dict(zip(column_names, raw, strict=True))
        driver_id = record.get("driver_id")
        if driver_id is None:
            continue
        keyed.setdefault(int(driver_id), []).append(record)
    return keyed


def _is_dnf(row: Mapping[str, Any]) -> bool:
    """True when the prior-race row reads as a DNF.

    Heuristic: ``r_car_status`` other than ``RUNNING`` (case-insensitive) is
    treated as a DNF. Rows with ``r_car_status IS NULL`` are conservatively
    treated as not-DNF so a missing status doesn't inflate DNF rates.
    """
    status = row.get("r_car_status")
    if status is None:
        return False
    return str(status).strip().upper() != _RUNNING_STATUS


def _safe_avg(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 3)


def _finish_positions(rows: list[Mapping[str, Any]]) -> list[float]:
    result: list[float] = []
    for row in rows:
        finish = row.get("r_finish_position")
        if finish is None:
            continue
        result.append(float(finish))
    return result


def _numeric_values(
    rows: list[Mapping[str, Any]],
    column: str,
) -> list[float]:
    out: list[float] = []
    for row in rows:
        value = row.get(column)
        if value is None:
            continue
        try:
            out.append(float(value))
        except (TypeError, ValueError):
            continue
    return out


def _rate_top_n(rows: list[Mapping[str, Any]], n: int) -> float | None:
    finishes = _finish_positions(rows)
    if not finishes:
        return None
    return round(sum(1 for f in finishes if f <= n) / len(finishes), 3)


def _dnf_rate(rows: list[Mapping[str, Any]]) -> float | None:
    if not rows:
        return None
    return round(sum(1 for row in rows if _is_dnf(row)) / len(rows), 3)


def _streak_top_n(rows: list[Mapping[str, Any]], n: int) -> int:
    """Length of the trailing streak (most-recent-first) of top-N finishes."""
    streak = 0
    for row in reversed(rows):
        finish = row.get("r_finish_position")
        if finish is None or float(finish) > n:
            break
        streak += 1
    return streak


def _streak_dnf(rows: list[Mapping[str, Any]]) -> int:
    streak = 0
    for row in reversed(rows):
        if not _is_dnf(row):
            break
        streak += 1
    return streak


def _dfs_points_for_rows(
    rows: list[Mapping[str, Any]],
    scoring_rules: Mapping[str, Any] | str,
) -> list[float]:
    points: list[float] = []
    for row in rows:
        if row.get("r_finish_position") is None:
            continue
        try:
            total, _breakdown = compute_dfs_points(row, scoring_rules)
        except Exception:  # noqa: BLE001 - prior-race scoring should never abort a build
            _LOG.warning(
                "driver_form: failed to score prior race for driver_id=%s week_id=%s",
                row.get("driver_id"),
                row.get("week_id"),
            )
            continue
        points.append(float(total))
    return points


def compute_driver_form_features(
    prior_races: list[Mapping[str, Any]],
    *,
    current_season_id: int,
    scoring_rules: Mapping[str, Any] | str,
) -> dict[str, float | None]:
    """Compute every ``df_*`` key for one driver.

    ``prior_races`` must already be ordered oldest-first by
    ``fetch_prior_race_rows``. ``current_season_id`` is used to scope the YTD
    aggregates.
    """
    features: dict[str, float | None] = {key: None for key in DRIVER_FORM_FEATURE_KEYS}
    if not prior_races:
        return features

    last3 = prior_races[-3:]
    last5 = prior_races[-5:]
    last10 = prior_races[-10:]
    ytd = [row for row in prior_races if row.get("season_id") == current_season_id]

    features["df_last3_avg_finish"] = _safe_avg(_finish_positions(last3))
    features["df_last5_avg_finish"] = _safe_avg(_finish_positions(last5))
    features["df_last10_avg_finish"] = _safe_avg(_finish_positions(last10))

    features["df_last5_top5_rate"] = _rate_top_n(last5, 5)
    features["df_last5_top10_rate"] = _rate_top_n(last5, 10)
    features["df_last5_dnf_rate"] = _dnf_rate(last5)

    features["df_last5_avg_dfs_points"] = _safe_avg(
        _dfs_points_for_rows(last5, scoring_rules)
    )
    features["df_last5_avg_laps_led"] = _safe_avg(
        _numeric_values(last5, "r_total_laps_led")
    )
    features["df_last5_avg_fastest_laps"] = _safe_avg(
        _numeric_values(last5, "r_total_fastest_laps")
    )

    features["df_ytd_avg_finish"] = _safe_avg(_finish_positions(ytd))
    features["df_ytd_top5_rate"] = _rate_top_n(ytd, 5)
    features["df_ytd_avg_dfs_points"] = _safe_avg(
        _dfs_points_for_rows(ytd, scoring_rules)
    )

    features["df_streak_top10"] = float(_streak_top_n(prior_races, 10))
    features["df_streak_dnf"] = float(_streak_dnf(prior_races))

    return features
