"""Unit tests for the C1 ``df_*`` driver-recent-form feature family."""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock

for _name in (
    "airflow",
    "airflow.providers",
    "airflow.providers.mysql",
    "airflow.providers.mysql.hooks",
    "airflow.providers.mysql.hooks.mysql",
):
    sys.modules.setdefault(_name, MagicMock())

from include.nascar.ml.driver_form import (  # noqa: E402
    DRIVER_FORM_FEATURE_KEYS,
    _is_dnf,
    compute_driver_form_features,
    fetch_prior_race_rows,
)

_DK_RULES = {
    "version": 1,
    "components": [
        {
            "name": "finish",
            "source": "r_finish_position",
            "type": "finish_table",
            "table": {},
            "formula": {"base": 41, "subtract_position": True, "min": 0},
        },
        {
            "name": "fastest_laps",
            "source": "r_total_fastest_laps",
            "type": "linear",
            "multiplier": 0.45,
        },
        {
            "name": "laps_led",
            "source": "r_total_laps_led",
            "type": "linear",
            "multiplier": 0.25,
        },
    ],
}


def _row(
    *,
    season_id: int,
    week_id: int,
    season_number: int,
    finish: int,
    car_status: str = "RUNNING",
    start: int = 10,
    laps_led: int = 0,
    fastest_laps: int = 0,
) -> dict[str, Any]:
    return {
        "driver_id": 201,
        "season_id": season_id,
        "week_id": week_id,
        "season_number": season_number,
        "r_start_position": start,
        "r_finish_position": finish,
        "r_car_status": car_status,
        "r_total_laps_led": laps_led,
        "r_total_fastest_laps": fastest_laps,
        "r_place_diff": start - finish,
    }


def test_df_features_all_none_when_no_prior_races() -> None:
    features = compute_driver_form_features(
        [],
        current_season_id=2025,
        scoring_rules=_DK_RULES,
    )
    assert set(features.keys()) == set(DRIVER_FORM_FEATURE_KEYS)
    assert all(value is None for value in features.values())


def test_df_last_n_windows_use_most_recent_races() -> None:
    rows = [
        _row(season_id=2024, week_id=1, season_number=1, finish=30),
        _row(season_id=2025, week_id=10, season_number=1, finish=20),
        _row(season_id=2025, week_id=11, season_number=2, finish=15),
        _row(season_id=2025, week_id=12, season_number=3, finish=10),
        _row(season_id=2025, week_id=13, season_number=4, finish=5),
        _row(season_id=2025, week_id=14, season_number=5, finish=1),
    ]
    features = compute_driver_form_features(
        rows, current_season_id=2025, scoring_rules=_DK_RULES
    )
    # last 3 = {10, 5, 1} → 5.333
    assert features["df_last3_avg_finish"] == round((10 + 5 + 1) / 3, 3)
    # last 5 = {15, 10, 5, 1, 20} → 10.2
    assert features["df_last5_avg_finish"] == round((15 + 10 + 5 + 1 + 20) / 5, 3)
    # last 10 takes all 6 rows: (30+20+15+10+5+1)/6 = 13.5
    assert features["df_last10_avg_finish"] == round(
        (30 + 20 + 15 + 10 + 5 + 1) / 6, 3
    )


def test_df_top_n_and_streak_use_most_recent_window() -> None:
    rows = [
        _row(season_id=2025, week_id=5, season_number=1, finish=20),
        _row(season_id=2025, week_id=6, season_number=2, finish=8),
        _row(season_id=2025, week_id=7, season_number=3, finish=12),
        _row(season_id=2025, week_id=8, season_number=4, finish=4),
        _row(season_id=2025, week_id=9, season_number=5, finish=2),
    ]
    features = compute_driver_form_features(
        rows, current_season_id=2025, scoring_rules=_DK_RULES
    )
    assert features["df_last5_top5_rate"] == round(2 / 5, 3)
    assert features["df_last5_top10_rate"] == round(3 / 5, 3)
    # streak counts back from most recent: 2, 4, 12 (breaks) → 2 races top10
    assert features["df_streak_top10"] == 2.0


def test_df_dnf_rate_and_streak_use_car_status() -> None:
    rows = [
        _row(season_id=2025, week_id=1, season_number=1, finish=15, car_status="RUNNING"),
        _row(
            season_id=2025,
            week_id=2,
            season_number=2,
            finish=37,
            car_status="ACCIDENT",
        ),
        _row(
            season_id=2025,
            week_id=3,
            season_number=3,
            finish=38,
            car_status="ENGINE",
        ),
        _row(
            season_id=2025,
            week_id=4,
            season_number=4,
            finish=39,
            car_status="ELECTRICAL",
        ),
    ]
    features = compute_driver_form_features(
        rows, current_season_id=2025, scoring_rules=_DK_RULES
    )
    assert features["df_last5_dnf_rate"] == round(3 / 4, 3)
    assert features["df_streak_dnf"] == 3.0


def test_df_ytd_features_filter_by_current_season() -> None:
    rows = [
        _row(season_id=2024, week_id=1, season_number=10, finish=5),
        _row(season_id=2024, week_id=2, season_number=11, finish=3),
        _row(season_id=2025, week_id=3, season_number=1, finish=20),
        _row(season_id=2025, week_id=4, season_number=2, finish=18),
    ]
    features = compute_driver_form_features(
        rows, current_season_id=2025, scoring_rules=_DK_RULES
    )
    assert features["df_ytd_avg_finish"] == round((20 + 18) / 2, 3)
    assert features["df_ytd_top5_rate"] == 0.0


def test_df_dfs_points_aggregates_use_active_rules() -> None:
    rows = [
        _row(
            season_id=2025,
            week_id=1,
            season_number=1,
            finish=1,
            laps_led=100,
            fastest_laps=10,
        ),
        _row(
            season_id=2025,
            week_id=2,
            season_number=2,
            finish=2,
            laps_led=50,
            fastest_laps=5,
        ),
    ]
    features = compute_driver_form_features(
        rows, current_season_id=2025, scoring_rules=_DK_RULES
    )
    # finish=1 -> base 41 - 1 = 40, laps_led 100*0.25=25, fastest 10*0.45=4.5 → 69.5
    # finish=2 -> 39 + 50*0.25=12.5 + 5*0.45=2.25 → 53.75
    assert features["df_last5_avg_dfs_points"] == round((69.5 + 53.75) / 2, 3)
    assert features["df_ytd_avg_dfs_points"] == round((69.5 + 53.75) / 2, 3)


def test_is_dnf_treats_missing_status_as_not_dnf() -> None:
    assert _is_dnf({"r_car_status": None}) is False
    assert _is_dnf({"r_car_status": "RUNNING"}) is False
    assert _is_dnf({"r_car_status": "running"}) is False  # case-insensitive
    assert _is_dnf({"r_car_status": "ACCIDENT"}) is True
    assert _is_dnf({"r_car_status": "ENGINE"}) is True


def test_fetch_prior_race_rows_filters_by_week_id_and_series() -> None:
    """The SQL must enforce ``week_id < current`` and ``series_id`` match (no leakage)."""
    hook = MagicMock()
    hook.get_records.return_value = []
    fetch_prior_race_rows(
        hook,
        series_id=1,
        week_id=100,
        driver_ids=[201, 202],
        scoring_rules=_DK_RULES,
    )
    args, kwargs = hook.get_records.call_args
    sql = args[0]
    assert "awr.week_id < %s" in sql
    assert "awr.series_id = %s" in sql
    assert "awr.r_finish_position IS NOT NULL" in sql
    assert "awr.deleted_at IS NULL" in sql
    params = kwargs["parameters"]
    assert params[:2] == (1, 100)
    assert set(params[2:]) == {201, 202}


def test_fetch_prior_race_rows_handles_empty_driver_list() -> None:
    """Empty driver list short-circuits without issuing a query."""
    hook = MagicMock()
    result = fetch_prior_race_rows(
        hook,
        series_id=1,
        week_id=100,
        driver_ids=[],
        scoring_rules=_DK_RULES,
    )
    assert result == {}
    hook.get_records.assert_not_called()


def test_fetch_prior_race_rows_keys_by_driver_id() -> None:
    """Returned dict groups rows by ``driver_id`` in order."""
    hook = MagicMock()
    hook.get_records.return_value = [
        (201, 2025, 1, 5, 10, "RUNNING", 5, 1, 5, 1),
        (201, 2025, 2, 8, 15, "RUNNING", 3, 0, 7, 2),
        (202, 2025, 1, 30, 38, "ACCIDENT", -8, 0, 0, 1),
    ]
    result = fetch_prior_race_rows(
        hook,
        series_id=1,
        week_id=100,
        driver_ids=[201, 202],
        scoring_rules=_DK_RULES,
    )
    assert set(result.keys()) == {201, 202}
    assert len(result[201]) == 2
    assert len(result[202]) == 1
    assert result[201][0]["r_finish_position"] == 10
    assert result[202][0]["r_car_status"] == "ACCIDENT"
