"""Unit tests for the C4 ``season_*`` and ``track_attr_*`` feature families."""

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

from include.nascar.ml import season_features  # noqa: E402
from include.nascar.ml.season_features import (  # noqa: E402
    SEASON_FEATURE_KEYS,
    TRACK_ATTRIBUTE_FEATURE_KEYS,
    fetch_season_context,
    fetch_track_attributes,
)


def _hook_returning(records_by_pattern: dict[str, list[tuple]]) -> MagicMock:
    hook = MagicMock()

    def get_records(sql: str, parameters: Any = ()) -> list[tuple]:
        text = sql.lower()
        for needle, rows in records_by_pattern.items():
            if needle in text:
                return rows
        return []

    hook.get_records.side_effect = get_records
    return hook


def test_fetch_season_context_returns_all_keys_with_none_when_no_row() -> None:
    hook = _hook_returning({})
    features = fetch_season_context(hook, week_id=999)
    assert set(features.keys()) == set(SEASON_FEATURE_KEYS)
    assert all(value is None for value in features.values())


def test_fetch_season_context_mid_season() -> None:
    """Week 10 of 36 → mid-season, neither playoffs nor chase."""
    hook = _hook_returning({"from nascar.weeks": [(10, 36, 2025)]})
    features = fetch_season_context(hook, week_id=42)
    assert features["season_week_number"] == 10.0
    assert features["season_year"] == 2025.0
    assert features["season_progress_pct"] == round(10 / 36, 4)
    assert features["season_is_playoffs"] == 0.0
    assert features["season_is_chase_round"] == 0.0


def test_fetch_season_context_playoff_round() -> None:
    """Week 28 of 36 → 8 races remaining → playoffs but not yet championship round."""
    hook = _hook_returning({"from nascar.weeks": [(28, 36, 2025)]})
    features = fetch_season_context(hook, week_id=42)
    assert features["season_is_playoffs"] == 1.0
    assert features["season_is_chase_round"] == 0.0


def test_fetch_season_context_championship_round() -> None:
    """Final 4 weeks fire both playoffs and chase flags."""
    hook = _hook_returning({"from nascar.weeks": [(33, 36, 2025)]})
    features = fetch_season_context(hook, week_id=42)
    assert features["season_is_playoffs"] == 1.0
    assert features["season_is_chase_round"] == 1.0


def test_fetch_season_context_handles_zero_total_races() -> None:
    """Defensive: total=0 must not divide by zero or flag playoffs."""
    hook = _hook_returning({"from nascar.weeks": [(1, 0, 2025)]})
    features = fetch_season_context(hook, week_id=42)
    assert features["season_week_number"] == 1.0
    assert features["season_progress_pct"] is None
    assert features["season_is_playoffs"] is None
    assert features["season_is_chase_round"] is None


def test_fetch_track_attributes_emits_all_keys() -> None:
    season_features.clear_tracks_columns_cache()
    hook = _hook_returning(
        {
            "information_schema": [("id",), ("name",), ("length",)],
            "from nascar.tracks": [(1.54,)],
        }
    )
    features = fetch_track_attributes(hook, track_id=7)
    assert set(features.keys()) == set(TRACK_ATTRIBUTE_FEATURE_KEYS)
    assert features["track_attr_length_miles"] == 1.54


def test_fetch_track_attributes_none_when_length_column_missing() -> None:
    """Opportunistic gating: missing ``length`` column → None, no SELECT fired."""
    season_features.clear_tracks_columns_cache()
    hook = _hook_returning(
        {"information_schema": [("id",), ("name",)]}
    )
    features = fetch_track_attributes(hook, track_id=7)
    assert features["track_attr_length_miles"] is None


def test_fetch_track_attributes_none_when_row_missing() -> None:
    season_features.clear_tracks_columns_cache()
    hook = _hook_returning(
        {
            "information_schema": [("id",), ("length",)],
            "from nascar.tracks": [],
        }
    )
    features = fetch_track_attributes(hook, track_id=7)
    assert features["track_attr_length_miles"] is None


def test_tracks_columns_cached_across_calls() -> None:
    season_features.clear_tracks_columns_cache()
    hook = _hook_returning(
        {
            "information_schema": [("id",), ("length",)],
            "from nascar.tracks": [(2.5,)],
        }
    )
    fetch_track_attributes(hook, track_id=1)
    fetch_track_attributes(hook, track_id=2)
    info_calls = [
        call
        for call in hook.get_records.call_args_list
        if "information_schema" in call.args[0].lower()
    ]
    assert len(info_calls) == 1
