"""Configuration for joining agg_week_prev_* tables into ML training snapshots."""

from __future__ import annotations

from dataclasses import dataclass

DRAFTKINGS_DFS_ID = 1
FANDUEL_DFS_ID = 37
GRAIN_CAR_DRIVER = "car_driver"

SNAPSHOT_KIND_TRAIN = "train"
SNAPSHOT_KIND_PREDICT = "predict"

AUDIT_COLUMNS = frozenset(
    {"id", "created_at", "updated_at", "deleted_at"},
)

GRAIN_COLUMNS = frozenset(
    {
        "series_id",
        "season_id",
        "week_id",
        "track_id",
        "track_type_id",
        "car_driver_id",
        "driver_id",
        "car_id",
        "team_id",
        "manufacturer_id",
        "car_generation_id",
        "car_generation_model_id",
    },
)

TIME_COLUMN_SUFFIXES = ("_time",)

# Same-week pre-race columns from ``agg_week_results`` (not lagged prev-track history).
ANCHOR_PRELOCK_EXPLICIT_COLUMNS: tuple[str, ...] = ("r_start_position",)
ANCHOR_PRELOCK_PREFIXES: tuple[str, ...] = ("q_", "pt_", "pf_")

DRIVER_FORM_PREFIX = "df_"
FIELD_STRENGTH_PREFIX = "qf_"
SEASON_PREFIX = "season_"
TRACK_ATTRIBUTE_PREFIX = "track_attr_"

ANCHOR_LEVEL_FEATURE_PREFIXES: tuple[str, ...] = (
    DRIVER_FORM_PREFIX,
    FIELD_STRENGTH_PREFIX,
    SEASON_PREFIX,
    TRACK_ATTRIBUTE_PREFIX,
)


@dataclass(frozen=True)
class PrevTrackSource:
    table_name: str
    prefix: str
    join_column: str


@dataclass(frozen=True)
class PrevTrackTypesSource:
    table_name: str
    prefix: str
    join_column: str


PREV_TRACK_SOURCES: tuple[PrevTrackSource, ...] = (
    PrevTrackSource("agg_week_prev_track_cars_drivers_results", "cd_pt", "car_driver_id"),
    PrevTrackSource("agg_week_prev_track_drivers_results", "drv_pt", "driver_id"),
    PrevTrackSource("agg_week_prev_track_cars_results", "car_pt", "car_id"),
    PrevTrackSource("agg_week_prev_track_teams_results", "team_pt", "team_id"),
    PrevTrackSource("agg_week_prev_track_manufacturers_results", "mfr_pt", "manufacturer_id"),
    PrevTrackSource(
        "agg_week_prev_track_car_generations_results",
        "cg_pt",
        "car_generation_id",
    ),
    PrevTrackSource(
        "agg_week_prev_track_car_generation_models_results",
        "cgm_pt",
        "car_generation_model_id",
    ),
    PrevTrackSource("agg_week_prev_track_cg_cars_drivers_results", "cg_cd_pt", "car_driver_id"),
    PrevTrackSource("agg_week_prev_track_cgm_cars_drivers_results", "cgm_cd_pt", "car_driver_id"),
    PrevTrackSource("agg_week_prev_track_cg_drivers_results", "cg_drv_pt", "driver_id"),
    PrevTrackSource("agg_week_prev_track_cgm_drivers_results", "cgm_drv_pt", "driver_id"),
    PrevTrackSource("agg_week_prev_track_cg_cars_results", "cg_car_pt", "car_id"),
    PrevTrackSource("agg_week_prev_track_cgm_cars_results", "cgm_car_pt", "car_id"),
    PrevTrackSource("agg_week_prev_track_cg_teams_results", "cg_team_pt", "team_id"),
    PrevTrackSource("agg_week_prev_track_cgm_teams_results", "cgm_team_pt", "team_id"),
    PrevTrackSource("agg_week_prev_track_cg_manufacturers_results", "cg_mfr_pt", "manufacturer_id"),
    PrevTrackSource(
        "agg_week_prev_track_cgm_manufacturers_results", "cgm_mfr_pt", "manufacturer_id"
    ),
)

PREV_TRACK_TYPES_SOURCES: tuple[PrevTrackTypesSource, ...] = (
    PrevTrackTypesSource(
        "agg_week_prev_track_types_cars_drivers_results",
        "cd",
        "car_driver_id",
    ),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_drivers_results",
        "drv",
        "driver_id",
    ),
    PrevTrackTypesSource("agg_week_prev_track_types_cars_results", "car", "car_id"),
    PrevTrackTypesSource("agg_week_prev_track_types_teams_results", "team", "team_id"),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_manufacturers_results",
        "mfr",
        "manufacturer_id",
    ),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_car_generations_results",
        "cg",
        "car_generation_id",
    ),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_car_generation_models_results",
        "cgm",
        "car_generation_model_id",
    ),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_cg_cars_drivers_results",
        "cg_cd",
        "car_driver_id",
    ),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_cgm_cars_drivers_results",
        "cgm_cd",
        "car_driver_id",
    ),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_cg_drivers_results",
        "cg_drv",
        "driver_id",
    ),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_cgm_drivers_results",
        "cgm_drv",
        "driver_id",
    ),
    PrevTrackTypesSource("agg_week_prev_track_types_cg_cars_results", "cg_car", "car_id"),
    PrevTrackTypesSource("agg_week_prev_track_types_cgm_cars_results", "cgm_car", "car_id"),
    PrevTrackTypesSource("agg_week_prev_track_types_cg_teams_results", "cg_team", "team_id"),
    PrevTrackTypesSource("agg_week_prev_track_types_cgm_teams_results", "cgm_team", "team_id"),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_cg_manufacturers_results",
        "cg_mfr",
        "manufacturer_id",
    ),
    PrevTrackTypesSource(
        "agg_week_prev_track_types_cgm_manufacturers_results",
        "cgm_mfr",
        "manufacturer_id",
    ),
)
