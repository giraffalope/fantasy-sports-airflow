"""Path helpers for DFS provider files on the Airflow volume."""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_DFS_ROOT = "/home/airflow/dfs"
CUP_DB_NAME = "cup"


def dfs_root() -> Path:
    return Path(os.environ.get("AIRFLOW_DFS_ROOT", DEFAULT_DFS_ROOT))


def draftkings_salaries_dir(season_year: int, week_number: int) -> Path:
    return dfs_root() / "draftkings" / "nascar" / CUP_DB_NAME / str(season_year) / str(week_number)


def draftkings_salaries_csv(season_year: int, week_number: int) -> Path:
    return draftkings_salaries_dir(season_year, week_number) / "DKSalaries.csv"


def draftkings_salaries_glob() -> Path:
    return dfs_root() / "draftkings" / "nascar" / CUP_DB_NAME
