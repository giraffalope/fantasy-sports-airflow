"""Import DraftKings DKSalaries.csv files into ``dfs_salaries``."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.dfs.paths import draftkings_salaries_dir, draftkings_salaries_glob
from include.nascar.enums import NascarSeries
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID

_LOG = logging.getLogger(__name__)

_CURRENT_WEEK_SQL = """
SELECT w.id AS week_id,
       w.number AS week_number,
       s.year AS season_year,
       r.id AS race_id
FROM nascar.race_results rr
JOIN nascar.races r ON rr.race_id = r.id AND r.deleted_at IS NULL
JOIN nascar.weeks w ON r.week_id = w.id AND w.deleted_at IS NULL
JOIN nascar.seasons s ON w.season_id = s.id AND s.deleted_at IS NULL
WHERE rr.deleted_at IS NULL
  AND s.series_id = %s
ORDER BY w.id DESC
LIMIT 1
"""

_WEEK_RACE_SQL = """
SELECT w.id AS week_id, r.id AS race_id
FROM nascar.weeks w
JOIN nascar.seasons s ON w.season_id = s.id AND s.deleted_at IS NULL
JOIN nascar.races r ON r.week_id = w.id AND r.deleted_at IS NULL
WHERE s.series_id = %s
  AND s.year = %s
  AND w.number = %s
  AND w.deleted_at IS NULL
LIMIT 1
"""

_CAR_DRIVER_BY_DFS_ID_SQL = """
SELECT car_driver_id
FROM nascar.dfs_salaries
WHERE dfs_id = %s
  AND race_id = %s
  AND dfs_driver_id = %s
  AND deleted_at IS NULL
LIMIT 1
"""

_RACE_DRIVER_NAMES_SQL = """
SELECT cd.id AS car_driver_id,
       d.first_name,
       d.last_name,
       d.suffix
FROM nascar.race_results rr
JOIN nascar.cars_drivers cd ON rr.car_driver_id = cd.id AND cd.deleted_at IS NULL
JOIN nascar.drivers d ON cd.driver_id = d.id AND d.deleted_at IS NULL
WHERE rr.race_id = %s
  AND rr.deleted_at IS NULL
"""


@dataclass(frozen=True)
class CupWeekContext:
    week_id: int
    week_number: int
    season_year: int
    race_id: int


def _normalize_driver_name(raw: str) -> str:
    name = re.sub(r"\s+", " ", raw.strip().lower())
    name = re.sub(r"[.,']", "", name)
    for suffix in (" jr", " sr", " iii", " ii", " iv"):
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    return name


def _driver_display_name(first: str | None, last: str | None, suffix: str | None) -> str:
    parts = [p for p in (first, last, suffix) if p]
    return _normalize_driver_name(" ".join(parts))


def resolve_current_cup_week(
    hook: NascarMySqlHook,
    *,
    series_id: int = NascarSeries.CUP.series_id,
) -> CupWeekContext | None:
    rows = hook.get_records(_CURRENT_WEEK_SQL, parameters=(series_id,))
    if not rows:
        return None
    week_id, week_number, season_year, race_id = rows[0]
    return CupWeekContext(
        week_id=int(week_id),
        week_number=int(week_number),
        season_year=int(season_year),
        race_id=int(race_id),
    )


def resolve_week_race(
    hook: NascarMySqlHook,
    *,
    season_year: int,
    week_number: int,
    series_id: int = NascarSeries.CUP.series_id,
) -> tuple[int, int] | None:
    rows = hook.get_records(
        _WEEK_RACE_SQL,
        parameters=(series_id, season_year, week_number),
    )
    if not rows:
        return None
    week_id, race_id = rows[0]
    return int(week_id), int(race_id)


def ensure_draftkings_week_directory_task(**_context: object) -> dict[str, Any]:
    """Create the DFS folder for the current Cup week (no CSV file)."""
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    ctx = resolve_current_cup_week(hook)
    if ctx is None:
        _LOG.info("ensure_draftkings_week_directory: no current Cup week with race_results")
        return {"skipped": True, "reason": "no_current_week"}

    directory = draftkings_salaries_dir(ctx.season_year, ctx.week_number)
    directory.mkdir(parents=True, exist_ok=True)
    _LOG.info(
        "ensure_draftkings_week_directory: series week=%s year=%s path=%s",
        ctx.week_number,
        ctx.season_year,
        directory,
    )
    return {
        "season_year": ctx.season_year,
        "week_number": ctx.week_number,
        "week_id": ctx.week_id,
        "race_id": ctx.race_id,
        "directory": str(directory),
    }


def _read_dk_salaries_csv(path: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    columns = {str(c).strip().lower(): c for c in frame.columns}
    required = ("id", "name", "salary")
    missing = [c for c in required if c not in columns]
    if missing:
        raise ValueError(f"DKSalaries.csv missing columns {missing}: {path}")

    out = pd.DataFrame(
        {
            "dfs_driver_id": frame[columns["id"]],
            "driver_name": frame[columns["name"]].astype(str),
            "salary": frame[columns["salary"]],
        }
    )
    out["dfs_driver_id"] = pd.to_numeric(out["dfs_driver_id"], errors="coerce")
    out["salary"] = (
        out["salary"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
    )
    out["salary"] = pd.to_numeric(out["salary"], errors="coerce")
    out = out.dropna(subset=["dfs_driver_id", "salary"])
    out["dfs_driver_id"] = out["dfs_driver_id"].astype(int)
    out["salary"] = out["salary"].astype(int)
    return out


def _match_car_driver_id(
    hook: NascarMySqlHook,
    *,
    race_id: int,
    dfs_driver_id: int,
    driver_name: str,
) -> int | None:
    rows = hook.get_records(
        _CAR_DRIVER_BY_DFS_ID_SQL,
        parameters=(DRAFTKINGS_DFS_ID, race_id, dfs_driver_id),
    )
    if rows:
        return int(rows[0][0])

    target = _normalize_driver_name(driver_name)
    candidates = hook.get_records(_RACE_DRIVER_NAMES_SQL, parameters=(race_id,))
    for car_driver_id, first, last, suffix in candidates:
        if _driver_display_name(first, last, suffix) == target:
            return int(car_driver_id)
    return None


def _import_csv_file(
    hook: NascarMySqlHook,
    path: str,
    *,
    season_year: int,
    week_number: int,
) -> dict[str, Any]:
    week_race = resolve_week_race(hook, season_year=season_year, week_number=week_number)
    if week_race is None:
        return {"path": path, "skipped": True, "reason": "week_not_in_db"}
    _week_id, race_id = week_race

    try:
        frame = _read_dk_salaries_csv(path)
    except (OSError, ValueError, pd.errors.ParserError) as exc:
        _LOG.warning("import_draftkings_salaries: skip %s: %s", path, exc)
        return {"path": path, "skipped": True, "reason": str(exc)}

    hook.run(
        """
        UPDATE nascar.dfs_salaries
        SET deleted_at = UTC_TIMESTAMP()
        WHERE dfs_id = %s
          AND race_id = %s
          AND deleted_at IS NULL
        """,
        parameters=(DRAFTKINGS_DFS_ID, race_id),
    )

    inserted = 0
    unmatched = 0
    for row in frame.itertuples(index=False):
        car_driver_id = _match_car_driver_id(
            hook,
            race_id=race_id,
            dfs_driver_id=int(row.dfs_driver_id),
            driver_name=str(row.driver_name),
        )
        if car_driver_id is None:
            unmatched += 1
            continue
        hook.run(
            """
            INSERT INTO nascar.dfs_salaries (
                dfs_id, race_id, dfs_driver_id, car_driver_id, salary
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            parameters=(
                DRAFTKINGS_DFS_ID,
                race_id,
                int(row.dfs_driver_id),
                car_driver_id,
                int(row.salary),
            ),
        )
        inserted += 1

    return {
        "path": path,
        "season_year": season_year,
        "week_number": week_number,
        "race_id": race_id,
        "rows_read": len(frame),
        "rows_inserted": inserted,
        "rows_unmatched": unmatched,
    }


def _parse_draftkings_salary_path(path: Path) -> tuple[int, int] | None:
    """Return ``(season_year, week_number)`` from a Cup DK salaries path."""
    if path.name != "DKSalaries.csv":
        return None
    parts = path.parts
    try:
        cup_idx = parts.index("cup")
        return int(parts[cup_idx + 1]), int(parts[cup_idx + 2])
    except (ValueError, IndexError):
        return None


def import_draftkings_salaries_task(**_context: object) -> dict[str, Any]:
    """Load every ``DKSalaries.csv`` found under the Cup DFS tree."""
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")
    root = draftkings_salaries_glob()
    if not root.exists():
        _LOG.info("import_draftkings_salaries: no dfs root at %s", root)
        return {"files": [], "imported": 0}

    results: list[dict[str, Any]] = []
    for path in sorted(root.glob("*/*/DKSalaries.csv")):
        parsed = _parse_draftkings_salary_path(path)
        if parsed is None:
            _LOG.warning("import_draftkings_salaries: unrecognized path %s", path)
            continue
        season_year, week_number = parsed
        results.append(
            _import_csv_file(
                hook,
                str(path),
                season_year=season_year,
                week_number=week_number,
            )
        )

    total_inserted = sum(r.get("rows_inserted", 0) for r in results)
    return {"files": results, "imported": total_inserted}
