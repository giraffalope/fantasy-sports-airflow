"""Static SQL contracts for Cup aggregate load templates."""

from __future__ import annotations

import re
from pathlib import Path

from include.nascar.cup.aggregate_load import ALL_AGG_LOAD_TASKS

SQL_ROOT = Path(__file__).resolve().parents[2] / "dags" / "sql" / "nascar.cup.aggregate"

_PROC_RE = re.compile(
    r"CALL nascar\.(load_agg_[a-z0-9_]+)\(\{\{ series_id \}\}(?:, \{\{ week\.week_id \}\})?\);"
)


def test_aggregate_sql_files_cover_wave_tasks() -> None:
    sql_files = sorted(SQL_ROOT.glob("load_agg_*.sql"))
    assert len(sql_files) >= 35

    task_ids = {path.stem for path in sql_files}
    for task_id in ALL_AGG_LOAD_TASKS:
        assert task_id in task_ids, f"missing SQL for {task_id}"


def test_aggregate_sql_files_call_matching_procedures() -> None:
    for path in sorted(SQL_ROOT.glob("load_agg_*.sql")):
        text = path.read_text(encoding="utf-8")
        proc_name = path.stem
        if proc_name == "load_agg_week_results":
            assert text.strip() == f"CALL nascar.{proc_name}({{{{ series_id }}}});"
            continue

        matches = _PROC_RE.findall(text)
        assert matches, f"{path.name} has no CALL statements"
        assert all(match == proc_name for match in matches), path.name
        assert "{{ series_id }}" in text
        assert "week.week_id" in text
