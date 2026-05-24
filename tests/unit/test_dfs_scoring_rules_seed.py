"""Contract tests for DFS scoring rules seed SQL."""

from __future__ import annotations

from pathlib import Path

SEED_ROOT = (
    Path(__file__).resolve().parents[2].parent / "fantasy-sports-db" / "nascar" / "seed"
)


def test_dfs_seed_files_exist_and_reference_draftkings() -> None:
    dfs_sql = (SEED_ROOT / "9001-insert_dfs_seed.sql").read_text(encoding="utf-8")
    rules_sql = (SEED_ROOT / "9002-insert_dfs_scoring_rules_seed.sql").read_text(encoding="utf-8")

    assert "INSERT INTO nascar.dfs" in dfs_sql
    assert "'DraftKings'" in dfs_sql
    assert "VALUES (1," in dfs_sql

    assert "INSERT INTO nascar.dfs_scoring_rules" in rules_sql
    assert "'cup_default'" in rules_sql
    assert "dfs_id" in rules_sql
    assert "1,\n    'cup_default'" in rules_sql or "1, 'cup_default'" in rules_sql
