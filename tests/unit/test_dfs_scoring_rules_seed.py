"""Contract tests for DFS scoring rules seed SQL."""

from __future__ import annotations

from pathlib import Path

_REPO_SEED = Path(__file__).resolve().parents[2].parent / "fantasy-sports-db" / "nascar" / "seed"
_FIXTURE_SEED = Path(__file__).resolve().parent.parent / "fixtures" / "dfs_seed"
SEED_ROOT = _FIXTURE_SEED if _FIXTURE_SEED.is_dir() else _REPO_SEED


def test_dfs_seed_files_exist_and_reference_draftkings() -> None:
    dfs_sql = (SEED_ROOT / "9001-insert_dfs_seed.sql").read_text(encoding="utf-8")
    rules_sql = (SEED_ROOT / "9002-insert_dfs_scoring_rules_seed.sql").read_text(encoding="utf-8")

    assert "INSERT INTO nascar.dfs" in dfs_sql
    assert "'DraftKings'" in dfs_sql
    assert "VALUES (1," in dfs_sql

    assert "dfs_scoring_rules" in rules_sql
    assert "'cup_default'" in rules_sql
    assert "dfs_id" in rules_sql
