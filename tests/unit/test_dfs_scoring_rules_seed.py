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


def test_dfs_strategies_seed_inserts_required_strategy_rows() -> None:
    """Seed 9003 must materialize the strategies the lineup DAG looks up by name.

    ``generate_draftkings_lineups_task`` resolves DK lineup strategies via
    :func:`include.nascar.dfs.lineup_generator_task._strategy_id_by_name`,
    which raises ``RuntimeError`` when the row is missing. Pinning the
    seed contents here means a future rename in the seed (or a missing
    strategy) fails CI before it can crash the lineup DAG in prod.
    """
    seed_sql = (SEED_ROOT / "9003-insert_dfs_strategies_seed.sql").read_text(
        encoding="utf-8"
    )

    assert "INSERT INTO nascar.dfs_strategies" in seed_sql
    assert "'DOUBLE_UP'" in seed_sql
    assert "'HIGH_RISK_REWARD'" in seed_sql
    # Idempotent overlay style (matches 9001/9002): revives soft-deleted
    # rows rather than colliding on the unique key.
    assert "ON DUPLICATE KEY UPDATE" in seed_sql
    assert "deleted_at = NULL" in seed_sql
