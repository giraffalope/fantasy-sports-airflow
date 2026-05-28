-- Required DFS lineup strategy rows used by the lineup generator DAG.
-- Idempotent overlay when applying seeds without the full 9000 snapshot.
-- Canonical DDL: nascar/ddl/tables/1080-create_dfs_strategies_table.sql.
-- See migration 20260525180000_dfs_strategies_and_race_lineups_pivot.sql for
-- the originating change on existing databases.

INSERT INTO nascar.dfs_strategies (name)
VALUES ('DOUBLE_UP')
ON DUPLICATE KEY UPDATE
    deleted_at = NULL;

INSERT INTO nascar.dfs_strategies (name)
VALUES ('HIGH_RISK_REWARD')
ON DUPLICATE KEY UPDATE
    deleted_at = NULL;
