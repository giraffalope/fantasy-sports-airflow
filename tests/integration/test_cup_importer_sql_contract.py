from __future__ import annotations

from pathlib import Path


SQL_ROOT = Path(__file__).resolve().parents[2] / "dags" / "sql" / "nascar.cup.scraper.importer"


def _sql_text(name: str) -> str:
    return (SQL_ROOT / name).read_text(encoding="utf-8")


def test_practice_import_sql_inserts_results_and_entity_upserts() -> None:
    sql = _sql_text("import_frcs_practice_staging_into_db.sql")
    assert "INSERT INTO nascar.practices" in sql
    assert "INSERT INTO nascar.practice_results" in sql
    assert "INSERT INTO nascar.drivers" in sql
    assert "INSERT INTO nascar.manufacturers" in sql
    assert "INSERT INTO nascar.cars" in sql
    assert "INSERT INTO nascar.cars_drivers" in sql


def test_qualifying_import_sql_inserts_results_and_entity_upserts() -> None:
    sql = _sql_text("import_frcs_qualifying_staging_into_db.sql")
    assert "INSERT INTO nascar.qualifyings" in sql
    assert "INSERT INTO nascar.qualifying_results" in sql
    assert "INSERT INTO nascar.drivers" in sql
    assert "INSERT INTO nascar.manufacturers" in sql
    assert "INSERT INTO nascar.cars" in sql
    assert "INSERT INTO nascar.cars_drivers" in sql


def test_practice_and_qualifying_import_sql_nulls_zero_speed_and_time() -> None:
    practice_sql = _sql_text("import_frcs_practice_staging_into_db.sql")
    qualifying_sql = _sql_text("import_frcs_qualifying_staging_into_db.sql")
    for sql in (practice_sql, qualifying_sql):
        assert "CAST(TRIM(t.speed) AS DECIMAL(8, 3)) = 0 THEN NULL" in sql
        assert "CAST(TRIM(t.time) AS DECIMAL(12, 3)) = 0 THEN NULL" in sql
        assert "CAST(TRIM(t.time) AS TIME(3)) = '00:00:00.000' THEN NULL" in sql
        assert "AND TRIM(t.time) != ''" in sql


def test_race_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_frcs_race_staging_into_db.sql")
    assert "INSERT INTO nascar.races" in sql
    assert "INSERT INTO nascar.race_results" in sql
    assert "INSERT INTO nascar.drivers" in sql
    assert "INSERT INTO nascar.manufacturers" in sql
    assert "INSERT INTO nascar.cars" in sql
    assert "INSERT INTO nascar.cars_drivers" in sql


def test_entrylist_and_lineup_import_sql_include_team_upserts() -> None:
    entrylist_sql = _sql_text("import_frcs_entrylist_staging_into_db.sql")
    lineup_sql = _sql_text("import_frcs_lineup_staging_into_db.sql")
    assert "INSERT INTO nascar.teams" in entrylist_sql
    assert "INSERT INTO nascar.teams" in lineup_sql
    assert "INSERT INTO nascar.drivers" in lineup_sql
    assert "INSERT INTO nascar.manufacturers" in lineup_sql
    assert "INSERT INTO nascar.cars" in lineup_sql
    assert "INSERT INTO nascar.cars_drivers" in lineup_sql


def test_penalties_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_frcs_penalties_staging_into_db.sql")
    assert "INSERT INTO nascar.penalties" in sql
    assert "INSERT INTO nascar.drivers" in sql


def test_pitstops_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_frcs_pit_stops_detail_staging_into_db.sql")
    assert "INSERT INTO nascar.pitstops" in sql
    assert "crew_time" in sql
    assert "COALESCE" in sql and "driver_time" in sql
    assert "ROW_NUMBER()" in sql


def test_draftkings_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_frcs_draftkings_staging_into_db.sql")
    assert "INSERT INTO nascar.dfs " in sql
    assert "INSERT INTO nascar.dfs_salaries" in sql
    assert "ROW_NUMBER()" in sql


def test_fanduel_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_frcs_fanduel_staging_into_db.sql")
    assert "INSERT INTO nascar.dfs " in sql
    assert "INSERT INTO nascar.dfs_salaries" in sql
    assert "ROW_NUMBER()" in sql


def test_load_stage_practice_and_qualifying_sqls_target_staging_tables() -> None:
    practice_sql = _sql_text("load_frcs_practice_csv_into_staging_table.sql")
    qualifying_sql = _sql_text("load_frcs_qualifying_csv_into_staging_table.sql")
    assert "INTO TABLE nascar.stg_frcs_practice_" in practice_sql
    assert "LOAD DATA LOCAL INFILE" in practice_sql
    assert "INTO TABLE nascar.stg_frcs_qualifying_" in qualifying_sql
    assert "LOAD DATA LOCAL INFILE" in qualifying_sql


def test_load_stage_race_penalty_pitstop_dfs_sqls_target_staging_tables() -> None:
    race_sql = _sql_text("load_frcs_race_csvs_into_staging_tables.sql")
    assert "race_results_csv_n_columns" in race_sql
    assert "race_results_idx_status" in race_sql
    assert "@c" in race_sql and "IGNORE 1 LINES" in race_sql
    penalties_sql = _sql_text("load_frcs_penalties_csv_into_staging_table.sql")
    pitstops_sql = _sql_text("load_frcs_pit_stops_detail_csv_into_staging_table.sql")
    dk_sql = _sql_text("load_frcs_draftkings_csv_into_staging_table.sql")
    fd_sql = _sql_text("load_frcs_fanduel_csv_into_staging_table.sql")
    assert "stg_frcs_race_results_" in race_sql and "stg_frcs_race_loop_data_" in race_sql
    assert "stg_frcs_penalties_" in penalties_sql
    assert "stg_frcs_pit_stops_detail_" in pitstops_sql
    assert "stg_frcs_draftkings_" in dk_sql
    assert "stg_frcs_fanduel_" in fd_sql
