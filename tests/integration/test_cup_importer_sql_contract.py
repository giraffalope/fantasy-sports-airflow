from __future__ import annotations

from pathlib import Path

SQL_ROOT = Path(__file__).resolve().parents[2] / "dags" / "sql" / "nascar.cup.importer"


def _sql_text(name: str) -> str:
    return (SQL_ROOT / name).read_text(encoding="utf-8")


def test_practice_import_sql_inserts_results_without_entity_writes() -> None:
    sql = _sql_text("import_practice_staging_into_db.sql")
    assert "INSERT INTO nascar.practices" in sql
    assert "INSERT INTO nascar.practice_results" in sql
    assert "tmp_car_{{ week_id }}_{{ table_suffix }}" in sql
    assert "INSERT INTO nascar.drivers" not in sql
    assert "INSERT INTO nascar.manufacturers" not in sql
    assert "INSERT INTO nascar.teams" not in sql
    assert "INSERT INTO nascar.cars" not in sql
    assert "INSERT INTO nascar.cars_drivers" not in sql


def test_qualifying_import_sql_inserts_results_without_entity_writes() -> None:
    sql = _sql_text("import_qualifying_staging_into_db.sql")
    assert "INSERT INTO nascar.qualifyings" in sql
    assert "INSERT INTO nascar.qualifying_results" in sql
    assert "tmp_car_{{ week_id }}_{{ table_suffix }}" in sql
    assert "INSERT INTO nascar.drivers" not in sql
    assert "INSERT INTO nascar.manufacturers" not in sql
    assert "INSERT INTO nascar.teams" not in sql
    assert "INSERT INTO nascar.cars" not in sql
    assert "INSERT INTO nascar.cars_drivers" not in sql


def test_practice_and_qualifying_import_sql_filters_blank_time_and_null_speed() -> None:
    practice_sql = _sql_text("import_practice_staging_into_db.sql")
    qualifying_sql = _sql_text("import_qualifying_staging_into_db.sql")
    for sql in (practice_sql, qualifying_sql):
        assert "AND TRIM(t.time) != ''" in sql
        assert "by_driver.speed IS NOT NULL" in sql


def test_load_practice_and_qualifying_apply_clean_speed_and_time() -> None:
    practice_load = _sql_text("load_practice_csv_into_staging_table.sql")
    qualifying_load = _sql_text("load_qualifying_csv_into_staging_table.sql")
    assert "nascar.clean_speed(@speed)" in practice_load
    assert "nascar.clean_time(@time)" in practice_load
    assert "nascar.clean_speed(@speed)" in qualifying_load
    assert "nascar.clean_time(@time)" in qualifying_load


def test_qualifying_import_sql_only_uses_numeric_rank_for_finish_position() -> None:
    sql = _sql_text("import_qualifying_staging_into_db.sql")
    assert "CAST(t.rank AS UNSIGNED) AS finish_position" in sql
    assert "AND by_driver.finish_position IS NOT NULL" in sql


def test_race_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_race_staging_into_db.sql")
    assert "INSERT INTO nascar.races" in sql
    assert "INSERT INTO nascar.race_results" in sql
    assert "tmp_car_{{ week_id }}_{{ table_suffix }}" in sql
    assert "INSERT INTO nascar.drivers" not in sql
    assert "INSERT INTO nascar.manufacturers" not in sql
    assert "INSERT INTO nascar.teams" not in sql
    assert "INSERT INTO nascar.cars" not in sql
    assert "INSERT INTO nascar.cars_drivers" not in sql


def test_lineup_import_sql_uses_tmp_car_and_does_not_write_entities() -> None:
    lineup_sql = _sql_text("import_lineup_staging_into_db.sql")
    assert "INSERT INTO nascar.race_results" in lineup_sql
    assert "tmp_car_{{ week_id }}_{{ table_suffix }}" in lineup_sql
    assert "INSERT INTO nascar.data_sources_drivers" not in lineup_sql
    assert "INSERT INTO nascar.data_sources_manufacturers" not in lineup_sql
    assert "INSERT INTO nascar.data_sources_teams" not in lineup_sql
    assert "INSERT INTO nascar.drivers" not in lineup_sql
    assert "INSERT INTO nascar.manufacturers" not in lineup_sql
    assert "INSERT INTO nascar.teams" not in lineup_sql
    assert "INSERT INTO nascar.cars" not in lineup_sql
    assert "INSERT INTO nascar.cars_drivers" not in lineup_sql


def test_lineup_import_sql_does_not_delete_races() -> None:
    lineup_sql = _sql_text("import_lineup_staging_into_db.sql")
    assert "DELETE FROM nascar.races" not in lineup_sql


def test_race_import_sql_deletes_lineup_shells_and_staged_drivers_before_insert() -> None:
    race_sql = _sql_text("import_race_staging_into_db.sql")
    assert "total_laps_completed IS NULL" in race_sql
    assert "TRIM(rr.finish_position) = ''" in race_sql
    assert "tmp_race_{{ week_id }}_{{ table_suffix }}" in race_sql
    assert "incoming.car_driver_id" in race_sql
    assert "ON DUPLICATE KEY UPDATE" in race_sql
    assert "start_position = VALUES(start_position)" in race_sql
    assert "finish_position = VALUES(finish_position)" in race_sql


def test_schedule_import_sql_enforces_rr_frcs_guard() -> None:
    schedule_sql = _sql_text("import_schedule.sql")
    assert "tmp_cup_schedule_rr_match_guard" in schedule_sql
    assert "schedule_rr_frcs_guard_ok" in schedule_sql
    assert "1 / 0" in schedule_sql


def test_lineup_import_sql_writes_start_position_not_finish() -> None:
    lineup_sql = _sql_text("import_lineup_staging_into_db.sql")
    assert "INSERT INTO nascar.race_results (race_id, car_driver_id, start_position)" in lineup_sql
    assert "CAST(stg.start AS UNSIGNED) AS start_position" in lineup_sql
    assert "CAST(stg.start AS UNSIGNED) AS finish_position" not in lineup_sql
    assert "ON DUPLICATE KEY UPDATE" in lineup_sql
    assert "start_position = VALUES(start_position)" in lineup_sql
    assert "total_laps_completed IS NULL" in lineup_sql
    assert (
        "DELETE rr\nFROM nascar.race_results rr\nINNER JOIN nascar.races r\n"
        "    ON r.id = rr.race_id\nWHERE r.week_id = {{ week_id }}\n"
        "    AND r.deleted_at IS NULL\n"
        "    AND rr.deleted_at IS NULL\n"
        "    AND rr.total_laps_completed IS NULL"
    ) in lineup_sql


def test_downstream_sql_requires_car_driver_id_for_fact_inserts() -> None:
    practice_sql = _sql_text("import_practice_staging_into_db.sql")
    qualifying_sql = _sql_text("import_qualifying_staging_into_db.sql")
    race_sql = _sql_text("import_race_staging_into_db.sql")
    for sql in (practice_sql, qualifying_sql, race_sql):
        assert "car_driver_id IS NOT NULL" in sql


def test_penalties_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_penalties_staging_into_db.sql")
    assert "DELETE pen FROM nascar.penalties" in sql
    assert "INSERT INTO nascar.penalties" in sql
    assert "tmp_car_{{ week_id }}_{{ table_suffix }}" in sql


def test_pitstops_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_pit_stops_detail_staging_into_db.sql")
    assert "DELETE ps FROM nascar.pitstops" in sql
    assert "INSERT INTO nascar.pitstops" in sql
    assert "tmp_car_{{ week_id }}_{{ table_suffix }}" in sql


def test_draftkings_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_draftkings_staging_into_db.sql")
    assert "INSERT INTO nascar.dfs " in sql
    assert "DELETE dsal FROM nascar.dfs_salaries" in sql
    assert "INSERT INTO nascar.dfs_salaries" in sql
    assert "tmp_car_{{ week_id }}_{{ table_suffix }}" in sql


def test_fanduel_import_sql_inserts_expected_destination_tables() -> None:
    sql = _sql_text("import_fanduel_staging_into_db.sql")
    assert "INSERT INTO nascar.dfs " in sql
    assert "DELETE dsal FROM nascar.dfs_salaries" in sql
    assert "INSERT INTO nascar.dfs_salaries" in sql
    assert "tmp_car_{{ week_id }}_{{ table_suffix }}" in sql


def test_import_sqls_use_car_number_then_driver_name_fallback() -> None:
    files = [
        "import_practice_staging_into_db.sql",
        "import_qualifying_staging_into_db.sql",
        "import_race_staging_into_db.sql",
        "import_lineup_staging_into_db.sql",
        "import_pit_stops_detail_staging_into_db.sql",
        "import_draftkings_staging_into_db.sql",
        "import_fanduel_staging_into_db.sql",
    ]
    for file_name in files:
        sql = _sql_text(file_name)
        assert "tc_num" in sql
        assert "tc_driver" in sql
        if file_name == "import_race_staging_into_db.sql":
            assert "COALESCE(tc_driver.car_driver_id, tc_num.car_driver_id)" in sql
        else:
            assert "tc_num.car_driver_id IS NULL" in sql


def test_load_stage_practice_and_qualifying_sqls_target_staging_tables() -> None:
    practice_sql = _sql_text("load_practice_csv_into_staging_table.sql")
    qualifying_sql = _sql_text("load_qualifying_csv_into_staging_table.sql")
    assert "INTO TABLE nascar.stg_frcs_practice_" in practice_sql
    assert "LOAD DATA LOCAL INFILE" in practice_sql
    assert "INTO TABLE nascar.stg_frcs_qualifying_" in qualifying_sql
    assert "LOAD DATA LOCAL INFILE" in qualifying_sql
    assert "data_source_id BIGINT UNSIGNED NOT NULL" in practice_sql
    assert "data_source_id BIGINT UNSIGNED NOT NULL" in qualifying_sql
    assert "stg_car_season_{{ season_id }}_{{ table_suffix }}" in practice_sql
    assert "stg_car_season_{{ season_id }}_{{ table_suffix }}" in qualifying_sql
    assert "week_id BIGINT UNSIGNED NOT NULL" in practice_sql
    assert "week_id BIGINT UNSIGNED NOT NULL" in qualifying_sql
    assert "ds.id" in practice_sql
    assert "ds.id" in qualifying_sql


def test_load_stage_race_penalty_pitstop_dfs_sqls_target_staging_tables() -> None:
    race_sql = _sql_text("load_race_csvs_into_staging_tables.sql")
    assert "stg_frcs_race_results_" in race_sql
    assert "stg_frcs_race_loop_data_" in race_sql
    assert "@position" in race_sql and "@driver_name" in race_sql
    assert race_sql.count("LOAD DATA LOCAL INFILE") == 2
    assert race_sql.count("IGNORE 1 LINES") == 2
    assert "owner = nascar.clean_team_name(@owner)" in race_sql
    assert "clean_team_name(stg.sponsor)" not in race_sql
    penalties_sql = _sql_text("load_penalties_csv_into_staging_table.sql")
    pitstops_sql = _sql_text("load_pit_stops_detail_csv_into_staging_table.sql")
    dk_sql = _sql_text("load_draftkings_csv_into_staging_table.sql")
    fd_sql = _sql_text("load_fanduel_csv_into_staging_table.sql")
    assert "stg_frcs_penalties_" in penalties_sql
    assert "stg_frcs_pit_stops_detail_" in pitstops_sql
    assert "stg_frcs_draftkings_" in dk_sql
    assert "stg_frcs_fanduel_" in fd_sql
    assert "data_source_id BIGINT UNSIGNED NOT NULL" in race_sql
    assert "data_source_id BIGINT UNSIGNED NOT NULL" in penalties_sql
    assert "data_source_id BIGINT UNSIGNED NOT NULL" in pitstops_sql
    assert "data_source_id BIGINT UNSIGNED NOT NULL" in dk_sql
    assert "data_source_id BIGINT UNSIGNED NOT NULL" in fd_sql
    assert "stg_car_season_{{ season_id }}_{{ table_suffix }}" in race_sql
    assert "stg_car_season_{{ season_id }}_{{ table_suffix }}" in penalties_sql
    assert "stg_car_season_{{ season_id }}_{{ table_suffix }}" in pitstops_sql
    assert "stg_car_season_{{ season_id }}_{{ table_suffix }}" in dk_sql
    assert "stg_car_season_{{ season_id }}_{{ table_suffix }}" in fd_sql
    assert "ds.id" in race_sql
    assert "ds.id" in penalties_sql
    assert "ds.id" in pitstops_sql
    assert "ds.id" in dk_sql
    assert "ds.id" in fd_sql


def test_import_schedule_sql_merges_dual_sources_and_validates_rr_match() -> None:
    load_stage_sql = _sql_text("load_stage_schedule_csvs.sql")
    assert "{{ frcs_schedule_csv_file_path }}" in load_stage_sql
    assert "{{ racing_reference_schedule_csv_file_path }}" in load_stage_sql
    assert "{{ schedule_csv_file_path }}" not in load_stage_sql

    sql = _sql_text("import_schedule.sql")
    assert "tmp_cup_schedule_frcs_{{ table_suffix }}" in sql
    assert "tmp_cup_schedule_rr_{{ table_suffix }}" in sql
    assert "tmp_cup_schedule_merged_{{ table_suffix }}" in sql
    assert "tmp_cup_schedule_rr_match_guard_{{ table_suffix }}" in sql
    assert "rr_rows_must_match_frcs TINYINT NOT NULL" in sql
    assert "THEN NULL" in sql
    assert "ds.name = '{{ frcs_source_name }}'" in sql
    assert "ds.name = '{{ racing_reference_source_name }}'" in sql
    assert (
        "COALESCE(NULLIF(TRIM(frcs.week_track), ''), NULLIF(TRIM(rr.week_track), '')) AS week_track_preferred"
        in sql
    )
    assert "week_track_canonical" in sql
    assert "resolved_track_id" in sql
    assert "COALESCE(dst_frcs.track_id, dst_rr.track_id)" in sql
    assert "ON t.id = merged.resolved_track_id" in sql
    assert "ON DUPLICATE KEY UPDATE\n    `tracks`.`deleted_at` = `tracks`.`deleted_at`" in sql
    assert (
        "JOIN nascar.tracks t\n    ON TRIM(t.name) = TRIM(merged.week_track_preferred)" not in sql
    )
    assert (
        "INSERT INTO nascar.weeks (season_id, track_id, number, season_number, race_name, scheduled_start, event_type)"
        in sql
    )
    assert "WHEN CAST(merged.frcs_week_number AS UNSIGNED) IS NULL THEN 'OTHER'" in sql
    assert "WHEN CAST(merged.frcs_week_number AS UNSIGNED) = 0 THEN 'OTHER'" in sql
    assert "ELSE 'REGULAR'" in sql


def test_resolve_drivers_cars_manufacturers_teams_sql_is_scoped_and_can_create_entities() -> None:
    sql = _sql_text("resolve_drivers_cars_manufacturers_teams.sql")
    assert "nascar.stg_car_{{ week_id }}_{{ table_suffix }}" in sql
    assert "nascar.stg_car_season_{{ season_id }}_{{ table_suffix }}" in sql
    assert "nascar.tmp_car_{{ week_id }}_{{ table_suffix }}" in sql
    assert "nascar.tmp_car_season_{{ season_id }}_{{ table_suffix }}" in sql
    assert "tmp_stg_car_numbers_{{ week_id }}_{{ table_suffix }}" in sql
    assert "tmp_stg_car_numbers_season_{{ season_id }}_{{ table_suffix }}" in sql
    assert "tmp_stg_car_historical_season_{{ season_id }}_{{ table_suffix }}" in sql
    assert "tmp_current_week_season_context_{{ season_id }}_{{ table_suffix }}" in sql
    assert "tmp_stg_car_frcs_to_rr_scored_{{ week_id }}_{{ table_suffix }}" in sql
    assert "tmp_car_driver_rank_{{ week_id }}_{{ table_suffix }}" in sql
    assert "tmp_car_manufacturer_rank_{{ week_id }}_{{ table_suffix }}" in sql
    assert "tmp_car_team_rank_{{ week_id }}_{{ table_suffix }}" in sql
    assert "tmp_car_team_source_stats_{{ week_id }}_{{ table_suffix }}" in sql
    assert "tmp_car_unresolved_teams_{{ week_id }}_{{ table_suffix }}" in sql
    assert "ON ds.name IN ('{{ frcs_source_name }}', '{{ racing_reference_source_name }}')" in sql
    assert "CASE WHEN nascar.clean_driver_name(f.driver_name) IS NOT NULL" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "w.number < ctx.current_week_number" in sql
    assert "FROM nascar.practice_results pr" in sql
    assert "JOIN nascar.practices p" in sql
    assert "FROM nascar.qualifying_results qr" in sql
    assert "JOIN nascar.qualifyings q" in sql
    assert "FROM nascar.race_results rr" in sql
    assert "JOIN nascar.races r" in sql
    assert "FROM nascar.pitstops ps" in sql
    assert "FROM nascar.penalties p" in sql
    assert "FROM nascar.dfs_salaries dsal" in sql
    assert "SELECT pr.week_id" not in sql
    assert "SELECT qr.week_id" not in sql
    assert "SELECT rr.week_id" not in sql
    assert ") IS NOT NULL;\nUNION ALL" not in sql
    assert ") IS NOT NULL\nUNION ALL\nSELECT\n    nascar.clean_car_number(h.car_number)" in sql
    assert "UNION ALL" in sql
    assert "PRIMARY KEY (car_number)" in sql
    assert "JOIN nascar.tmp_car_season_{{ season_id }}_{{ table_suffix }} s" in sql
    assert "w.driver_name = COALESCE(w.driver_name, s.driver_name)" in sql
    assert "w.car_id = COALESCE(w.car_id, s.car_id)" not in sql
    assert "INSERT INTO nascar.drivers" in sql
    assert "INSERT INTO nascar.teams" in sql
    assert "INSERT INTO nascar.manufacturers" in sql
    assert "INSERT INTO nascar.cars (" in sql
    assert "tmp_car_candidate_rank_{{ week_id }}_{{ table_suffix }}" in sql
    assert "PARTITION BY t.car_number" in sql
    assert "CASE WHEN c.team_id IS NOT NULL THEN 1 ELSE 0 END DESC" in sql
    assert "CASE WHEN c.manufacturer_id IS NOT NULL THEN 1 ELSE 0 END DESC" in sql
    assert "AND t.team_id IS NOT NULL" in sql
    assert "AND t.team_id IS NULL" in sql
    assert "AND NOT EXISTS (" in sql
    assert "team_id = COALESCE(nascar.cars.team_id, VALUES(team_id))" in sql
    assert "manufacturer_id = COALESCE(nascar.cars.manufacturer_id, VALUES(manufacturer_id))" in sql
    assert "AND nascar.get_driver_first_name(t.driver_name) IS NOT NULL" in sql
    assert "AND nascar.get_driver_last_name(t.driver_name) IS NOT NULL" in sql
    assert "UPPER(TRIM(t.team_name)) <> 'N/A'" in sql
    assert "INSERT INTO nascar.cars_drivers" in sql
    assert "INSERT INTO nascar.data_sources_drivers" in sql
    assert "INSERT INTO nascar.data_sources_manufacturers" in sql
    assert "INSERT INTO nascar.data_sources_teams" in sql


def test_import_sqls_do_not_write_entity_or_mapping_tables() -> None:
    import_files = [
        "import_practice_staging_into_db.sql",
        "import_qualifying_staging_into_db.sql",
        "import_lineup_staging_into_db.sql",
        "import_race_staging_into_db.sql",
        "import_penalties_staging_into_db.sql",
        "import_pit_stops_detail_staging_into_db.sql",
        "import_draftkings_staging_into_db.sql",
        "import_fanduel_staging_into_db.sql",
    ]
    forbidden_tokens = [
        "INSERT INTO nascar.data_sources_drivers",
        "INSERT INTO nascar.data_sources_manufacturers",
        "INSERT INTO nascar.data_sources_teams",
        "UPDATE nascar.data_sources_drivers",
        "UPDATE nascar.data_sources_manufacturers",
        "UPDATE nascar.data_sources_teams",
        "INSERT INTO nascar.drivers",
        "INSERT INTO nascar.manufacturers",
        "INSERT INTO nascar.teams",
        "INSERT INTO nascar.cars",
        "INSERT INTO nascar.cars_drivers",
        "UPDATE nascar.drivers",
        "UPDATE nascar.manufacturers",
        "UPDATE nascar.teams",
        "UPDATE nascar.cars",
        "UPDATE nascar.cars_drivers",
    ]
    for file_name in import_files:
        sql = _sql_text(file_name)
        for token in forbidden_tokens:
            assert token not in sql
