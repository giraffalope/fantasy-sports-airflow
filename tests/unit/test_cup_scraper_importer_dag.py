"""Unit tests for DAG ``nascar.cup.importer``."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import nascar.cup.importer as importer_dag
from nascar.cup.scraper.backfill_assets import (
    BACKFILL_YEARLY_SCHEDULE,
    CUP_SCRAPER_PROD_HOURLY_CRON,
)


def test_dag_metadata() -> None:
    dag = importer_dag.dag
    assert dag.dag_id == "nascar.cup.importer"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON


def test_backfill_dag_metadata() -> None:
    dag = importer_dag.backfill_dag
    assert dag.dag_id == "backfill.nascar.cup.importer"
    assert dag.catchup is True
    assert dag.schedule == BACKFILL_YEARLY_SCHEDULE


def test_task_ids_and_count() -> None:
    dag = importer_dag.dag
    assert set(dag.task_dict.keys()) == {
        "wait_frcs_race_week_sync_race_results",
        "wait_racing_reference_race_week_sync_race_results",
        "load_stage_schedule_csvs",
        "import_schedule",
        "load_stage_practices_csvs",
        "import_practices",
        "load_stage_qualifyings_csvs",
        "import_qualifyings",
        "load_stage_lineup_csvs",
        "load_stage_entrylist_csvs",
        "import_lineup",
        "load_stage_races_csvs",
        "import_races",
        "load_stage_penalties_csvs",
        "import_penalties",
        "load_stage_pitstops_csvs",
        "import_pitstops",
        "load_stage_draftkings_csvs",
        "import_draftkings",
        "load_stage_fanduel_csvs",
        "bootstrap_shared_stg_car_tables",
        "import_fanduel",
        "resolve_drivers_cars_manufacturers_teams",
        "cleanup_importer_staging_and_temp_tables",
    }
    assert len(dag.tasks) == 24
    assert "import_entrylist" not in dag.task_dict


def test_wait_tasks_target_prod_race_week_dags() -> None:
    dag = importer_dag.dag
    wait_frcs = dag.get_task("wait_frcs_race_week_sync_race_results")
    wait_rr = dag.get_task("wait_racing_reference_race_week_sync_race_results")
    assert wait_frcs.external_dag_id == "nascar.cup.scraper.frcs.race_week"
    assert wait_rr.external_dag_id == "nascar.cup.scraper.racing_reference.race_week"
    assert wait_frcs.external_task_id == "sync_race_results"
    assert wait_rr.external_task_id == "sync_race_results"


def test_backfill_wait_tasks_target_backfill_race_week_dags() -> None:
    dag = importer_dag.backfill_dag
    wait_frcs = dag.get_task("wait_frcs_race_week_sync_race_results")
    wait_rr = dag.get_task("wait_racing_reference_race_week_sync_race_results")
    assert wait_frcs.external_dag_id == "backfill.nascar.cup.scraper.frcs.race_week"
    assert wait_rr.external_dag_id == "backfill.nascar.cup.scraper.racing_reference.race_week"


def test_linear_dependencies() -> None:
    dag = importer_dag.dag
    wait_frcs = dag.get_task("wait_frcs_race_week_sync_race_results")
    wait_rr = dag.get_task("wait_racing_reference_race_week_sync_race_results")
    load_stage_schedule = dag.get_task("load_stage_schedule_csvs")
    import_schedule = dag.get_task("import_schedule")
    load_stage_practices = dag.get_task("load_stage_practices_csvs")
    import_practices = dag.get_task("import_practices")
    load_stage_qualifyings = dag.get_task("load_stage_qualifyings_csvs")
    import_qualifyings = dag.get_task("import_qualifyings")
    load_stage_lineup = dag.get_task("load_stage_lineup_csvs")
    load_stage_entrylist = dag.get_task("load_stage_entrylist_csvs")
    import_lineup = dag.get_task("import_lineup")
    load_stage_races = dag.get_task("load_stage_races_csvs")
    import_races = dag.get_task("import_races")
    load_stage_penalties = dag.get_task("load_stage_penalties_csvs")
    import_penalties = dag.get_task("import_penalties")
    load_stage_pitstops = dag.get_task("load_stage_pitstops_csvs")
    import_pitstops = dag.get_task("import_pitstops")
    load_stage_draftkings = dag.get_task("load_stage_draftkings_csvs")
    import_draftkings = dag.get_task("import_draftkings")
    load_stage_fanduel = dag.get_task("load_stage_fanduel_csvs")
    bootstrap_shared_stg_car_tables = dag.get_task("bootstrap_shared_stg_car_tables")
    import_fanduel = dag.get_task("import_fanduel")
    resolve_drivers_cars_manufacturers_teams = dag.get_task(
        "resolve_drivers_cars_manufacturers_teams"
    )
    cleanup = dag.get_task("cleanup_importer_staging_and_temp_tables")

    assert wait_frcs.downstream_task_ids == {load_stage_schedule.task_id}
    assert wait_rr.downstream_task_ids == {load_stage_schedule.task_id}
    assert load_stage_schedule.downstream_task_ids == {import_schedule.task_id}
    assert import_schedule.downstream_task_ids == {load_stage_practices.task_id}
    assert load_stage_practices.downstream_task_ids == {load_stage_qualifyings.task_id}
    assert load_stage_qualifyings.downstream_task_ids == {load_stage_lineup.task_id}
    assert load_stage_lineup.downstream_task_ids == {load_stage_entrylist.task_id}
    assert load_stage_entrylist.downstream_task_ids == {load_stage_races.task_id}
    assert load_stage_races.downstream_task_ids == {load_stage_penalties.task_id}
    assert load_stage_penalties.downstream_task_ids == {load_stage_pitstops.task_id}
    assert load_stage_pitstops.downstream_task_ids == {load_stage_draftkings.task_id}
    assert load_stage_draftkings.downstream_task_ids == {load_stage_fanduel.task_id}
    assert load_stage_fanduel.downstream_task_ids == {bootstrap_shared_stg_car_tables.task_id}
    assert bootstrap_shared_stg_car_tables.downstream_task_ids == {
        resolve_drivers_cars_manufacturers_teams.task_id
    }
    assert resolve_drivers_cars_manufacturers_teams.downstream_task_ids == {import_practices.task_id}
    assert import_practices.downstream_task_ids == {import_qualifyings.task_id}
    assert import_qualifyings.downstream_task_ids == {import_lineup.task_id}
    assert import_lineup.downstream_task_ids == {import_races.task_id}
    assert import_races.downstream_task_ids == {import_penalties.task_id}
    assert import_penalties.downstream_task_ids == {import_pitstops.task_id}
    assert import_pitstops.downstream_task_ids == {import_draftkings.task_id}
    assert import_draftkings.downstream_task_ids == {import_fanduel.task_id}
    assert import_fanduel.downstream_task_ids == {cleanup.task_id}
    assert cleanup.downstream_task_ids == set()


def test_cleanup_prefixes_include_schedule_stage_and_tmp_tables() -> None:
    prefixes = importer_dag._IMPORTER_STAGING_PREFIXES
    assert "stg_car_" in prefixes
    assert "stg_cup_racing_reference_schedule_import_" in prefixes
    assert "stg_car_season_" in prefixes
    assert "tmp_cup_schedule_frcs_" in prefixes
    assert "tmp_cup_schedule_rr_" in prefixes
    assert "tmp_cup_schedule_rr_match_guard_" in prefixes
    assert "tmp_cup_schedule_merged_" in prefixes
    assert "tmp_car_" in prefixes
    assert "tmp_car_season_" in prefixes


def test_cleanup_drops_all_current_suffix_stg_car_tables(monkeypatch) -> None:
    suffix = "202604301900"
    listed = [
        (f"stg_car_1019_{suffix}",),
        (f"stg_car_1020_{suffix}",),
        (f"stg_car_season_17_{suffix}",),
        ("stg_car_1018_202604301800",),
    ]
    executed: list[str] = []

    class _Cursor:
        def execute(self, sql: str) -> None:
            executed.append(sql)

        def close(self) -> None:
            return None

    class _Conn:
        def cursor(self) -> _Cursor:
            return _Cursor()

        def commit(self) -> None:
            return None

        def close(self) -> None:
            return None

    class _Hook:
        def __init__(self, mysql_conn_id: str) -> None:
            self.mysql_conn_id = mysql_conn_id

        def get_records(self, sql: str, parameters: tuple[str, ...]) -> list[tuple[str]]:
            return listed

        def get_conn(self) -> _Conn:
            return _Conn()

    monkeypatch.setattr(importer_dag, "NascarMySqlHook", _Hook)
    monkeypatch.setattr(importer_dag, "_importer_table_suffix", lambda _: suffix)

    kwargs = {
        "ti": SimpleNamespace(xcom_pull=lambda **_: [f"stg_car_1019_{suffix}"]),
        "data_interval_start": datetime(2026, 4, 30),
        "logical_date": datetime(2026, 4, 30),
        "dag_run": SimpleNamespace(run_id="manual__cleanup"),
    }
    importer_dag.cleanup_importer_staging_and_temp_tables(**kwargs)

    assert f"DROP TABLE IF EXISTS `nascar`.`stg_car_1019_{suffix}`" in executed
    assert f"DROP TABLE IF EXISTS `nascar`.`stg_car_1020_{suffix}`" in executed
    assert f"DROP TABLE IF EXISTS `nascar`.`stg_car_season_17_{suffix}`" in executed
    assert "DROP TABLE IF EXISTS `nascar`.`stg_car_1018_202604301800`" not in executed
