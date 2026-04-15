"""Unit tests for DAG ``nascar.cup.scraper.importer``."""

from __future__ import annotations

import nascar.cup.scraper.importer as importer_dag
from nascar.cup.scraper.backfill_assets import (
    BACKFILL_YEARLY_SCHEDULE,
    CUP_SCRAPER_PROD_HOURLY_CRON,
)


def test_dag_metadata() -> None:
    dag = importer_dag.dag
    assert dag.dag_id == "nascar.cup.scraper.importer"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule == CUP_SCRAPER_PROD_HOURLY_CRON


def test_backfill_dag_metadata() -> None:
    dag = importer_dag.backfill_dag
    assert dag.dag_id == "backfill.nascar.cup.scraper.importer"
    assert dag.catchup is True
    assert dag.schedule == BACKFILL_YEARLY_SCHEDULE


def test_task_ids_and_count() -> None:
    dag = importer_dag.dag
    assert set(dag.task_dict.keys()) == {
        "wait_frcs_race_week_sync_race_results",
        "wait_racing_reference_race_week_sync_race_results",
        "load_stage_schedule_csv",
        "import_schedule",
        "load_stage_entrylist_csvs",
        "import_entrylist",
        "load_stage_practices_csvs",
        "import_practices",
        "load_stage_qualifyings_csvs",
        "import_qualifyings",
        "load_stage_lineup_csvs",
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
        "import_fanduel",
        "cleanup_importer_staging_and_temp_tables",
    }
    assert len(dag.tasks) == 23


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
    load_stage_schedule = dag.get_task("load_stage_schedule_csv")
    import_schedule = dag.get_task("import_schedule")
    load_stage_entrylist = dag.get_task("load_stage_entrylist_csvs")
    import_entrylist = dag.get_task("import_entrylist")
    load_stage_practices = dag.get_task("load_stage_practices_csvs")
    import_practices = dag.get_task("import_practices")
    load_stage_qualifyings = dag.get_task("load_stage_qualifyings_csvs")
    import_qualifyings = dag.get_task("import_qualifyings")
    load_stage_lineup = dag.get_task("load_stage_lineup_csvs")
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
    import_fanduel = dag.get_task("import_fanduel")
    cleanup = dag.get_task("cleanup_importer_staging_and_temp_tables")

    assert wait_frcs.downstream_task_ids == {load_stage_schedule.task_id}
    assert wait_rr.downstream_task_ids == {load_stage_schedule.task_id}
    assert load_stage_schedule.downstream_task_ids == {import_schedule.task_id}
    assert import_schedule.downstream_task_ids == {load_stage_entrylist.task_id}
    assert load_stage_entrylist.downstream_task_ids == {import_entrylist.task_id}
    assert import_entrylist.downstream_task_ids == {load_stage_practices.task_id}
    assert load_stage_practices.downstream_task_ids == {import_practices.task_id}
    assert import_practices.downstream_task_ids == {load_stage_qualifyings.task_id}
    assert load_stage_qualifyings.downstream_task_ids == {import_qualifyings.task_id}
    assert import_qualifyings.downstream_task_ids == {load_stage_lineup.task_id}
    assert load_stage_lineup.downstream_task_ids == {import_lineup.task_id}
    assert import_lineup.downstream_task_ids == {load_stage_races.task_id}
    assert load_stage_races.downstream_task_ids == {import_races.task_id}
    assert import_races.downstream_task_ids == {load_stage_penalties.task_id}
    assert load_stage_penalties.downstream_task_ids == {import_penalties.task_id}
    assert import_penalties.downstream_task_ids == {load_stage_pitstops.task_id}
    assert load_stage_pitstops.downstream_task_ids == {import_pitstops.task_id}
    assert import_pitstops.downstream_task_ids == {load_stage_draftkings.task_id}
    assert load_stage_draftkings.downstream_task_ids == {import_draftkings.task_id}
    assert import_draftkings.downstream_task_ids == {load_stage_fanduel.task_id}
    assert load_stage_fanduel.downstream_task_ids == {import_fanduel.task_id}
    assert import_fanduel.downstream_task_ids == {cleanup.task_id}
    assert cleanup.downstream_task_ids == set()
