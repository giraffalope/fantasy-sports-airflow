"""Unit tests for DAG ``nascar.cup.scraper.importer``."""

from __future__ import annotations

import nascar.cup.scraper.importer as importer_dag
import nascar.cup.scraper.frcs.race_week as frcs_race_week_dag
import nascar.cup.scraper.racing_reference.race_week as rr_race_week_dag


def test_dag_metadata() -> None:
    dag = importer_dag.dag
    assert dag.dag_id == "nascar.cup.scraper.importer"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert frcs_race_week_dag.NASCAR_FRCS_CUP_RACE_WEEK_ASSET in dag.schedule
    assert rr_race_week_dag.NASCAR_RACING_REFERENCE_CUP_RACE_WEEK_ASSET in dag.schedule


def test_task_ids_and_count() -> None:
    dag = importer_dag.dag
    assert set(dag.task_dict.keys()) == {
        "load_stage_frcs_schedule_csv",
        "import_frcs_schedule",
        "load_stage_frcs_entrylist_csvs",
        "import_frcs_entrylist",
        "load_stage_frcs_practices_csvs",
        "import_frcs_practices",
        "load_stage_frcs_qualifyings_csvs",
        "import_frcs_qualifyings",
        "load_stage_frcs_lineup_csvs",
        "import_frcs_lineup",
        "load_stage_frcs_races_csvs",
        "import_frcs_races",
        "load_stage_frcs_penalties_csvs",
        "import_frcs_penalties",
        "load_stage_frcs_pitstops_csvs",
        "import_frcs_pitstops",
        "load_stage_frcs_draftkings_csvs",
        "import_frcs_draftkings",
        "load_stage_frcs_fanduel_csvs",
        "import_frcs_fanduel",
        "load_stage_racing_reference_schedule_csv",
    }
    assert len(dag.tasks) == 21


def test_linear_dependencies() -> None:
    dag = importer_dag.dag
    import_frcs_schedule = dag.get_task("load_stage_frcs_schedule_csv")
    import_frcs_data = dag.get_task("import_frcs_schedule")
    load_stage_entrylist = dag.get_task("load_stage_frcs_entrylist_csvs")
    import_entrylist = dag.get_task("import_frcs_entrylist")
    load_stage_practices = dag.get_task("load_stage_frcs_practices_csvs")
    import_practices = dag.get_task("import_frcs_practices")
    load_stage_qualifyings = dag.get_task("load_stage_frcs_qualifyings_csvs")
    import_qualifyings = dag.get_task("import_frcs_qualifyings")
    load_stage_lineup = dag.get_task("load_stage_frcs_lineup_csvs")
    import_lineup = dag.get_task("import_frcs_lineup")
    load_stage_races = dag.get_task("load_stage_frcs_races_csvs")
    import_races = dag.get_task("import_frcs_races")
    load_stage_penalties = dag.get_task("load_stage_frcs_penalties_csvs")
    import_penalties = dag.get_task("import_frcs_penalties")
    load_stage_pitstops = dag.get_task("load_stage_frcs_pitstops_csvs")
    import_pitstops = dag.get_task("import_frcs_pitstops")
    load_stage_draftkings = dag.get_task("load_stage_frcs_draftkings_csvs")
    import_draftkings = dag.get_task("import_frcs_draftkings")
    load_stage_fanduel = dag.get_task("load_stage_frcs_fanduel_csvs")
    import_fanduel = dag.get_task("import_frcs_fanduel")
    import_racing_reference_schedule = dag.get_task("load_stage_racing_reference_schedule_csv")

    assert import_frcs_schedule.downstream_task_ids == {import_frcs_data.task_id}
    assert import_frcs_data.downstream_task_ids == {load_stage_entrylist.task_id}
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
    assert import_fanduel.downstream_task_ids == {import_racing_reference_schedule.task_id}
    assert import_racing_reference_schedule.downstream_task_ids == set()
