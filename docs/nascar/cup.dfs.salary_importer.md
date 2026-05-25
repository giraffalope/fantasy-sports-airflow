# `nascar.cup.dfs.salary_importer`

Hourly Cup DraftKings salary import. Ensures the current-week DFS folder exists on
the mounted volume, then loads `DKSalaries.csv` for the series current week only
(from `v_series_current_week`) into `nascar.dfs_salaries`.

- **DAG ID:** `nascar.cup.dfs.salary_importer` (no `backfill.*` twin)
- **Source:** [dags/nascar/cup/dfs/salary_importer.py](../../dags/nascar/cup/dfs/salary_importer.py)
- **Tests:** [tests/unit/test_dfs_salary_importer_dag.py](../../tests/unit/test_dfs_salary_importer_dag.py),
  [tests/unit/test_dfs_salary_import.py](../../tests/unit/test_dfs_salary_import.py),
  [tests/unit/test_dfs_paths.py](../../tests/unit/test_dfs_paths.py)

## Schedule and settings

| Setting | Value |
| --- | --- |
| Cron / `schedule` | `CUP_SCRAPER_PROD_HOURLY_CRON` (`0 * * * *`) |
| `catchup` | `False` |
| `max_active_runs` | `1` |

## Volume layout

Host path (default): `fantasy-sports-airflow/dfs-data/` bind-mounted to
`/home/airflow/dfs` in containers (`AIRFLOW_DFS_DATA_DIR` override).

Drop the DraftKings export here before the hourly import runs:

```text
dfs-data/draftkings/nascar/cup/{season_year}/{week_number}/DKSalaries.csv
```

The first task creates the current-week directory automatically; it does **not**
create the CSV file.

## Tasks

| Task ID | Callable | Purpose |
| --- | --- | --- |
| `ensure_draftkings_week_directory` | `ensure_draftkings_week_directory_task` | `mkdir -p` for the current Cup week (`v_series_current_week`) |
| `import_draftkings_salaries` | `import_draftkings_salaries_task` | Import current-week `DKSalaries.csv` only; replace `dfs_salaries` for the race via delete+insert (`dfs_id=1`) |

### CSV mapping

| Column | DB field |
| --- | --- |
| `ID` | `dfs_driver_id` |
| `Name` | driver name matching → `car_driver_id` |
| `Salary` | `salary` |

Matching order per row: existing `dfs_salaries` by `(dfs_driver_id, race_id)`, else
normalized driver name against `race_results` for that week’s race.

## Related

- Lineup DAG: [cup.dfs.lineup_generator.md](./cup.dfs.lineup_generator.md)
- ML predictions: [cup.machine_learning.md](./cup.machine_learning.md)
