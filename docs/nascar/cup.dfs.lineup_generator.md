# `nascar.cup.dfs.lineup_generator`

Hourly Cup DraftKings lineup generator. Waits for Monte Carlo
`validate_simulation_batch` on
[`nascar.cup.machine_learning.monte_carlo`](./cup.machine_learning.monte_carlo.md)
(which itself waits on ML `predict_upcoming_week`), then builds top-K optimal
lineups per strategy when simulation summaries and DK salaries exist.

- **DAG ID:** `nascar.cup.dfs.lineup_generator` (no `backfill.*` twin)
- **Source:** [dags/nascar/cup/dfs/lineup_generator.py](../../dags/nascar/cup/dfs/lineup_generator.py)
- **Tests:** [tests/unit/test_dfs_lineup_generator_dag.py](../../tests/unit/test_dfs_lineup_generator_dag.py),
  [tests/unit/test_dfs_lineup_optimizer.py](../../tests/unit/test_dfs_lineup_optimizer.py),
  [tests/unit/test_dfs_lineup_generator_gates.py](../../tests/unit/test_dfs_lineup_generator_gates.py)

## Schedule and settings

| Setting | Value |
| --- | --- |
| Cron / `schedule` | `CUP_SCRAPER_PROD_HOURLY_CRON` (`0 * * * *`) |
| `catchup` | `False` |
| `max_active_runs` | `1` |

## Upstream dependency

`wait_mc_complete` sensors `nascar.cup.machine_learning.monte_carlo` /
`validate_simulation_batch` (same poke/timeout as aggregate→ML sensors). The
Monte Carlo DAG itself sensors `predict_upcoming_week`, so this also blocks
implicitly on ML predict.

`wait_dk_salaries_complete` sensors `nascar.cup.dfs.salary_importer` /
`import_draftkings_salaries`. The generate task still soft-skips with a log if
salaries or simulation summaries are missing in the database.

## Tasks

| Task ID | Purpose |
| --- | --- |
| `wait_mc_complete` | `ExternalTaskSensor` on Monte Carlo validate task |
| `wait_dk_salaries_complete` | `ExternalTaskSensor` on DK salary import task |
| `generate_draftkings_lineups` | Top-K CP-SAT lineups → `dfs_race_lineups` + child rows |

## Skip conditions

Skip paths log at WARNING with `reason=…`. The generate task returns
`{"skipped": true, "reason": ...}` when any of:

- No active `dfs_scoring_rules` for DraftKings
- No latest `kind='predict'` snapshot
- No active `ml_race_simulation_batches` row for that snapshot (`no_mc_summaries`)
- No `races` row for snapshot `week_id`
- No active `dfs_salaries` for DraftKings on that `week_id` (join via `races`)
- Player pool smaller than lineup size (default 6) — inner join of MC summaries + salaries
- Optimizer finds zero feasible lineups under salary cap (`no_feasible_lineups`)

Parent lineup rows use `NascarMySqlHook.run_insert_returning_id` (not
`LAST_INSERT_ID()` on a pooled connection) so child `dfs_race_lineups_car_drivers`
rows get a valid `dfs_race_lineup_id`.

## Optimizer

Module: [include/nascar/dfs/lineup_optimizer.py](../../include/nascar/dfs/lineup_optimizer.py)
(OR-Tools CP-SAT).

| Env var | Default | Meaning |
| --- | --- | --- |
| `FS_DK_LINEUP_SIZE` | `6` | Drivers per lineup |
| `FS_DK_SALARY_CAP` | `50000` | Salary cap |
| `FS_DK_TOP_K` | `20` | Lineups per strategy |
| `FS_DK_MIN_DIFF_DOUBLE_UP` | `2` | Min drivers different vs prior lineup |
| `FS_DK_MIN_DIFF_HIGH_RISK_REWARD` | `1` | Min drivers different vs prior lineup |
| `FS_DK_MC_OBJECTIVE` | `p50` | Monte Carlo summary column used as `projected_points` (`mean`, `p05`, `p50`, `p95`, `min`, `max`) |

Runs twice per race: `DOUBLE_UP` and `HIGH_RISK_REWARD`. Soft-deletes prior active
lineups for `(dfs_id, race_id, strategy)` before insert. `projected_points` is
sourced from the latest active `ml_race_simulation_driver_summaries` row using
the column selected by `FS_DK_MC_OBJECTIVE`.

## Database tables

- `dfs_race_lineups` — lineup header (projected/salary totals)
- `dfs_strategies` — strategy lookup (`name`: `DOUBLE_UP`, `HIGH_RISK_REWARD`)
- `dfs_strategies_race_lineups` — pivot linking lineups to strategies
- `dfs_race_lineups_car_drivers` — picked drivers (`dfs_driver_id` from salaries)

Driver exposure by strategy: view `nascar.v_dfs_strategies_drivers_exposure`.

DDL: `fantasy-sports-db/nascar/ddl/tables/1078-*.sql`, `1079-*.sql`, `1080-*.sql`, `1081-*.sql`.

## Related

- Salary import: [cup.dfs.salary_importer.md](./cup.dfs.salary_importer.md)
