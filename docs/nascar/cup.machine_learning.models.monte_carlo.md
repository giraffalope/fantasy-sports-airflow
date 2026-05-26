# `nascar.cup.machine_learning.models.monte_carlo`

Hourly Cup Monte Carlo race-simulation DAG. Waits for
`nascar.cup.machine_learning.models` / `predict_upcoming_week`, draws at least
1,000 simulated DraftKings-point outcomes per driver from the calibrated
prediction uncertainty, and persists batch + per-driver summaries to MariaDB
for the lineup generator and downstream BI.

- **DAG ID:** `nascar.cup.machine_learning.models.monte_carlo` (no `backfill.*` twin)
- **Source:** [dags/nascar/cup/machine_learning_models_monte_carlo.py](../../dags/nascar/cup/machine_learning_models_monte_carlo.py)
- **Tests:** [tests/unit/test_machine_learning_models_monte_carlo_dag.py](../../tests/unit/test_machine_learning_models_monte_carlo_dag.py),
  [tests/unit/test_monte_carlo.py](../../tests/unit/test_monte_carlo.py),
  [tests/unit/test_monte_carlo_task.py](../../tests/unit/test_monte_carlo_task.py)

## Schedule and settings

| Setting | Value |
| --- | --- |
| Cron / `schedule` | `CUP_SCRAPER_PROD_HOURLY_CRON` (`0 * * * *`) |
| `catchup` | `False` |
| `max_active_runs` | `1` |

## Task graph

```mermaid
flowchart LR
  wait_ml_predict_complete --> run_monte_carlo_simulation
  run_monte_carlo_simulation --> validate_simulation_batch
```

## Tasks

### `wait_ml_predict_complete`

`ExternalTaskSensor` on `nascar.cup.machine_learning.models` /
`predict_upcoming_week` (same poke / timeout as other Cup ML sensors).

### `run_monte_carlo_simulation`

`PythonOperator` calling
[`include/nascar/ml/monte_carlo_task.py::run_monte_carlo_simulation_task`](../../include/nascar/ml/monte_carlo_task.py).
Pure-Python core lives in
[`include/nascar/ml/monte_carlo.py`](../../include/nascar/ml/monte_carlo.py)
(no Airflow / MySQL imports).

**Flow:**

1. Resolve the active DraftKings `dfs_scoring_rules` row. Skips with
   `no_active_rules` if missing.
2. Load the latest `kind='predict'` snapshot for the active rules
   (`v_series_current_week`). Skips with `no_predict_snapshot` if missing.
3. Load active `ml_predictions` for the snapshot joined with
   `ml_training_rows` (for `r_start_position`). Skips with
   `no_ml_predictions` if empty.
4. For each target in `DEFAULT_TARGETS` (`r_finish_position`,
   `r_total_fastest_laps`, `r_total_laps_led`), look up the latest
   active `ml_model_evaluations` row for any model whose feature set
   targets that single stat in the same series. Prefer `metric='rmse'`;
   when only `metric='mae'` exists (the current production state) convert
   to σ assuming Gaussian residuals (σ ≈ MAE / 0.7979). Missing
   secondary-target σ collapses to zero (target held constant). Missing
   primary-target σ short-circuits with `no_primary_target_sigma` since
   no useful dispersion can be produced.
5. Build `MonteCarloDriverInput` records and call
   `simulate_driver_distributions`. Per simulation draw each target is
   perturbed with `Normal(0, σ)`, projected into its valid range
   (`r_finish_position ∈ [1, field_size]`, laps / fastest laps clipped at
   `0`; all three rounded to int), and re-scored via
   `compute_dfs_points_from_targets`. Drivers are treated as independent
   conditional on features (v1 limitation; rank-coupling is out of scope).
6. Aggregate each driver's simulated DFS-point vector into mean, std, min,
   p05, p50, p95, and max.
7. Persist inside a single transaction: soft-delete any prior active
   `ml_race_simulation_batches` row for the same snapshot (and its
   summaries), insert the new batch header, then `executemany` the
   per-driver summary rows. Re-runs are idempotent.

### `validate_simulation_batch`

`PythonOperator` calling `validate_simulation_batch_task`. Hard-fails the
DAG run when:

- No active batch exists for the snapshot.
- `n_simulations < 1000` on the latest batch.
- The count of active driver summaries does not match the active
  prediction pool for the snapshot.

This is the task the lineup generator's `wait_mc_complete` sensor blocks
on.

## Skip conditions

The run task returns `{"skipped": true, "reason": ...}` with the reason
logged at WARNING. The validate task instead raises (these are not skip
conditions; they are programming or pipeline errors).

| Reason | Cause |
| --- | --- |
| `no_active_rules` | No active `dfs_scoring_rules` row for DraftKings |
| `no_predict_snapshot` | No `kind='predict'` snapshot for the current week |
| `no_ml_predictions` | No active `ml_predictions` rows for the snapshot |
| `no_primary_target_sigma` | Missing `rmse` / `mae` evaluation for `r_finish_position` |

## Configuration

- **Airflow connection:** `nascar_conn` (MySQL).
- **Env vars** (also documented in `.env.example`):

| Var | Default | Meaning |
| --- | --- | --- |
| `FS_MC_N_SIMULATIONS` | `1000` | Draws per driver (validation enforces ≥ 1000) |
| `FS_MC_RANDOM_SEED` | derived from snapshot id | Override the numpy seed for reproducibility |
| `FS_PIPELINE_VERSION` / `GIT_COMMIT` | unset | Stamped onto the batch row for lineage |

## Database tables

- `ml_race_simulation_batches` — one row per snapshot, holds
  `n_simulations`, `random_seed`, `pipeline_version`, `status`. Indexed on
  `(ml_training_snapshot_id, deleted_at)`.
- `ml_race_simulation_driver_summaries` — one row per
  `(batch_id, car_driver_id)` with mean / std / min / p05 / p50 / p95 /
  max DFS-point statistics. Unique on `(batch_id, car_driver_id)`.

DDL: `fantasy-sports-db/nascar/ddl/tables/1082-*.sql`, `1083-*.sql`;
migration `fantasy-sports-db/nascar/migrations/20260526T120000_ml_race_simulation_tables.sql`.

## Limitations (v1)

- Drivers are independent conditional on features; correlations across
  drivers (shared "race chaos") are not modeled.
- Raw per-draw rows are intentionally not stored (40k+ rows per Cup week
  would dwarf the summaries with little downstream value).
- Per-target σ is read from any matching active model, not specifically
  the one that produced the prediction (per-target model ids are not
  stored in `ml_predictions`). Both are typically the most recent active
  model and agree in practice; making this exact would require widening
  `ml_predictions` and is deferred.

## Related

- Upstream: [cup.machine_learning.models.md](./cup.machine_learning.models.md)
- Downstream: [cup.dfs.lineup_generator.md](./cup.dfs.lineup_generator.md)
