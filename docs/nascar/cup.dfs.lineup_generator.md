# `nascar.cup.dfs.lineup_generator`

Hourly Cup DraftKings lineup generator. Waits for Monte Carlo
`validate_simulation_batch` on
[`nascar.cup.machine_learning.models.monte_carlo`](./cup.machine_learning.models.monte_carlo.md)
(which itself waits on ML `predict_upcoming_week`), then builds top-K optimal
lineups per strategy when simulation summaries and DK salaries exist.

- **DAG ID:** `nascar.cup.dfs.lineup_generator` (no `backfill.*` twin)
- **Source:** [dags/nascar/cup/dfs/lineup_generator.py](../../dags/nascar/cup/dfs/lineup_generator.py)
- **Tests:** [tests/unit/test_dfs_lineup_generator_dag.py](../../tests/unit/test_dfs_lineup_generator_dag.py),
  [tests/unit/test_dfs_lineup_optimizer.py](../../tests/unit/test_dfs_lineup_optimizer.py),
  [tests/unit/test_dfs_lineup_generator_gates.py](../../tests/unit/test_dfs_lineup_generator_gates.py),
  [tests/unit/test_lineup_portfolio.py](../../tests/unit/test_lineup_portfolio.py)

## Schedule and settings

| Setting | Value |
| --- | --- |
| Cron / `schedule` | `CUP_SCRAPER_PROD_HOURLY_CRON` (`0 * * * *`) |
| `catchup` | `False` |
| `max_active_runs` | `1` |

## Upstream dependency

`wait_mc_complete` sensors `nascar.cup.machine_learning.models.monte_carlo` /
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
- CP-SAT candidate pool yields zero feasible lineups under salary cap (`no_feasible_lineups`)

Parent lineup rows use `NascarMySqlHook.run_insert_returning_id` (not
`LAST_INSERT_ID()` on a pooled connection) so child `dfs_race_lineups_car_drivers`
rows get a valid `dfs_race_lineup_id`.

## Portfolio-aware optimizer

The lineup task now runs in **three** layered stages so the final 20 lineups
reflect both Monte Carlo variance and portfolio-level diversification:

```mermaid
flowchart LR
  MC["MC summaries +<br/>joint draws (.npz)"] --> POOL["CP-SAT candidate pool<br/>top_k × multiplier"]
  POOL --> Score["Score each candidate<br/>against joint draws"]
  Score --> Sel["Greedy diversify<br/>(per-driver exposure caps,<br/>per-strategy objective)"]
  Sel --> LU["dfs_race_lineups<br/>+ 8-column MC fingerprint"]
```

1. **Candidate pool.**
   [`generate_candidate_pool`](../../include/nascar/dfs/lineup_optimizer.py) runs
   the CP-SAT solver in a loop with `min_diff=1` so the pool spans as many
   distinct lineups as possible (`top_k × FS_DK_CANDIDATE_POOL_MULTIPLIER`,
   default 20 × 5 = 100). The CP-SAT objective is the salary-cap-respecting
   sum of `projected_points`, where `projected_points` is the per-driver
   summary column selected by `FS_DK_MC_OBJECTIVE` (default `p50`). This
   knob now only controls **which candidates make the pool**, not the final
   ranking.
2. **Joint MC scoring.**
   [`score_candidates_against_joint_samples`](../../include/nascar/dfs/lineup_portfolio.py)
   sums each candidate's six driver rows in the
   `(n_drivers, n_simulations)` joint matrix loaded from the per-batch
   `.npz` sidecar (see Monte Carlo doc), then computes lineup mean / std /
   p10 / p50 / p90 / sharpe. Because the matrix preserves Phase E2
   rank-coupling (every iteration is a finish-position permutation), the
   resulting lineup std is materially lower than a naive sum-of-driver-stds
   — that's the whole point.
3. **Greedy diversify.**
   [`greedy_diversify`](../../include/nascar/dfs/lineup_portfolio.py) sorts
   the scored pool by the strategy's objective metric, walks descending,
   and accepts each lineup iff every driver still has remaining exposure
   budget (`ceil(top_k × exposure_cap)`). When the cap is too tight to
   fill `top_k` lineups the cap is relaxed `+0.05` at a time with a
   WARNING log per relaxation; the operator never silently gets a short
   portfolio.

### Per-strategy objectives

| Strategy | Default objective | Default cap | Why |
| --- | --- | --- | --- |
| `DOUBLE_UP` (cash / 50-50) | `joint_p10` (high floor) | `0.70` | Cash payouts reward consistency over upside; duplicating reliable floors across many lineups is fine. |
| `HIGH_RISK_REWARD` (GPP / tournament) | `joint_p90` (high ceiling) | `0.40` | Top-heavy payouts reward ceilings; spreading risk across more drivers protects against a single bust. |

Valid objective tokens (env vars below): `joint_p10`, `joint_p50`,
`joint_p90`, `joint_mean`, `joint_sharpe`. Anything else falls back to
`joint_p10` / `joint_p90` and logs a WARNING.

### Configuration

| Env var | Default | Meaning |
| --- | --- | --- |
| `FS_DK_LINEUP_SIZE` | `6` | Drivers per lineup |
| `FS_DK_SALARY_CAP` | `50000` | Salary cap |
| `FS_DK_TOP_K` | `20` | Lineups per strategy |
| `FS_DK_CANDIDATE_POOL_MULTIPLIER` | `5` | CP-SAT pool size = `top_k × multiplier` |
| `FS_DK_MIN_DIFF_DOUBLE_UP` | `2` | CP-SAT overlap blocker for DOUBLE_UP candidate solves |
| `FS_DK_MIN_DIFF_HIGH_RISK_REWARD` | `1` | CP-SAT overlap blocker for HIGH_RISK_REWARD candidate solves |
| `FS_DK_MC_OBJECTIVE` | `p50` | Per-driver summary column used only to **order** the candidate pool (`mean`, `p05`, `p50`, `p95`, `min`, `max`) |
| `FS_DK_DOUBLE_UP_OBJECTIVE` | `joint_p10` | Portfolio objective for DOUBLE_UP |
| `FS_DK_DOUBLE_UP_EXPOSURE_CAP` | `0.70` | Max fraction of DOUBLE_UP lineups any driver may appear in |
| `FS_DK_HIGH_RISK_REWARD_OBJECTIVE` | `joint_p90` | Portfolio objective for HIGH_RISK_REWARD |
| `FS_DK_HIGH_RISK_REWARD_EXPOSURE_CAP` | `0.40` | Max fraction of HIGH_RISK_REWARD lineups any driver may appear in |

### Summary-fallback path

When `ml_race_simulation_batches.joint_samples_artifact_uri` is `NULL`,
the file is missing, or the schema check fails (driver missing,
`n_simulations` mismatch, etc.), the lineup task synthesizes an
independent-Gaussian joint matrix from each driver's
`mean_dfs_points` / `std_dfs_points` and stamps every row's
`mc_score_method` with `summary_fallback` so the audit trail
distinguishes it from true joint scoring. The fallback path keeps the
pipeline running on first deploy and on degraded artifact writes, but
the variance-aware portfolio benefit only kicks in once a
post-migration MC batch with a writable artifact root has shipped.

## Database tables

- `dfs_race_lineups` — lineup header. Widened with 8 MC fingerprint columns:
  `mc_mean_dfs_points`, `mc_std_dfs_points`, `mc_p10_dfs_points`,
  `mc_p50_dfs_points`, `mc_p90_dfs_points`, `mc_sharpe`, `objective_value`
  (the actual scalar the selector ranked on), and `mc_score_method` (audit
  trail: `joint_p10` / `joint_p50` / `joint_p90` / `joint_mean` /
  `joint_sharpe` / `summary_fallback`).
- `dfs_strategies` — strategy lookup (`name`: `DOUBLE_UP`, `HIGH_RISK_REWARD`).
- `dfs_strategies_race_lineups` — pivot linking lineups to strategies.
- `dfs_race_lineups_car_drivers` — picked drivers (`dfs_driver_id` from salaries).

Driver exposure by strategy: view `nascar.v_dfs_strategies_drivers_exposure`.

DDL: `fantasy-sports-db/nascar/ddl/tables/1078-*.sql`, `1079-*.sql`, `1080-*.sql`, `1081-*.sql`.
Migration adding the 8 MC fingerprint columns: `20260613T120000_lineup_mc_fingerprint.sql`.

## Related

- Salary import: [cup.dfs.salary_importer.md](./cup.dfs.salary_importer.md)
- Joint MC artifact: [cup.machine_learning.models.monte_carlo.md](./cup.machine_learning.models.monte_carlo.md)
