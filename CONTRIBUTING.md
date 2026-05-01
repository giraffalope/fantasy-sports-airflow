# Contributing

Thanks for contributing to `fantasy-sports-airflow`.

This repository runs Airflow-based NASCAR data pipelines that scrape, normalize, and import into MariaDB. Most contribution risk is in importer SQL and DAG orchestration, so validation and scope discipline are critical.

## Development Environment

Expected tools:

- `uv`
- `pyenv`
- Docker + Docker Compose

Recommended setup:

```bash
./script/setup
```

Alternative direct setup:

```bash
uv sync --group dev
```

If you prefer containerized tests and environment parity, use:

```bash
./script/test
```

## Local Workflow

1. Create a focused branch from your integration branch.
2. Keep changes tightly scoped to the requested behavior.
3. Run formatting and lint before tests:

```bash
./script/format
./script/lint
```

4. Run tests relevant to the files you changed.
5. Re-run broader tests when SQL/DAG behavior changes.

## Testing Policy

Primary host-side test command:

```bash
uv run --group dev pytest
```

Containerized test options:

```bash
./script/test
./script/test --unit
./script/test --integration
```

Notes:

- Coverage settings are defined in `pyproject.toml`.
- Use targeted tests during iteration, but include enough evidence in PRs that behavior is covered.
- If you change DAG task graphs or SQL contracts, update and run corresponding unit/integration tests.

## SQL and DAG Change Guidance

For importer and staging SQL:

- Preserve nullable semantics where intended.
- Do not introduce sentinel-value anti-patterns (for example literal `'None'` placeholders).
- Keep source-precedence rules explicit and test-covered.

For DAG changes:

- Keep task IDs and dependency order deterministic.
- Update DAG unit tests when task IDs/order/callables change.

For SQL contract changes:

- Update SQL contract tests whenever semantic clauses or table dependencies change.
- Prefer precise assertions for critical invariants (fallback logic, null-handling, source precedence).

## Importer DAG Process

The Cup importer is exposed as two DAGs with shared task logic:

- `nascar.cup.importer` (production schedule, no catchup)
- `backfill.nascar.cup.importer` (yearly backfill schedule, catchup enabled)
- `nascar.cup.aggregate` (hourly production aggregate load after importer cleanup)

Both DAGs are created by the same DAG builder in `dags/nascar/cup/scraper/importer.py`, so task IDs, task order, and SQL execution semantics are intentionally shared unless explicitly changed.

High-level flow:

1. Wait for FRCS and Racing Reference race-week sync tasks.
2. Load schedule staging and import schedule.
3. Run all remaining `load_stage_*` tasks, including shared car candidate staging (`stg_car_*`).
4. Run resolver logic to build per-week canonical car mappings (`tmp_car_*`) and resolve/create IDs.
5. Run all `import_*` tasks using resolved IDs from `tmp_car_*`.
6. Cleanup importer staging/temp tables for the run-specific suffix.

Single-writer rule for importer SQL:

- `resolve_drivers_cars_manufacturers_teams.sql` is the only writer for entity and mapping tables (`drivers`, `manufacturers`, `teams`, `cars`, `cars_drivers`, and `data_sources_*` mapping tables).
- `import_*` SQL files are consumers of resolver outputs: they read staged source data, join `tmp_car_*` for canonical IDs, and write only their fact/output tables.

Production vs backfill differences are configuration-level only:

- Different `dag_id`, `schedule`, and `catchup` settings.
- Different upstream external DAG IDs used by the wait sensors.
- Same importer task chain and SQL contracts.

When changing importer orchestration:

- Treat task IDs and dependency order as contract surface.
- Update DAG unit tests when task IDs/callables/dependencies change.
- Update SQL contract tests when importer SQL behavior changes.
- Validate both production and backfill DAG assumptions still hold.

## Commit and PR Standards

Commit guidance:

- Use concise, descriptive commit messages that explain intent.
- Group related changes in one commit where practical.

PR expectations:

- Clear summary of what changed and why.
- Explicit testing section with commands run and outcomes.
- Call out schema/import behavior impact.
- Mention rollback considerations for risky importer changes.

Suggested PR checklist:

- [ ] Scope is limited to the stated change.
- [ ] Lint/format pass.
- [ ] Relevant tests pass.
- [ ] DAG/SQL contract tests updated when needed.
- [ ] No unrelated file churn.

## Troubleshooting Quick Reference

### `pytest` missing in host Python

Use project-managed environment:

```bash
uv run --group dev pytest
```

### Docker services unhealthy

- Check `docker ps`.
- Rebuild/start stack using project scripts:

```bash
./script/build
./script/server
```

### Missing scraper-data files

- Confirm `scraper-data/` exists locally and mount path is correct.
- Verify `AIRFLOW_SCRAPER_DATA_DIR` if using a custom path.

### MariaDB connectivity confusion

- Prefer repository-standard credentials/config from `docker-compose.yaml` and Airflow connection env vars.
- Avoid hardcoding local-only overrides in committed code.
