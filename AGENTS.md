# AGENTS

Repository-specific instructions for AI coding agents working in `fantasy-sports-airflow`.

## Cursor rules

Focused rules live in [`.cursor/rules/`](./.cursor/rules/). They auto-attach by glob (or always apply for navigation):

| Rule file | Scope |
| --- | --- |
| `repo-navigation.mdc` | Always apply — repo layout, docs index, `uv run` / `script/test` |
| `test-coverage.mdc` | `include/`, `dags/`, `airflow_utils/` Python — ~100% coverage on touched modules |
| `dag-changes.mdc` | `dags/**/*.py` — thin DAGs, tests + `docs/nascar/` coupling, backfill twins |
| `importer-sql.mdc` | `dags/sql/**/*.sql` — resolver/import boundary, DELETE reloads, contract tests |
| `nascar-mysql-writes.mdc` | `include/`, `airflow_utils/` — `NascarMySqlHook`, transactions, unique-key reloads |
| `integration-tests.mdc` | `tests/integration/**/*.py` — `@pytest.mark.integration`, `AIRFLOW_CONN_NASCAR_CONN` |

## Canonical Process Docs

For importer DAG process details (including `nascar.cup.importer`,
`backfill.nascar.cup.importer`, and downstream `nascar.cup.aggregate`), use:

- [`CONTRIBUTING.md`](./CONTRIBUTING.md) -> `## Importer DAG Process`

## Per-DAG Documentation

Authoritative per-DAG references live under [`docs/nascar/`](./docs/nascar/), one
markdown file per DAG source file. Start at [`docs/nascar/README.md`](./docs/nascar/README.md)
for the cross-DAG flow diagram, then drill into the file matching the DAG you
are touching. Keep these docs in sync when you change task IDs, ordering,
schedules, or external-sensor targets.

## Backfill DAG visibility

The five `backfill.*` Cup DAGs are hidden by default. Set `FS_ENABLE_BACKFILL_DAGS=1`
in [`.env`](./.env) (see [`.env.example`](./.env.example)) and restart scheduler /
dag-processor after toggling. Unit tests enable backfill via [`tests/conftest.py`](./tests/conftest.py).

## Scope and Priorities

- Prioritize data integrity for importer SQL, staging logic, and entity mapping.
- Prefer explicit nullability over sentinel placeholder values.
- Keep changes minimal and directly tied to the user request.

## Test Coverage for Agent Changes

When you add or change Python in `include/`, `dags/`, or `airflow_utils/`, **add or
extend unit tests in the same change** so touched modules stay as close to **100%
line coverage** as practical.

### What to cover

- **Happy paths** — normal task/SQL/helper execution and return payloads.
- **Branches** — early returns (`skipped`, `None`, empty query results), error handling,
  and guard clauses.
- **Edge cases** — empty inputs, dedupe logic, nullable fields, and formatting/parsing
  variants you introduce or rely on.

Skip tests that only assert obvious language behavior (for example that a constant
equals itself). Prefer one focused test per branch over a single shallow smoke test.

### Where tests live

| Code location | Test location |
| --- | --- |
| `include/nascar/...` | `tests/unit/test_<area>_<feature>.py` (mirror existing names) |
| `dags/nascar/cup/...` | `tests/unit/test_<dag>_dag.py` for DAG metadata and task graph |
| Importer SQL semantics | `tests/integration/test_*_sql_contract.py` |

Place CSV/HTML fixtures under `tests/unit/fixtures/` when needed. Follow patterns in
[`tests/unit/test_dfs_salary_import.py`](./tests/unit/test_dfs_salary_import.py) for
task modules: mock `NascarMySqlHook`, patch filesystem paths, invoke the write callback
from `run_in_transaction`, and assert SQL shape (not only that a mock was called).

### How to verify coverage on touched files

Run pytest with coverage scoped to the module you changed:

```bash
# Example: salary import task module
uv run pytest tests/unit/test_dfs_salary_import.py \
  --cov=include.nascar.dfs.salary_import_task \
  --cov-report=term-missing -o addopts=''
```

Aim for **no missing lines** in the report for modules you edited. If a line is
intentionally unreachable, use `# pragma: no cover` sparingly and comment why.

Repository-wide coverage is enforced at **30%** minimum via `pyproject.toml`
(`--cov-fail-under=30`); that gate is not a substitute for covering **your** diff.

### Commands

```bash
# Targeted unit tests (fast iteration)
uv run pytest -o addopts='' tests/unit/test_<relevant>.py

# Full unit suite with project coverage settings
uv run --group dev pytest tests/unit

# Docker parity
./script/test --unit
```

## Required Validation

When editing DAG or importer SQL, run relevant tests and checks before finalizing:

- **Unit tests** for new/changed Python modules (see [Test Coverage for Agent Changes](#test-coverage-for-agent-changes)).
- SQL/DAG contract tests for importer behavior changes.
- DAG unit tests for task ID/order/callable changes.
- Lint or compile checks for touched Python files.

Typical commands:

```bash
uv run pytest -o addopts='' tests/integration/test_cup_importer_sql_contract.py
uv run pytest -o addopts='' tests/unit/test_cup_scraper_importer_dag.py
```

If host environment tooling is missing, prefer `uv run ...` for project-local execution.

## Safe-Edit Expectations

- Do not edit plan files unless explicitly instructed.
- Do not revert unrelated local changes.
- Do not introduce destructive operations unless explicitly requested.
- Keep file edits tightly bounded to required scope.

## Change-Coupling Rules

- If modifying Python task helpers, callables, or utilities under `include/` or `airflow_utils/`,
  add or update unit tests and verify module coverage (target ~100% on touched files).
- If modifying DAG task IDs or dependency chains, update DAG unit tests.
- If modifying loader/import SQL semantics, update SQL contract assertions.
- If changing source-precedence or fallback behavior, add explicit test assertions for that rule.
- If changing null-handling behavior, add test coverage for both mapped and unmapped paths.
- If changing DAG task graphs, schedules, or external-sensor targets, refresh the matching file under [`docs/nascar/`](./docs/nascar/) in the same PR.

## Importer-Specific Guardrails

- Avoid creating sentinel entities like `'None'` in domain tables when nullable foreign keys are expected.
- Treat malformed source values defensively (sanitize before upsert/mapping).
- Preserve deterministic mapping order and precedence when merging FRCS and Racing Reference data.

## Operational Notes

- Prefer existing repository scripts and conventions:
  - `script/setup`, `script/format`, `script/lint`, `script/typecheck`, `script/deadcode`, `script/update`, `script/test`
  - `uv run ...` for deterministic local execution
- In final responses, include concise evidence of:
  - changed files
  - behavior outcome
  - test commands and pass/fail status
  - coverage on touched modules when Python logic changed (pass/fail and % if run)
