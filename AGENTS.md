# AGENTS

Repository-specific instructions for AI coding agents working in `fantasy-sports-airflow`.

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

## Scope and Priorities

- Prioritize data integrity for importer SQL, staging logic, and entity mapping.
- Prefer explicit nullability over sentinel placeholder values.
- Keep changes minimal and directly tied to the user request.

## Required Validation

When editing DAG or importer SQL, run relevant tests and checks before finalizing:

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
  - `script/setup`, `script/format`, `script/lint`, `script/test`
  - `uv run ...` for deterministic local execution
- In final responses, include concise evidence of:
  - changed files
  - behavior outcome
  - test commands and pass/fail status
