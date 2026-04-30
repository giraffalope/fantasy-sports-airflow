# fantasy-sports-airflow

Apache Airflow 3 project that schedules and runs data pipelines for fantasy NASCAR-style workloads. DAGs download and normalize racing data (for example from [Racing Reference](https://www.racing-reference.info/)), convert HTML exports to CSV, and load rows into a MariaDB **nascar** schema via templated SQL.

The stack is intended to run in Docker (Airflow with Celery, PostgreSQL, Redis, and a MariaDB service that is typically built from a sibling **fantasy-sports-db** repository). Shared Python helpers and scraper logic live under `include/`; Airflow-specific operators and executors live under `airflow_utils/`.

## Documentation

- [`CONTRIBUTING.md`](CONTRIBUTING.md): full contributor workflow, testing policy, SQL/DAG change guidance, and PR standards.
- [`AGENTS.md`](AGENTS.md): repository-specific AI coding agent conventions, validation requirements, and safe-edit expectations.

## Repository layout

| Path | Purpose |
|------|---------|
| `dags/` | NASCAR DAGs under `dags/nascar/scraper/` (`dag.py`, `sql/`, `racing_reference/`) |
| `include/` | Reusable libraries: NASCAR scrape sources, HTML→CSV conversion, file utilities |
| `airflow_utils/` | Custom Airflow operators/executors |
| `config/`, `plugins/` | Airflow configuration and plugins |
| `tests/unit`, `tests/integration` | Pytest suites (integration tests use fixture HTML, no live network) |
| `script/` | Developer entrypoints (shell wrappers); see below |
| `scraper-data/` | **Not in git** (see `.gitignore`). Docker mounts it to **`/home/airflow/scraper`**, so scrape output appears under **`scraper-data/nascar/`** (then hostname, series, year). Override the host path with **`AIRFLOW_SCRAPER_DATA_DIR`** in `.env` if needed. |

Python dependencies and versions are declared in `pyproject.toml` and locked with `uv` (`uv.lock`). The runtime image is defined in `Dockerfile`; local full-stack orchestration is in `docker-compose.yaml`.

## Prerequisites

- **Docker** and **Docker Compose** for the Airflow + database environment
- For **local** Python (tests, IDE, `uv sync`): **uv** and **pyenv** as expected by `script/setup`, plus a `.python-version` file at the repo root

On Linux, building the optional `mysqlclient` wheel needs MariaDB headers and a Python with development headers; `script/setup` can skip `mysqlclient` and rely on PyMySQL-compatible drivers when those are missing.

## Where to start

For first-time contributors:

1. Run `./script/setup` to prepare Python and dependencies.
2. Make your change in a focused branch.
3. Run `./script/format` and `./script/lint`.
4. Run tests with `uv run --group dev pytest` (or `./script/test` for containerized runs).
5. Open a PR with test evidence and any schema/import impact notes.

Use [`CONTRIBUTING.md`](CONTRIBUTING.md) for full policy and process details.

## Lint

After `script/setup` (or `uv sync --group dev`):

```bash
./script/lint
./script/lint --fix   # apply safe Ruff auto-fixes, then re-run to confirm
```

## Format

Apply the Ruff formatter (line breaks, quotes, etc.) to the same Python trees as lint:

```bash
./script/format
```

Use **`./script/lint`** afterward to ensure **`ruff check`** and **`format --check`** still pass. **`./script/format --diff`** previews changes without writing files.

## Tests

Run pytest on the host (after `uv sync --group dev`). Coverage is configured in **`pyproject.toml`**: **`include/`**, **`dags/`**, and **`airflow_utils/`** are measured; the run fails if line coverage is below **30%** (raise **`--cov-fail-under`** in **`[tool.pytest.ini_options]`** as the suite grows). Reports: terminal (with missed lines) and **`coverage.xml`**. Skip coverage with **`pytest --no-cov`**.

```bash
uv run --group dev pytest
```

Run tests inside the unit-test Docker image (same coverage rules via **`pyproject.toml`**):

```bash
./script/test                 # unit + integration
./script/test --unit
./script/test --integration
./script/test --no-cov        # faster, no coverage gate
```

Compose also defines a `unit-tests` service (profile `tests`) that builds the same test image.

## The `script/` folder

These scripts are thin wrappers run from the **repository root** (they `cd` to the parent of `script/`). They exist so day-to-day workflows stay consistent across machines.

| Script | Role |
|--------|------|
| `script/setup` | Ensures **uv** is available, installs the Python version from `.python-version` via **pyenv**, then runs **`uv sync`** for this project (with Linux-specific logic to skip `mysqlclient` when headers are missing). Use once per clone or when dependencies change. |
| `script/build` | Runs **`docker compose build`** (after `docker compose rm -f`). Pass through extra args to `docker compose build` if needed. |
| `script/server` | Tears down volumes, runs **`airflow-init`**, then brings the stack up with **`docker compose --profile development up`**. Adjust **`COMPOSE_FILES`** or profiles inside the script if your Compose file uses different service profiles. |
| `script/clear-db` | Removes local **`db/data/`** and **`db/test-data`**, and tries to remove old MariaDB-related Docker images. Uses **`sudo`** for filesystem cleanup; use only when you intend to wipe local DB state. |
| `script/test` | Builds **`Dockerfile.unit-tests`** and runs **pytest** in a container for **`tests/unit`**, **`tests/integration`**, or both. Supports **`--unit`**, **`--integration`**, and **`--help`**. Set **`UNIT_TESTS_IMAGE`** to override the image tag. |
| `script/lint` | Runs **[Ruff](https://docs.astral.sh/ruff/)** **`check`** then **`format --check`** on **`dags/`**, **`include/`**, **`airflow_utils/`**, and **`tests/`** (requires **`uv`** and dev deps from **`script/setup`**). Use **`./script/lint --fix`** to apply safe auto-fixes; **`./script/lint --help`** for usage. |
| `script/format` | Runs **`ruff format`** on the same paths (writes files). Extra args are passed to **`ruff format`** (e.g. **`--diff`**). See **`./script/format --help`**. |

When adding a new workflow, prefer a new executable under `script/` with a short header comment describing usage and assumptions (paths, profiles, env vars).
