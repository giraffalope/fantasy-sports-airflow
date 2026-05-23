# `fantasy-sports-airflow` documentation

Reference docs for the Airflow 3 project that orchestrates NASCAR scraping,
import, aggregation, and machine-learning workflows.

- Per-DAG documentation lives under [`nascar/`](./nascar/README.md). Start
  there to understand how DAGs are wired together, what each task does, and
  which configuration knobs and tables it touches.
- Repository-wide process notes (importer staging contract, contributor
  workflow) live in [`../CONTRIBUTING.md`](../CONTRIBUTING.md).
- AI agent guardrails for this repo live in [`../AGENTS.md`](../AGENTS.md).
