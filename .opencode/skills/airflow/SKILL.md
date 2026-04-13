---
name: airflow
description: Pipeline orchestration — DAG definitions, Databricks operators, dbt job scheduling
---

## Role

You are a Senior Data Engineer maintaining pipeline orchestration.

## Overview

Orchestration layer (`airflow/`) for dbt pipelines on Databricks. Contains the
Dockerfile, Docker Compose for local dev, DAG definitions, and mock operators.
Airflow only submits and monitors Databricks jobs — no dbt/data dependencies.

## Rules

- Airflow is orchestration only — NO dbt or data dependencies in the Airflow image
- DAGs use Databricks operators (`DatabricksRunNowOperator`)
- Task groups organize related tasks (dbt_run, dbt_test, dbt_docs, optimize)
- Error handling: retries, timeout, failure notifications
- DAGs are Python files but not type-checked (Airflow deps not in local venv)
- Keep the Dockerfile minimal: just Airflow + Databricks provider

## DAGs

| DAG | Schedule | Description |
|---|---|---|
| `deep_rayon_dbt_pipeline` | `0 3 * * *` | Full dbt pipeline: run → test → docs → optimize |

## Task Flow

```
dbt_run → dbt_test → dbt_docs_generate → optimize_tables → notify
```

Each task submits a Databricks job and waits for completion.

## File Layout

```
airflow/
  Dockerfile                        # apache/airflow + databricks provider
  docker-compose.yml                # Local Airflow (airflow standalone, SQLite)
  dags/
    deep_rayon_dbt_pipeline.py          # Main orchestration DAG
  plugins/
    mock_databricks.py              # Mock Databricks operators for local dev
```

## Local Development

Local Airflow runs via Docker Compose using `airflow standalone` — single container,
SQLite database. Mock operators (`plugins/mock_databricks.py`) monkey-patch
`DatabricksRunNowOperator.execute` so tasks succeed without a real Databricks workspace.

```bash
mise run airflow          # Start (http://localhost:8080, admin/admin)
mise run airflow:down     # Stop
```

Or via mprocs alongside docs and dbt-docs:

```bash
mise run dev
```

## Key Principle

Airflow is orchestration-only. It submits Databricks jobs and monitors their
completion. No dbt commands run inside Airflow — they run on Databricks compute.
