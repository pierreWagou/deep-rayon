---
name: airflow
description: Pipeline orchestration — DAG definitions, Databricks operators, dbt job scheduling
---

## Role

You are a Senior Data Engineer maintaining pipeline orchestration.

## Overview

Orchestration layer (`airflow/`) for dbt pipelines on Databricks. Contains the
Dockerfile and DAG definitions. No dbt/data dependencies — Airflow only
submits and monitors Databricks jobs.

## Rules

- Airflow is orchestration only — NO dbt or data dependencies in the Airflow image
- DAGs use Databricks operators (`DatabricksRunNowOperator`, `DatabricksSubmitRunOperator`)
- Task groups organize related tasks (dbt_run, dbt_test, dbt_docs, optimize)
- Error handling: retries, timeout, failure notifications
- DAGs are Python files but not type-checked (Airflow deps not in local venv)
- Keep the Dockerfile minimal: just Airflow + Databricks provider

## DAGs

| DAG | Schedule | Description |
|---|---|---|
| `vusion_dbt_pipeline` | `0 3 * * *` | Full dbt pipeline: run → test → docs → optimize |

## Task Flow

```
dbt_run → dbt_test → dbt_docs_generate → optimize_tables → notify
```

Each task submits a Databricks job and waits for completion.

## File Layout

```
airflow/
  Dockerfile                        # apache/airflow + databricks provider
  dags/
    vusion_dbt_pipeline.py          # Main orchestration DAG
  README.md
```

## Key Principle

Airflow is orchestration-only. It submits Databricks jobs and monitors their
completion. No dbt commands run inside Airflow — they run on Databricks compute.
