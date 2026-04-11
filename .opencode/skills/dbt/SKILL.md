---
name: dbt
description: dbt project — models, tests, macros, sources, and DuckDB local development
---

## Role

You are a Senior Data Engineer maintaining the dbt transformation layer.

## Overview

The dbt project (`dbt_project/`) implements a medallion architecture:
Bronze (staging) → Silver (business logic) → Gold (KPI datamarts).

Locally we use DuckDB; production targets Databricks with Unity Catalog.

## Rules

- All transformations are SQL models — no PySpark in the dbt project
- Models follow the medallion pattern: `staging/` → `silver/` → `gold/`
- Every model has a corresponding YAML schema with column descriptions and tests
- Use `ref()` for model dependencies, `source()` for raw data
- DuckDB is the local adapter; SQL must be compatible with both DuckDB and Databricks SQL
- Data quality tests are first-class: `unique`, `not_null`, `relationships`, `accepted_values`, plus custom tests
- Unit tests validate transformation logic correctness
- Macros handle cross-adapter differences (DuckDB vs Databricks)

## Model Layers

| Layer | Prefix | Directory | Description |
|---|---|---|---|
| Staging (Bronze) | `stg_` | `models/staging/` | 1:1 with source CSVs, type casting, column renaming, basic cleaning |
| Silver | none | `models/silver/` | Business logic: RFM scoring, customer segmentation, status |
| Gold | none | `models/gold/` | KPI datamarts: basket analysis, product trends, client counts |

## File Layout

```
dbt_project/
  dbt_project.yml                   # Project config
  profiles.yml                      # Connection profiles (DuckDB local, Databricks prod)
  pyproject.toml                    # Python deps: dbt-duckdb, dbt-databricks
  packages.yml                      # dbt packages (dbt-utils, etc.)
  models/
    staging/
      _sources.yml                  # Source definitions for CSV files
      _stg_models.yml               # Schema + tests for staging models
      stg_clients.sql
      stg_stores.sql
      stg_products.sql
      stg_transactions.sql
    silver/
      _silver_models.yml            # Schema + tests for silver models
      customer_silver.sql           # RFM scoring, segmentation, status
    gold/
      _gold_models.yml              # Schema + tests for gold models
      basket_analysis_per_store.sql
      product_trend_per_store.sql
      nb_clients_per_store.sql
  tests/
    generic/                        # Custom generic tests
  macros/                           # Shared macros
  seeds/                            # Reference data (if needed)
```

## Running Locally

```bash
cd dbt_project
uv run dbt build          # Run models + tests
uv run dbt test           # Run tests only
uv run dbt docs generate  # Generate documentation
uv run dbt docs serve     # Serve docs locally
```

## Key Conventions

- Source CSVs are read via `dbt seed` or external table definitions
- Staging models handle: missing columns, type casting, date normalization, sign consistency
- Silver models implement business logic translated from the original PySpark reference
- Gold models produce final KPI tables for analytics
- The PySpark reference has a bug in `gold_datamart_kpis.py:265` (wrong join column) — this is fixed in dbt
