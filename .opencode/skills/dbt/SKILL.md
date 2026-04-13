---
name: dbt
description: dbt project — models, tests, macros, sources, and DuckDB local development
---

## Role

You are a Senior Data Engineer maintaining the dbt transformation layer.

## Overview

The dbt project (`dbt_project/`) implements a medallion architecture:
Bronze → Silver (business logic) → Gold (KPI datamarts).

Locally we use DuckDB; production targets Databricks with Unity Catalog.

## Rules

- All transformations are SQL models — no PySpark in the dbt project
- Models follow the medallion pattern: `bronze/` → `silver/` → `gold/`
- Every model has a corresponding YAML schema with column descriptions and tests
- Use `ref()` for model dependencies, `source()` for raw data on Databricks
- Bronze models use the `read_source` macro for dual-target data reading (DuckDB read_csv / Databricks source)
- DuckDB is the local adapter; SQL must be compatible with both DuckDB and Databricks SQL
- Data quality tests are first-class: `unique`, `not_null`, `relationships`, `accepted_values`, plus custom tests
- Unit tests validate transformation logic correctness
- Macros handle cross-adapter differences (DuckDB vs Databricks)
- SQL style: lowercase keywords, 4-space indentation, aligned column aliases, implicit table aliases

## Model Layers

| Layer | Prefix | Directory | Description |
|---|---|---|---|
| Bronze | `_bronze` | `models/bronze/` | 1:1 with source CSVs, type casting, column renaming, basic cleaning |
| Silver | none | `models/silver/` | Business logic: RFM scoring, customer segmentation, status |
| Gold | none | `models/gold/` | KPI datamarts: basket analysis, product trends, client counts |

## File Layout

```
dbt_project/
  dbt_project.yml                   # Project config
  profiles.yml                      # Connection profiles (DuckDB local, Databricks prod)
  models/
    bronze/
      _sources.yml                  # Source definitions for CSV files
      _bronze_models.yml            # Schema + tests for bronze models
      clients_bronze.sql
      stores_bronze.sql
      products_bronze.sql
      transactions_bronze.sql
    silver/
      _silver_models.yml            # Schema + tests for silver models
      _silver_unit_tests.yml        # Unit tests for RFM logic
      customer_silver.sql           # RFM scoring, segmentation, status
    gold/
      _gold_models.yml              # Schema + tests for gold models
      basket_analysis_per_store_gold.sql
      product_trend_per_store_gold.sql
      nb_clients_per_store_gold.sql
  tests/
    generic/                        # Custom generic tests (sign_consistency, positive_value, valid_date_range)
    assert_*.sql                    # Singular tests
  macros/                           # Shared macros (read_source, optimize_tables)
```

## Running Locally

All commands run from the **repo root** (not from `dbt_project/`):

```bash
mise run dbt          # Build models + run all tests
mise run dbt:run      # Models only
mise run dbt:test     # Tests only
mise run dbt:docs     # Generate and serve docs (localhost:8200)
```

Or directly:

```bash
uv run dbt build --project-dir dbt_project --profiles-dir dbt_project
```

## SQL Linting

SQL models are linted by sqlfluff (DuckDB dialect, dbt templater). Config in `.sqlfluff`.

```bash
uv run sqlfluff lint dbt_project/models/ dbt_project/tests/ --ignore parsing
```

## Key Conventions

- Source CSVs are read via the `read_source` macro in bronze models (dispatches to `read_csv()` on DuckDB, `{{ source() }}` on Databricks)
- Bronze models handle: missing columns, type casting, date normalization, sign consistency
- Silver models implement business logic translated from the original PySpark reference (`reference/pipeline/`)
- Gold models produce final KPI tables for analytics
- The PySpark reference has a bug in `reference/pipeline/gold_datamart_kpis.py:265` (wrong join column) — this is fixed in dbt
