# ⚗️ Hyper-Rayon

> Turning messy retail data into reliable store intelligence

[![CI](https://github.com/pierreWagou/vusion/actions/workflows/ci.yml/badge.svg)](https://github.com/pierreWagou/vusion/actions/workflows/ci.yml)
[![CD](https://github.com/pierreWagou/vusion/actions/workflows/cd.yml/badge.svg)](https://github.com/pierreWagou/vusion/actions/workflows/cd.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![dbt](https://img.shields.io/badge/dbt-1.11-FF694B.svg?logo=dbt)](https://docs.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-local%20dev-FEF000.svg?logo=duckdb)](https://duckdb.org/)
[![Databricks](https://img.shields.io/badge/Databricks-production-FF3621.svg?logo=databricks)](https://www.databricks.com/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Production-grade retail data pipeline — take-home test for [Vusion](https://www.vusion.com/). Migrates a PySpark ETL to **dbt + Databricks** with a medallion architecture (Bronze / Silver / Gold).

## Quick Start

```bash
mise install          # Python 3.12, uv, mprocs
mise run setup        # Sync deps + pre-commit hooks
mise run dbt          # Build all models + run tests (DuckDB)
mise run dev          # Start docs + dbt-docs + Airflow (mprocs)
```

| Service | URL | Command |
|---------|-----|---------|
| MkDocs | [localhost:8100](http://localhost:8100) | `mise run docs` |
| dbt docs | [localhost:8200](http://localhost:8200) | `mise run dbt:docs` |
| Airflow | [localhost:8080](http://localhost:8080) (admin/admin) | `mise run airflow` |

## Architecture

```
CSV sources  →  Bronze (views)  →  Silver (tables)  →  Gold (tables)
4 files          *_bronze            customer_silver     basket_analysis
500K rows        type casting        RFM scoring         product_trend
                 sign correction     segmentation        nb_clients
                 normalization       lifecycle
```

| Layer | Models | Purpose |
|-------|--------|---------|
| **Bronze** | `clients_bronze`, `stores_bronze`, `products_bronze`, `transactions_bronze` | 1:1 with CSVs. Type casting, sign correction, normalization. |
| **Silver** | `customer_silver` | RFM scoring (1-5), 8 segments, lifecycle, store loyalty. |
| **Gold** | `basket_analysis_per_store_gold`, `product_trend_per_store_gold`, `nb_clients_per_store_gold` | Store-level KPIs for dashboards. |

## Key Design Decisions

- **dbt-first** — all transformations are SQL models; PySpark is reference only
- **Dual-target** — same SQL runs on DuckDB (dev) and Databricks (prod); Databricks-specific features wrapped in target-aware macros
- **Data quality as code** — 55 data tests (45 schema, 7 generic, 3 singular), 3 unit tests. Six known data issues handled in the bronze layer.
- **Orchestration-only Airflow** — submits Databricks jobs, never runs dbt itself. Local dev uses mock operators via Docker Compose.
- **Databricks Asset Bundle** — declarative YAML with dev/prod targets, deployed via CI/CD
- **Bug fix** — corrected a join error in the original PySpark (`store_id == product_id` → `store_id == stores.id`)

## Data Quality

Six issues handled in the bronze layer: missing columns, inconsistent casing, multiple date formats, sign inconsistencies, brand naming, lat/lng parsing. All documented with test coverage.

## Optimizations Applied

### Table Optimization (Databricks)

| Table | Strategy | Z-ORDER Columns | Partitioning | Rationale |
|-------|----------|----------------|--------------|-----------|
| `transactions_bronze` | OPTIMIZE | *(none)* | `transaction_date` at scale | Largest table (500K→billions); compact small files from hourly ingestion |
| `customer_silver` | OPTIMIZE + Z-ORDER | `client_id`, `rfm_segment`, `customer_status` | None (500K rows) | Filter/join on client_id; segment-based dashboards |
| `basket_analysis_per_store_gold` | OPTIMIZE + Z-ORDER | `store_id`, `store_type` | None (aggregated) | Store-level queries always filter by store_id/type |
| `product_trend_per_store_gold` | OPTIMIZE + Z-ORDER | `store_id`, `product_id`, `trend_direction` | None (aggregated) | Multi-column filters in product analysis |
| `nb_clients_per_store_gold` | OPTIMIZE + Z-ORDER | `store_id`, `store_type` | None (aggregated) | Same access pattern as basket analysis |

### Query Optimization

- **Bronze as views** — staging models are views (zero storage cost), only materialized when queried downstream. This avoids duplicating 500K-row tables while keeping SQL clean.
- **Pre-aggregation in gold** — gold models aggregate at the store level, reducing billion-row scans to thousand-row lookups for dashboards.
- **CTE-based design** — each model uses CTEs instead of nested subqueries, enabling the query planner to optimize independently.

### Infrastructure Optimization (Databricks)

- **Single-node ephemeral cluster** for dbt CLI (no shuffle overhead for SQL pushdown)
- **SQL Warehouse** for actual query execution (auto-scaling, Photon engine)
- **OPTIMIZE after each build** — compacts small files from Delta write operations

### Impact

Benchmarks measure 4 JOIN-heavy queries on DuckDB (500K rows). On Databricks with Delta Lake optimizations:

| Optimization | Expected Impact |
|---|---|
| Z-ORDER on join columns | 3-10x fewer files scanned for filtered queries |
| OPTIMIZE (file compaction) | Eliminates small-file overhead from streaming/batch writes |
| Partition pruning (transactions) | Skips entire date partitions for time-range queries |
| Photon engine | 2-5x faster on aggregation-heavy gold queries |

Benchmarks run on Databricks as a separate job. Deploy with `mise run bundle:deploy`, then trigger with `databricks bundle run vusion_benchmark`.

## CI/CD

| Pipeline | Trigger | Steps |
|----------|---------|-------|
| **CI** | PR to main | Ruff lint → dbt build → benchmarks → MkDocs build → bundle validate |
| **CD** | Merge to main | MkDocs deploy (GitHub Pages) → Databricks bundle deploy (prod) |

## Project Structure

```
vusion/
├── dbt_project/           # dbt models, tests, macros, schema docs
├── airflow/               # Airflow DAG + Docker Compose for local dev
├── reference/             # Original PySpark pipeline + test instructions
├── databricks.yml         # Databricks Asset Bundle config
├── resources/             # Bundle job definition
├── benchmarks/            # Query performance benchmarks (pytest + Databricks wheel)
├── data/                  # Source CSV files (500K rows each)
├── docs/                  # MkDocs site content
├── .github/               # CI/CD workflows (lint, test, build, deploy)
├── pyproject.toml         # Python deps (dbt, duckdb, pytest)
├── mise.toml              # Tool versions + task runner
└── mkdocs.yml             # Documentation site config
```

## Documentation

Full documentation is available at the [MkDocs site](https://pierreWagou.github.io/vusion/) or locally via `mise run docs`:

- **dbt Project** — model layers, data quality tests, unit tests, optimization policy
- **Airflow** — DAG design, retry policy, local development with mock operators
- **Databricks** — asset bundle, targets, CI/CD deployment
- **Benchmarks** — 4 JOIN-heavy queries with performance baselines
