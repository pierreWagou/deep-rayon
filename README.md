<div align="center">

![header](https://capsule-render.vercel.app/api?type=waving&height=220&color=0:cba6f7,25:b4befe,50:89dceb,75:f5c2e7,100:f38ba8&text=Deep-Rayon&fontSize=60&fontColor=11111b&desc=from%20aisles%20to%20insights&descSize=18&descAlignY=62&descAlign=50&fontAlignY=38&animation=fadeIn&fontAlign=50)

[![CI](https://github.com/pierreWagou/vusion/actions/workflows/ci.yml/badge.svg)](https://github.com/pierreWagou/vusion/actions/workflows/ci.yml)
[![CD](https://github.com/pierreWagou/vusion/actions/workflows/cd.yml/badge.svg)](https://github.com/pierreWagou/vusion/actions/workflows/cd.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg?logo=python&logoColor=white)
![dbt 1.11](https://img.shields.io/badge/dbt-1.11-FF694B.svg)
![DuckDB](https://img.shields.io/badge/DuckDB-local%20dev-FEF000.svg?logo=duckdb)
![Databricks](https://img.shields.io/badge/Databricks-production-FF3621.svg?logo=databricks)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Production-grade retail data pipeline for [Vusion](https://www.vusion.com/) — migrates a PySpark ETL to **dbt + Databricks** with a medallion architecture.

</div>

---

## Overview

- **Medallion architecture** — Bronze (raw views) / Silver (business logic) / Gold (store KPIs)
- **dbt-first** — all transformations as SQL models; PySpark is reference only
- **Dual-target** — same SQL runs on DuckDB locally and Databricks in production
- **94 data tests + 14 unit tests** — data quality is a first-class concern
- **Airflow orchestration** — submits Databricks jobs, never runs dbt directly
- **Full CI/CD** — lint, test, build, deploy via GitHub Actions + Databricks Asset Bundles

## Architecture

```
 ┌──────────────────────────────────────────────────────────────────┐
 │                        Deep-Rayon Pipeline                       │
 ├─────────────┬─────────────┬──────────────────┬──────────────────┤
 │   Bronze    │   Silver    │       Gold       │    Serving       │
 │             │             │                  │                  │
 │  clients    │  customer   │  basket_analysis │                  │   dbt models
 │  stores     │  (RFM,      │  product_trend   │  Dashboards      │
 │  products   │   segments, │  nb_clients      │  & BI tools      │
 │  transactions  lifecycle) │  (per store)     │                  │
 ├─────────────┼─────────────┼──────────────────┼──────────────────┤
 │   views     │   tables    │     tables       │                  │   materialization
 └─────────────┴─────────────┴──────────────────┴──────────────────┘
       ▲                                                ▲
       │  CSV sources (Azure Blob)              Databricks SQL
       │  4 files, 500K rows each               Warehouse + Photon
```

| Layer | Models | Purpose |
|-------|--------|---------|
| **Bronze** | `clients`, `stores`, `products`, `transactions` | 1:1 with CSVs. Type casting, sign correction, normalization. |
| **Silver** | `customer` | RFM scoring (1-5), 8 segments, lifecycle stage, store loyalty. |
| **Gold** | `basket_analysis`, `product_trend`, `nb_clients` | Store-level KPIs for dashboards (all per-store). |

## Getting Started

```bash
mise install          # Python 3.12, uv, mprocs
mise run setup        # Sync deps + pre-commit hooks
mise run dbt          # Build all models + run tests (DuckDB)
```

| Service | URL | Command |
|---------|-----|---------|
| MkDocs | [localhost:8100](http://localhost:8100) | `mise run docs` |
| dbt docs | [localhost:8200](http://localhost:8200) | `mise run dbt:docs` |
| Airflow | [localhost:8080](http://localhost:8080) | `mise run airflow` |

Start everything at once with `mise run dev` (runs all three via mprocs).

## Data Quality

Six known issues in the source data, all handled in the bronze layer with test coverage:

| Issue | Handling |
|-------|----------|
| Missing columns | Default values applied |
| Inconsistent casing | Normalized to lowercase |
| Multiple date formats | Parsed via target-aware macro |
| Sign inconsistencies | Absolute value correction on quantities/spend |
| Brand naming variants | Mapped via seed file (`brand_mapping.csv`) |
| Lat/lng parsing | Cast with error handling |

## Structure

```
deep-rayon/
├── dbt_project/        # Models, tests, macros, schema docs
├── airflow/            # DAG + Docker Compose for local dev
├── benchmarks/         # 4 JOIN-heavy queries (Databricks wheel)
├── data/               # Source CSV files (500K rows each)
├── docs/               # MkDocs site content
├── resources/          # Databricks bundle job definitions
├── tests/              # Python unit tests
└── .github/            # CI/CD workflows
```

<details>
<summary>dbt project details</summary>

```
dbt_project/
├── models/
│   ├── bronze/         # 4 source views + schema YAML
│   ├── silver/         # customer table + unit tests
│   └── gold/           # 3 store KPI tables + unit tests
├── tests/
│   ├── generic/        # 3 reusable test definitions
│   └── *.sql           # 8 singular data tests
├── macros/             # 5 macros (date parsing, source reading, optimization)
└── seeds/              # brand_mapping.csv
```

</details>

## Databricks Deployment

```bash
# Upload source files (one-time)
databricks fs cp data/clients_500k.csv dbfs:/FileStore/data/clients_500k.csv
databricks fs cp data/stores_500k.csv dbfs:/FileStore/data/stores_500k.csv
databricks fs cp data/products_500k.csv dbfs:/FileStore/data/products_500k.csv
databricks fs cp data/transactions_500k.csv dbfs:/FileStore/data/transactions_500k.csv

# Build wheel + deploy bundle
mise run bundle:deploy

# Trigger the pipeline
databricks bundle run deep_rayon_dbt_pipeline
```

Configuration is managed through `.env` (loaded by mise automatically):

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `BUNDLE_VAR_warehouse_id` | SQL warehouse ID for dbt tasks |
| `BUNDLE_VAR_data_path` | Path to source CSVs on Databricks |
| `DATABRICKS_HOST` | Workspace URL |
| `DATABRICKS_TOKEN` | PAT or service principal token |

<details>
<summary>Table optimization policy</summary>

| Table | Strategy | Z-ORDER Columns | Partitioning |
|-------|----------|-----------------|--------------|
| `transactions` | OPTIMIZE | -- | `transaction_date` at scale |
| `customer` | OPTIMIZE + Z-ORDER | `client_id`, `rfm_segment`, `customer_status` | -- |
| `basket_analysis` | OPTIMIZE + Z-ORDER | `store_id`, `store_type` | -- |
| `product_trend` | OPTIMIZE + Z-ORDER | `store_id`, `product_id`, `trend_direction` | -- |
| `nb_clients` | OPTIMIZE + Z-ORDER | `store_id`, `store_type` | -- |

</details>

## CI/CD

| Pipeline | Trigger | Steps |
|----------|---------|-------|
| **CI** | Push / PR to `main` | ruff, yamllint, sqlfluff, ty, pytest, dbt build, mkdocs build, bundle validate |
| **CD** | Merge to `main` | MkDocs deploy (GitHub Pages), Databricks bundle deploy (prod) |

## Documentation

Full docs at **[pierreWagou.github.io/vusion](https://pierreWagou.github.io/vusion/)** or locally via `mise run docs`.

## Quick Reference

| Action | Command |
|--------|---------|
| Bootstrap | `mise run setup` |
| Build dbt models | `mise run dbt` |
| Run dbt tests only | `mise run dbt:test` |
| Run Python tests | `mise run test` |
| Full CI check | `mise run check` |
| Start all services | `mise run dev` |
| Deploy to Databricks | `mise run bundle:deploy` |

## License

[MIT](LICENSE) -- Pierre Romon, 2026
