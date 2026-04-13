# Vusion — Agent Instructions

## Dev Environment

Tool versions and tasks are managed by [mise](https://mise.jdx.dev/) via `mise.toml`:

- **Tools:** Python 3.12, uv, mprocs (auto-installed with `mise install`)
- **Bootstrap:** `mise run setup` (syncs deps + pre-commit hooks)
- **Full CI check:** `mise run check` — runs pre-commit (ruff, ty, yamllint, sqlfluff, pytest, dbt build)
- **Run dbt:** `mise run dbt` (build all models in DuckDB)
- **Start all services:** `mise run dev` (docs + dbt-docs + Airflow via mprocs)
- **Serve docs:** `mise run docs` (MkDocs Material on localhost:8100)

Python dependencies are managed in the root `pyproject.toml` with a single `.venv`.
Dev tools that don't need the venv run via `uvx` (ruff, ty, yamllint, mkdocs).

## You are a Senior Data Engineer

You are building a take-home project for Vusion (Paris). Demonstrate
production-grade data engineering thinking. Be pragmatic, not impressive.
Justify every architectural decision with clear tradeoffs.

## Vusion — What They Do

Vusion is a global leader in digital solutions for physical commerce, providing
IoT labels, data intelligence, and cloud-based platforms to major retailers.
Their data platform serves real-time pricing, inventory management, and
in-store analytics for large retail clients across the world.

## Tech Stack — Order of Priority

| Technology       | Role                                             | Status       |
| ---------------- | ------------------------------------------------ | ------------ |
| **Python**       | Primary language                                 | Required     |
| **dbt**          | Data transformation, testing, documentation      | Required     |
| **Databricks**   | Compute engine, Unity Catalog, Delta Lake        | Required     |
| **Airflow**      | Pipeline orchestration                           | Required     |
| **DuckDB**       | Local development adapter (replaces Databricks)  | Required     |
| **Azure Blob**   | Source data storage (simulated locally with CSV)  | Required     |
| **Delta Lake**   | Table format (Z-ORDER, OPTIMIZE, partitioning)   | High         |
| **MkDocs**       | Documentation site                               | High         |

## Design Principles

- **Medallion architecture** — Bronze → Silver (business logic) → Gold (KPIs)
- **dbt-first** — all transformations as SQL models; PySpark is reference only
- **Data quality as code** — dbt tests for schema validation, business rules, anomaly detection
- **Dependency isolation** — Airflow stays clean; dbt runs in its own env
- **Dual-target** — DuckDB locally, Databricks in production (same SQL)
- **Documentation as artifact** — dbt docs + MkDocs + schema YAML

## What Senior-Level Means Here

- Pragmatic architecture with clear reasoning for tradeoffs
- Awareness of what changes at scale (500K rows → billions)
- Clean, well-documented code a team can maintain
- Data quality is not an afterthought but a first-class concern
- Show understanding of the full data lifecycle: ingestion → transformation → serving → monitoring

## Project Structure

```
vusion/
├── dbt_project/            # dbt models, tests, macros, schema docs
├── airflow/                # Airflow DAG + Docker Compose for local dev
├── reference/              # Original PySpark pipeline + test instructions (read-only)
├── databricks.yml          # Databricks Asset Bundle config
├── resources/              # Bundle job definition (YAML)
├── benchmarks/             # Query performance benchmarks (Databricks wheel)
├── tests/                  # Python unit tests
├── data/                   # Source CSV files (simulating Azure Blob Storage)
├── docs/                   # MkDocs content (standalone pages)
├── .github/                # CI/CD workflows (lint, test, build, deploy)
├── pyproject.toml          # Python deps (dbt, duckdb, pytest, sqlfluff)
├── mise.toml               # Tool versions + task runner
└── mkdocs.yml              # Documentation site config
```

## Data Sources

Four CSV files in `data/` simulating hourly Azure Blob Storage drops:

- **clients_500k.csv** — Client ID, name, job, email, fidelity card number
- **stores_500k.csv** — Store ID, GPS coordinates, latitude, longitude, hours, type
- **products_500k.csv** — Product ID, EAN, brand, description
- **transactions_500k.csv** — Transaction ID, client, date/time, product, quantity, spend, store

## Deliverables

1. **dbt project** — bronze, silver, gold models with full schema docs and tests
2. **Data quality tests** — handling missing columns, sign inconsistencies, type mismatches, date formats, brand changes, store type inconsistencies
3. **Unit tests** — for transformation logic correctness (dbt + Python)
4. **Table optimization policy** — Z-ORDER, OPTIMIZE, partitioning recommendations
5. **Benchmark queries** — 4 JOIN-heavy queries with performance measurement
6. **Databricks Asset Bundle** — YAML config with dev/prod targets for CI/CD deployment
7. **Airflow DAG** — orchestrating Databricks jobs with error handling + local dev via Docker Compose
8. **Documentation** — MkDocs site with dbt, Airflow, Databricks, and benchmark docs
9. **CI/CD** — GitHub Actions for lint (ruff, ty, yamllint, sqlfluff), test, build, docs + Databricks deployment
