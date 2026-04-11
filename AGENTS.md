# Vusion — Agent Instructions

Read the README.md files in each sub-project for detailed context.

## Dev Environment

Tool versions and tasks are managed by [mise](https://mise.jdx.dev/) via `mise.toml`:

- **Tools:** Python 3.12, uv, mprocs (auto-installed with `mise install`)
- **Bootstrap:** `mise run setup` (syncs all sub-project deps + pre-commit hooks)
- **Full CI check:** `mise run check` — runs pre-commit (ruff, dbt build, pytest)
- **Run dbt:** `mise run dbt` (build all models in DuckDB)
- **Serve docs:** `mise run docs` (MkDocs Material on localhost:8100)

The dbt sub-project has its own `.venv`, `uv.lock`, `pyproject.toml`.
No root venv — dev tools run via `uvx`.

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

- **Medallion architecture** — Bronze (staging) → Silver (business logic) → Gold (KPIs)
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
├── data/                   # Source CSV files (simulating Azure Blob Storage)
├── dbt_project/            # dbt models, tests, macros, schema docs
├── airflow/                # Airflow DAGs for Databricks job orchestration
├── databricks/             # Databricks job configuration (JSON)
├── benchmarks/             # Query performance benchmarks
├── docs/                   # MkDocs content (symlinks to sub-project READMEs)

```

## Data Sources

Four CSV files in `data/` simulating hourly Azure Blob Storage drops:

- **clients_500k.csv** — Client ID, name, job, email, fidelity card number
- **stores_500k.csv** — Store ID, GPS coordinates, hours, type
- **products_500k.csv** — Product ID, EAN, brand, description
- **transactions_500k.csv** — Transaction ID, client, date/time, product, quantity, spend, store

## Deliverables

1. **dbt project** — staging, silver, gold models with full schema docs and tests
2. **Data quality tests** — handling missing columns, sign inconsistencies, type mismatches, date formats, brand changes, store type inconsistencies
3. **Unit tests** — for transformation logic correctness
4. **Table optimization policy** — Z-ORDER, OPTIMIZE, partitioning recommendations
5. **Benchmark queries** — 3+ JOIN-heavy queries with performance measurement
6. **Databricks job config** — JSON with dbt_run, dbt_test, dbt_docs_generate, optimize_tables
7. **Airflow DAG** — orchestrating Databricks jobs with error handling
8. **Documentation** — MkDocs site + README with optimization summary
9. **CI/CD** — GitHub Actions for lint, test, build, docs deployment
