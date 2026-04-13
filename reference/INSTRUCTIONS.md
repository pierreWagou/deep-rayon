
# Platform Data Engineer Test

> **Note**: This test simulates a production environment where data arrives from Azure Blob Storage. For the purposes of this test, the data files are provided locally in the `data/` folder. Your solution should be designed to work with Azure Blob Storage in production, but can read from local files during testing.

## Description
We recently onboarded a **large retail client** who is experiencing **major query latency issues**. The goal is to:
- improve **query performance**,
- reduce **compute and storage system costs**,
- validate ingestion and transformation logic on a realistic dataset, and
- improve **data quality and comprehension** using dbt catalog and lineage.

## Current State & Migration to dbt

**Production Environment**: The current codebase uses **external tables in Databricks** that are transformed using **PySpark notebooks**. These notebooks read data from Azure Blob Storage and create external tables in the Databricks catalog.

**For This Test**: The existing PySpark notebooks in the `pipeline/` folder demonstrate the current approach with managed tables in Databricks. They read from local CSV files in the `data/` folder (simulating Azure Blob Storage access) and perform transformations.

**Migration Goal**: To improve data quality, documentation, and maintainability, we need to migrate this to a dbt-based approach. Your dbt models should be designed to work with Azure Blob Storage in production, but for testing, they can read from the local `data/` folder.

### Required Migration Steps

1. **Translate PySpark notebooks to dbt models**: Convert the existing PySpark notebook transformations (located in `pipeline/`) to dbt models that create tables directly in the Databricks catalog (Unity Catalog). The dbt models should read from Azure Blob Storage in production, but for this test, they can read from the local `data/` folder.

2. **Include dbt schema and catalog for documentation**: Set up dbt schema.yml files and leverage dbt's catalog functionality to generate comprehensive documentation, including column descriptions, data types, and relationships.

3. **Data quality dbt files**: Implement dbt data quality tests using dbt's built-in testing framework (e.g., `unique`, `not_null`, `relationships`, `accepted_values`) and custom tests to validate data integrity and business rules.

   **Common Data Quality Issues**: In production, data is manually deposited into Azure Blob Storage. Several data quality problems frequently occur that must be detected and handled:
   
   - **Missing columns**: Some CSV files may be missing expected columns (e.g., `account_id` missing from clients, `latitude`/`longitude` missing from stores), causing schema evolution issues and null values in downstream transformations.
   
   - **Inconsistent sign conventions**: In transactions, `quantity` and `total_spend` may have inconsistent signs—positive quantity and negative spend or vice versa. The signs should always be in the same direction, signaling a purchase if positive and a return if negative.
   
   - **Data type inconsistencies**: Columns may change data types between file drops (e.g., `product_id` switching between integer and string formats, `store_id` appearing as numeric in some files and alphanumeric in others), causing type casting errors during ingestion.
   
   - **Date format variations**: Date columns (particularly in transactions) may appear in different formats across files (e.g., `YYYY-MM-DD`, `DD/MM/YYYY`, `MM-DD-YYYY`, or with timestamps), making temporal analysis unreliable without proper standardization.
   
   - **Product brand changes**: Product brand names may change between integrations (e.g., "Acme Corp" vs "Acme Corporation", or brand mergers resulting in name changes), causing fragmentation in brand-level analytics and requiring brand mapping logic.
   
   - **Store type inconsistencies**: Store type values may vary between files (e.g., "hyper", "Hyper", "HYPER", or new types like "supermarket" introduced without notice), breaking categorical aggregations and store type-based analysis.

4. **dbt unit tests**: Create unit tests for individual dbt models to ensure transformation logic correctness and catch regressions early in the development cycle.

5. **Policy for table optimizations for the Databricks catalog**: Define and implement a policy for table optimizations within the dbt project to ensure optimal query performance in the Databricks catalog.

## Accessing the Data

**Production Environment**: In a production environment, data arrives as hourly CSV drops into an **Azure Storage Account (Blob)**. You would receive:
- connection string
- container name
- file structure and ingestion details
- temporary read‑only access to read CSV files directly from Blob storage

**For This Test**: For the purposes of this test, the data files are located in the local `data/` folder:
- `data/clients_500k.csv`
- `data/stores_500k.csv`
- `data/products_500k.csv`
- `data/transactions_500k.csv`

You should read these CSV files from the local `data/` directory. Your solution should be designed to work with Azure Blob Storage in production (using connection strings and blob paths), but for testing purposes, you can read directly from the local files.

### Running the PySpark pipeline locally (`pipeline/`)

With PySpark installed, you can run the silver and gold scripts against the CSVs in `data/` (no Azure):

```bash
cd pipeline
python silver_customer_layer.py --local
python gold_datamart_kpis.py --local
```

Optional flags:

- `--data-dir /path/to/data` — folder containing `*_500k.csv` (default: `../data` relative to `pipeline/`).
- `--output-dir /path/to/out` — override output root (defaults: `output/silver`, `output/gold` under the repo root).

Or run both in sequence:

```bash
python airflow_dag_silver_gold.py --local
python airflow_dag_silver_gold.py --local --skip-silver   # gold only
```

Set `SPARK_MASTER` if you need a non-default Spark master (default in local mode: `local[*]`).

Log4j2 may print `Reconfiguration failed... at 'null'` if the JVM starts without a config. This repo sets `JAVA_TOOL_OPTIONS` before PySpark loads and ships `pipeline/spark_conf/log4j2.xml` to fix that. You may see one line `Picked up JAVA_TOOL_OPTIONS:` from the JVM—that is normal.

## Data Dictionary

### Clients

| Column      | Description                  |
|-------------|-----------------------------|
| `id`        | Client ID                    |
| `name`      | Name of the client           |
| `job`       | Job of the client            |
| `email`     | Email address of the client  |
| `account_id`| The number of the fidelity card |

### Stores

| Column      | Description                    |
|-------------|-------------------------------|
| `id`        | Store ID                      |
| `latlng`    | GPS coordinates of the Store  |
| `opening`   | Opening hour                  |
| `closing`   | Closing hour                  |
| `type`      | Type of the store             |
| `latitude`  | Latitude (if split from `latlng`) |
| `longitude` | Longitude (if split from `latlng`) |

### Products

| Column      | Description                  |
|-------------|-----------------------------|
| `id`        | Product ID                  |
| `ean`       | EAN product number          |
| `brand`     | Name of the brand of the product      |
| `description`| A description of the product   |

### Transactions

| Column         | Description                  |
|----------------|-----------------------------|
| `transaction_id`| Transaction ID             |
| `client_id`    | Client ID                   |
| `date`         | The date of the transaction |
| `hour`         | The hour of the transaction |
| `minute`       | The minute of the transaction |
| `product_id`   | Product ID                  |
| `quantity`     | The quantity bought         |
| `spend`        | The total spend amount for the transaction |
| `store_id`     | Store ID                    |


## Target Location for New Tables

**Production Environment**: In production, tables would be written to Databricks Unity Catalog at the following paths:
```
<catalog>.<schema>.clients
<catalog>.<schema>.stores
<catalog>.<schema>.products
<catalog>.<schema>.transactions
```

**For This Test**: You should design your dbt models to write to these catalog paths. If you're testing locally without Databricks, you can use a local database or file-based storage, but your models should be structured to work with Databricks Unity Catalog in production.

## Required Tables
- **clients**: id, name, job, email, account_id
- **stores**: id, latlng, opening, closing, type, latitude, longitude
- **products**: id, ean, brand, description
- **transactions**: transaction_id, client_id, date, hour, minute, product_id, quantity, spend, store_id

## Data Transformation & Optimization
You must propose the **best ways to optimize queries**. This can be through query optimization, table optimization or infrastructure optimization.

### Benchmark Queries
Propose at least **three JOIN‑heavy queries** anda replayable method to measure:
- **Query duration** (ms)
- **Files scanned** (via inputFiles or execution plan)
- [Bonus] **Compute cost estimate** (based on runtime × cluster size × DBU rate)

Example queries:
```
Q1 — Product sales by day
Q2 — Store-level revenue
Q3 — Client basket metrics
```

## Orchestration & Automation

### Databricks Jobs for dbt Execution

**Production Environment**: In production, you would create Databricks jobs to execute the dbt transformations. The job configuration should include multiple tasks:

1. **dbt_run**: Executes dbt models and transformations
2. **dbt_test**: Runs dbt data quality tests
3. **dbt_docs_generate**: Generates dbt documentation and catalog
4. **optimize_tables**: Optimizes Delta tables in Databricks catalog

**For This Test**: You should create a Databricks job configuration file (JSON format) that defines these tasks with proper dependencies, retries, and error handling. The job should reference a Databricks notebook that executes dbt commands with proper error handling and logging. Even if you're testing locally, the job configuration should be production-ready and work when deployed to Databricks.

### Airflow Orchestration

**Production Environment**: In production, you would create an Airflow DAG to orchestrate the Databricks jobs, ensuring proper execution order, error handling, and monitoring.

**For This Test**: You must create an Airflow DAG that orchestrates the Databricks jobs. The DAG should include:

- **Task Groups**: Organized execution of dbt run, test, and documentation generation
- **Sensors**: Wait for Databricks job completion before proceeding
- **Error Handling**: Retries and notifications on failure
- **Table Optimization**: Final step to optimize Delta tables

The Airflow DAG should use the Databricks operators to submit jobs and sensors to monitor their completion. The DAG should be production-ready and work when deployed to an Airflow environment connected to Databricks.


## Deliverables

- **dbt project** with:
  - dbt models translating the PySpark notebook transformations (from `pipeline/` folder)
  - dbt schema.yml files with comprehensive documentation
  - Data quality tests (dbt tests) addressing the data quality issues mentioned above
  - dbt unit tests for transformation logic
  - Policy and implementation for table optimizations in Databricks catalog
  - Models should be designed to read from Azure Blob Storage in production, but work with local `data/` folder for testing
- Replayable tests to improve the metrics for join-heavy queries (and others if relevant)
- Summary of optimizations applied, the reasoning and their impact in ReadMe (IMPORTANT)
- **Databricks job configuration** (JSON format) to run dbt transformations with proper task dependencies
- **Airflow DAG** (Python) to orchestrate the Databricks jobs with error handling and monitoring
- [Bonus] CI/CD yml with tests

**File Structure**: Your solution should be organized in a way that clearly separates:
- Source data location (`data/` folder for this test)
- dbt project structure
- Databricks job configurations
- Airflow DAGs
- Documentation and README files
