# Databricks Job Configuration

Databricks multi-task job definition for the Vusion dbt pipeline. Defined as JSON for version control and deployment via the Databricks Jobs API or Terraform.

## Job Structure

**Job name:** `vusion_dbt_pipeline`

```mermaid
flowchart LR
    run[dbt_run] --> test[dbt_test] --> docs[dbt_docs_generate] --> optimize[optimize_tables]
```

### Tasks

| Task | Command | Depends On | Timeout | Retries |
|------|---------|------------|---------|---------|
| `dbt_run` | `dbt run --target prod` | -- | 1 hour | 2 (60s interval) |
| `dbt_test` | `dbt test --target prod` | `dbt_run` | 30 min | 1 (30s interval) |
| `dbt_docs_generate` | `dbt docs generate --target prod` | `dbt_test` | 15 min | 1 (30s interval) |
| `optimize_tables` | OPTIMIZE + Z-ORDER on Delta tables | `dbt_docs_generate` | 30 min | 1 (60s interval) |

Each task runs a notebook at `/Repos/vusion/dbt_project/databricks/` that dispatches the appropriate dbt or SQL command based on `base_parameters`.

### Why This Order

1. **dbt_run** builds all models. Must complete before testing.
2. **dbt_test** validates data quality. Runs after models are materialized so tests query real data.
3. **dbt_docs_generate** creates the documentation catalog. Runs after tests pass to ensure docs reflect a healthy state.
4. **optimize_tables** compacts files and applies Z-ORDER. Runs last because it benefits from the final table state and should not interfere with model execution.

## Cluster Configuration

### Ephemeral Cluster (dbt_run)

The `dbt_run` task uses an ephemeral cluster (`new_cluster`) for isolation:

```json
{
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "num_workers": 2,
  "spark_conf": {
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true"
  },
  "azure_attributes": {
    "first_on_demand": 1,
    "availability": "ON_DEMAND_AZURE"
  }
}
```

- **Spark version 14.3**: Databricks Runtime with Delta Lake optimizations.
- **2 workers + 1 driver** (Standard_DS3_v2): Sized for 500K-row datasets. Scale `num_workers` for larger production volumes.
- **optimizeWrite + autoCompact**: Reduces small-file problems during writes without manual OPTIMIZE.
- **first_on_demand: 1**: The driver node uses on-demand pricing for reliability; workers can use spot instances.

### Shared Cluster (dbt_test, docs, optimize)

The remaining tasks reuse an existing cluster (`existing_cluster_id: ${var.shared_cluster_id}`) to avoid cluster startup latency. These tasks are lightweight and do not require dedicated compute.

## Retry Policy

| Task | Retries | Min Interval | Retry on Timeout |
|------|---------|-------------|------------------|
| `dbt_run` | 2 | 60 seconds | Yes |
| `dbt_test` | 1 | 30 seconds | No |
| `dbt_docs_generate` | 1 | 30 seconds | No |
| `optimize_tables` | 1 | 60 seconds | Yes |

**Rationale:**

- `dbt_run` gets 2 retries because model execution can fail due to transient cluster or storage issues. Retrying on timeout is enabled since long-running queries may hit temporary resource limits.
- `dbt_test` gets 1 retry with no timeout retry. If tests fail, the data quality issue is likely real, not transient.
- `optimize_tables` retries on timeout because OPTIMIZE can take longer than expected on large tables with many small files.

## Schedule

```
Cron: 0 0 3 * * ?     (daily at 03:00)
Timezone: Europe/Paris
Max concurrent runs: 1
Global timeout: 2 hours
```

The 03:00 schedule aligns with the Airflow DAG (`vusion_dbt_pipeline`) to ensure orchestration and job configuration are consistent. Notification on failure is sent to `data-engineering@vusion.com`.

## Deployment

The `job_config.json` can be deployed via:

```bash
# Databricks CLI
databricks jobs create --json @databricks/job_config.json

# Or update an existing job
databricks jobs reset --job-id <JOB_ID> --json @databricks/job_config.json
```

The `${var.shared_cluster_id}` placeholder should be replaced with the actual cluster ID during deployment, either through templating or Terraform variables.
