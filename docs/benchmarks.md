# Benchmarks

Query performance benchmarks for the Vusion dbt project. Measures execution time, files scanned, and estimated cost for JOIN-heavy analytical queries. Runs on DuckDB locally and on Databricks as a deployed Python wheel.

## Benchmark Queries

### Q1: Product Sales by Day

**Join complexity:** 3-way (transactions x products x stores)

Aggregates daily sales by brand and store type. Tests the performance of the most common analytical pattern: filtering and grouping across the fact table with two dimension joins.

```sql
SELECT t.transaction_date, p.brand, s.store_type, ...
FROM transactions t
JOIN products p ON t.product_id = p.product_id
JOIN stores s ON t.store_id = s.store_id
GROUP BY t.transaction_date, p.brand, s.store_type
ORDER BY t.transaction_date DESC, total_spend DESC
```

### Q2: Store-Level Revenue

**Join complexity:** 2-way (transactions x stores)

Computes revenue, unique clients, and average transaction value per store. Tests aggregation performance with fewer joins but heavier GROUP BY computations.

```sql
SELECT s.store_id, s.store_type, ...,
       SUM(t.spend) / NULLIF(COUNT(DISTINCT t.client_id), 0) AS revenue_per_client
FROM transactions t
JOIN stores s ON t.store_id = s.store_id
GROUP BY s.store_id, s.store_type, s.latitude, s.longitude
ORDER BY total_revenue DESC
```

### Q3: Client Basket Metrics

**Join complexity:** 4-way with CTE (transactions -> baskets CTE x clients x stores)

Builds per-transaction baskets, then computes client-level basket metrics by store type. The most complex query: a CTE followed by multi-table joins and HAVING filters.

```sql
WITH baskets AS (
    SELECT client_id, transaction_id, store_id, ...
    FROM transactions t
    GROUP BY client_id, transaction_id, store_id, transaction_date
)
SELECT c.client_id, c.name, s.store_type, ...
FROM baskets b
JOIN clients c ON b.client_id = c.client_id
JOIN stores s ON b.store_id = s.store_id
GROUP BY c.client_id, c.name, c.job, s.store_type
HAVING COUNT(DISTINCT b.transaction_id) >= 2
ORDER BY avg_basket_value DESC
LIMIT 1000
```

### Q4: RFM Segment Distribution by Store Type

**Join complexity:** Aggregation on silver layer (no joins)

Groups the pre-computed `customer` table by RFM segment, primary store type, and customer status. Tests read performance on the silver layer and validates that Z-ORDER on `rfm_segment` and `customer_status` would improve scan efficiency.

```sql
SELECT cs.rfm_segment, cs.primary_store_type, cs.customer_status, ...
FROM customer cs
GROUP BY cs.rfm_segment, cs.primary_store_type, cs.customer_status
ORDER BY client_count DESC
```

## How to Run

### On Databricks

The benchmarks are deployed as a separate Databricks job (`deep_rayon_benchmark`), independent from the daily dbt pipeline. Run it manually after the pipeline completes:

```bash
# Build the wheel and deploy the bundle (includes both jobs)
mise run bundle:deploy

# Trigger the benchmark job
databricks bundle run deep_rayon_benchmark
```

The dbt pipeline (`deep_rayon_dbt_pipeline`) handles: dbt_run → dbt_test → dbt_docs → optimize_tables.
The benchmark job (`deep_rayon_benchmark`) runs independently: 4 queries via `python_wheel_task`.

### Testing locally

The benchmark **logic** (timing, plan parsing, result structure) is tested locally via unit tests in `tests/test_benchmark.py`. These use an in-memory DuckDB with a small test table — no Databricks connection needed.

```bash
mise run test
```

### Methodology

Each benchmark query runs:

1. **Warm-up pass** -- executes the query once to prime caches
2. **3 timed iterations** -- measures wall-clock time with `time.perf_counter()`
3. **Median reported** -- the middle value of the 3 runs (reduces outlier noise)
4. **EXPLAIN EXTENDED** -- captures the Spark execution plan for file scan metrics

## Optimization Impact Summary

The benchmarks are designed to exercise the exact access patterns that table optimization addresses:

| Query | Join Pattern | Databricks Optimization |
|-------|-------------|------------------------|
| Q1 | `transaction_date`, `product_id`, `store_id` | Z-ORDER on `transaction_date` + `store_id` enables file skipping on the transactions table |
| Q2 | `store_id` filter + aggregation | Z-ORDER on `store_id` + `store_type` on gold tables reduces scan width |
| Q3 | `client_id`, `store_id` multi-join | Z-ORDER on `client_id` + `store_id` on transactions improves join probe performance |
| Q4 | `rfm_segment`, `customer_status` grouping | Z-ORDER on these columns in `customer` enables efficient grouped scans |

**DuckDB vs Databricks:** DuckDB runs all queries in-process on a single machine. The benchmark numbers reflect local performance only. On Databricks with Delta Lake, the same queries benefit from:

- **Z-ORDER** -- data co-location reduces the number of files scanned for filtered queries
- **OPTIMIZE** -- file compaction eliminates small-file overhead from batch ingestion
- **Partition pruning** -- date-partitioned transactions skip irrelevant time ranges entirely
- **Photon engine** -- vectorized C++ execution on Databricks Runtime for up to 3-8x speedup on aggregation-heavy queries

The benchmarks serve as a regression baseline: if a query exceeds 10 seconds locally on 500K rows, something is structurally wrong with the model or join logic.
