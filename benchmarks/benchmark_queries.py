"""
Benchmark queries for the Vusion dbt project.

Defines 4 JOIN-heavy analytical queries and a run_benchmark() function
that measures duration, files scanned, and estimated Databricks cost.

Deployed to Databricks as a Python wheel — entry point in __main__.py.
Tested locally via tests/test_benchmark.py with in-memory DuckDB.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable

BENCHMARK_ITERATIONS = 3  # default, can be overridden via run_benchmark(iterations=N)

# Databricks cost model for bonus estimate (i3.xlarge single-node, $0.25/DBU, ~2 DBU/h)
DBU_RATE_PER_HOUR = 2.0
DBU_COST_USD = 0.25


# ── Runner ───────────────────────────────────────────────────────────────────


def run_benchmark(
    execute: Callable[[str], list],
    name: str,
    query: str,
    explain_prefix: str = "EXPLAIN ANALYZE",
    iterations: int = BENCHMARK_ITERATIONS,
) -> dict:
    """Execute a query and return timing metrics.

    Args:
        execute: Callable that takes a SQL string and returns a list of rows.
        name: Human-readable name for this benchmark.
        query: SQL query to benchmark.
        explain_prefix: SQL prefix for execution plans.
            DuckDB uses "EXPLAIN ANALYZE", Spark uses "EXPLAIN EXTENDED".
        iterations: Number of timed iterations (median is reported).
    """
    # Warm-up run
    execute(query)

    # Timed run (median of N iterations)
    durations = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = execute(query)
        elapsed_ms = (time.perf_counter() - start) * 1000
        durations.append(elapsed_ms)

    durations.sort()
    median_ms = durations[iterations // 2]
    row_count = len(result)

    # Execution plan + files scanned
    plan_rows = execute(f"{explain_prefix} {query}")
    plan_text = "\n".join(str(row) for row in plan_rows)
    files_scanned = len(re.findall(r"TABLE_SCAN|FileScan|number of files read", plan_text))

    # Bonus: estimated Databricks compute cost (runtime x DBU rate)
    cost_estimate_usd = round(median_ms / 1000 / 3600 * DBU_RATE_PER_HOUR * DBU_COST_USD, 6)

    return {
        "name": name,
        "median_ms": round(median_ms, 2),
        "min_ms": round(durations[0], 2),
        "max_ms": round(durations[-1], 2),
        "rows": row_count,
        "files_scanned": files_scanned,
        "cost_estimate_usd": cost_estimate_usd,
        "plan": plan_text,
    }


# ── Queries ──────────────────────────────────────────────────────────────────
# Schema placeholders {bronze}, {silver}, {gold} are resolved at runtime
# by __main__.py before calling run_benchmark().

Q1_PRODUCT_SALES_BY_DAY = """
SELECT
    t.transaction_date,
    p.brand,
    s.store_type,
    COUNT(DISTINCT t.transaction_id)  AS num_transactions,
    SUM(t.quantity)                   AS total_quantity,
    SUM(t.spend)                      AS total_spend,
    COUNT(DISTINCT t.client_id)       AS unique_clients
FROM {bronze}.transactions t
JOIN {bronze}.products p ON t.product_id = p.product_id
JOIN {bronze}.stores s ON t.store_id = s.store_id
GROUP BY t.transaction_date, p.brand, s.store_type
ORDER BY t.transaction_date DESC, total_spend DESC
"""

Q2_STORE_REVENUE = """
SELECT
    s.store_id,
    s.store_type,
    s.latitude,
    s.longitude,
    COUNT(DISTINCT t.transaction_id)    AS total_transactions,
    COUNT(DISTINCT t.client_id)         AS unique_clients,
    SUM(t.spend)                        AS total_revenue,
    AVG(t.spend)                        AS avg_transaction_value,
    SUM(t.quantity)                     AS total_items_sold,
    SUM(t.spend) / NULLIF(COUNT(DISTINCT t.client_id), 0) AS revenue_per_client
FROM {bronze}.transactions t
JOIN {bronze}.stores s ON t.store_id = s.store_id
GROUP BY s.store_id, s.store_type, s.latitude, s.longitude
ORDER BY total_revenue DESC
"""

Q3_CLIENT_BASKET = """
WITH baskets AS (
    SELECT
        t.client_id,
        t.transaction_id,
        t.store_id,
        t.transaction_date,
        SUM(t.quantity)               AS basket_size,
        SUM(t.spend)                  AS basket_value,
        COUNT(DISTINCT t.product_id)  AS items_in_basket
    FROM {bronze}.transactions t
    GROUP BY t.client_id, t.transaction_id, t.store_id, t.transaction_date
)
SELECT
    c.client_id,
    c.name,
    c.job,
    s.store_type,
    COUNT(DISTINCT b.transaction_id)     AS num_baskets,
    AVG(b.basket_size)                   AS avg_basket_size,
    AVG(b.basket_value)                  AS avg_basket_value,
    MAX(b.basket_value)                  AS max_basket_value,
    AVG(b.items_in_basket)               AS avg_items_per_basket
FROM baskets b
JOIN {bronze}.clients c ON b.client_id = c.client_id
JOIN {bronze}.stores s ON b.store_id = s.store_id
GROUP BY c.client_id, c.name, c.job, s.store_type
HAVING COUNT(DISTINCT b.transaction_id) >= 2
ORDER BY avg_basket_value DESC
LIMIT 1000
"""

Q4_RFM_BY_STORE = """
SELECT
    cs.rfm_segment,
    cs.primary_store_type,
    cs.customer_status,
    COUNT(*)                              AS client_count,
    AVG(cs.frequency)                     AS avg_frequency,
    AVG(cs.monetary_value)                AS avg_monetary,
    AVG(cs.recency_days)                  AS avg_recency_days,
    AVG(cs.store_loyalty_score)           AS avg_loyalty_score
FROM {silver}.customer cs
GROUP BY cs.rfm_segment, cs.primary_store_type, cs.customer_status
ORDER BY client_count DESC
"""

BENCHMARKS = [
    ("Q1: Product sales by day", Q1_PRODUCT_SALES_BY_DAY),
    ("Q2: Store-level revenue", Q2_STORE_REVENUE),
    ("Q3: Client basket metrics", Q3_CLIENT_BASKET),
    ("Q4: RFM segment by store type", Q4_RFM_BY_STORE),
]
