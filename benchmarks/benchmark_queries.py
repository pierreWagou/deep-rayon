"""
Benchmark queries for the Vusion dbt project.

Measures query duration for JOIN-heavy analytical queries against DuckDB.
In production, these same queries would run against Databricks Delta tables
and benefit from Z-ORDER + OPTIMIZE optimizations.

Usage:
    cd dbt_project && uv run pytest ../benchmarks/ -v
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb
import pytest

# DuckDB database built by dbt
DB_PATH = Path(__file__).parent.parent / "dbt_project" / "target" / "vusion.duckdb"


@pytest.fixture(scope="module")
def conn():
    """Open a read-only connection to the dbt-built DuckDB database."""
    if not DB_PATH.exists():
        pytest.skip(f"DuckDB database not found at {DB_PATH}. Run 'mise run dbt' first.")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    yield con
    con.close()


def run_benchmark(conn, name: str, query: str) -> dict:
    """Execute a query and return timing metrics."""
    # Warm-up run
    conn.execute(query).fetchall()

    # Timed run (3 iterations, take median)
    durations = []
    for _ in range(3):
        start = time.perf_counter()
        result = conn.execute(query).fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000
        durations.append(elapsed_ms)

    durations.sort()
    median_ms = durations[1]  # median of 3
    row_count = len(result)

    # Get execution plan (EXPLAIN ANALYZE)
    plan = conn.execute(f"EXPLAIN ANALYZE {query}").fetchall()
    plan_text = "\n".join(row[1] for row in plan)

    return {
        "name": name,
        "median_ms": round(median_ms, 2),
        "min_ms": round(durations[0], 2),
        "max_ms": round(durations[2], 2),
        "rows": row_count,
        "plan": plan_text,
    }


# ── Q1: Product sales by day ─────────────────────────────────────────────────
# Joins transactions → products → stores (3-way join)

Q1_PRODUCT_SALES_BY_DAY = """
SELECT
    t.transaction_date,
    p.brand,
    s.store_type,
    COUNT(DISTINCT t.transaction_id)  AS num_transactions,
    SUM(t.quantity)                   AS total_quantity,
    SUM(t.spend)                      AS total_spend,
    COUNT(DISTINCT t.client_id)       AS unique_clients
FROM main_staging.stg_transactions t
JOIN main_staging.stg_products p ON t.product_id = p.product_id
JOIN main_staging.stg_stores s ON t.store_id = s.store_id
GROUP BY t.transaction_date, p.brand, s.store_type
ORDER BY t.transaction_date DESC, total_spend DESC
"""


def test_q1_product_sales_by_day(conn):
    """Q1 — Product sales by day: 3-way join (transactions × products × stores)."""
    result = run_benchmark(conn, "Q1: Product sales by day", Q1_PRODUCT_SALES_BY_DAY)
    print(f"\n  {result['name']}: {result['median_ms']}ms (median), {result['rows']} rows")
    assert result["rows"] > 0
    assert result["median_ms"] < 10000  # Should complete within 10 seconds


# ── Q2: Store-level revenue ──────────────────────────────────────────────────
# Joins transactions → stores → (aggregated basket metrics)

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
FROM main_staging.stg_transactions t
JOIN main_staging.stg_stores s ON t.store_id = s.store_id
GROUP BY s.store_id, s.store_type, s.latitude, s.longitude
ORDER BY total_revenue DESC
"""


def test_q2_store_revenue(conn):
    """Q2 — Store-level revenue: 2-way join (transactions × stores)."""
    result = run_benchmark(conn, "Q2: Store-level revenue", Q2_STORE_REVENUE)
    print(f"\n  {result['name']}: {result['median_ms']}ms (median), {result['rows']} rows")
    assert result["rows"] > 0
    assert result["median_ms"] < 10000


# ── Q3: Client basket metrics ────────────────────────────────────────────────
# Joins transactions → products → clients → stores (4-way join)

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
    FROM main_staging.stg_transactions t
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
JOIN main_staging.stg_clients c ON b.client_id = c.client_id
JOIN main_staging.stg_stores s ON b.store_id = s.store_id
GROUP BY c.client_id, c.name, c.job, s.store_type
HAVING COUNT(DISTINCT b.transaction_id) >= 2
ORDER BY avg_basket_value DESC
LIMIT 1000
"""


def test_q3_client_basket_metrics(conn):
    """Q3 — Client basket metrics: 4-way join with CTE + aggregation."""
    result = run_benchmark(conn, "Q3: Client basket metrics", Q3_CLIENT_BASKET)
    print(f"\n  {result['name']}: {result['median_ms']}ms (median), {result['rows']} rows")
    assert result["rows"] > 0
    assert result["median_ms"] < 10000


# ── Q4: RFM segment distribution by store type ──────────────────────────────
# Joins silver customer_silver → gold tables (cross-layer)

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
FROM main_silver.customer_silver cs
GROUP BY cs.rfm_segment, cs.primary_store_type, cs.customer_status
ORDER BY client_count DESC
"""


def test_q4_rfm_segment_distribution(conn):
    """Q4 — RFM segment distribution: aggregation on silver layer."""
    result = run_benchmark(conn, "Q4: RFM segment by store type", Q4_RFM_BY_STORE)
    print(f"\n  {result['name']}: {result['median_ms']}ms (median), {result['rows']} rows")
    assert result["rows"] > 0
    assert result["median_ms"] < 10000
