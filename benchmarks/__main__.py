"""
Run benchmarks on Databricks.

Entry points:
  - Databricks job: python_wheel_task with entry_point "deep-rayon-benchmark"
  - Databricks CLI: databricks bundle run deep_rayon_benchmark
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from benchmarks.benchmark_queries import BENCHMARKS, run_benchmark

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"


def main() -> None:
    """Run all 4 benchmark queries and print results."""
    parser = argparse.ArgumentParser(description="Run Vusion benchmark queries")
    parser.add_argument(
        "--catalog",
        default="hive_metastore",
        help="Unity Catalog name where dbt tables are written (default: hive_metastore)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Number of timed iterations per query, median is reported (default: 3)",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG {args.catalog}")

    def execute(sql: str) -> list:
        return spark.sql(sql).collect()

    print(f"\n{'=' * 60}")
    print(f"  Vusion Benchmark Results  (catalog: {args.catalog}, iterations: {args.iterations})")
    print(f"  Schemas: {BRONZE} / {SILVER} / {GOLD}")
    print(f"{'=' * 60}\n")

    for name, query_template in BENCHMARKS:
        query = query_template.format(bronze=BRONZE, silver=SILVER, gold=GOLD)
        result = run_benchmark(
            execute,
            name,
            query,
            explain_prefix="EXPLAIN EXTENDED",
            iterations=args.iterations,
        )
        print(
            f"  {result['name']}:\n"
            f"    Duration:      {result['median_ms']}ms (median),"
            f" {result['min_ms']}ms (min),"
            f" {result['max_ms']}ms (max)\n"
            f"    Rows:          {result['rows']}\n"
            f"    Files scanned: {result['files_scanned']}\n"
            f"    Est. cost:     ${result['cost_estimate_usd']}\n"
        )

    print(f"{'=' * 60}")
    print("  Done")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
