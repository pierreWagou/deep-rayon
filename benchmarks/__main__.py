"""
Run benchmarks on Databricks.

Entry points:
  - Databricks job: python_wheel_task with entry_point "vusion-benchmark"
  - Databricks CLI: databricks bundle run vusion_dbt_pipeline --task run_benchmarks
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from benchmarks.benchmark_queries import BENCHMARKS, run_benchmark

# Databricks schema convention: profiles.yml schema + "_" + layer
BRONZE = "retail_bronze"
SILVER = "retail_silver"
GOLD = "retail_gold"


def main() -> None:
    """Run all 4 benchmark queries and print results."""
    spark = SparkSession.builder.getOrCreate()

    def execute(sql: str) -> list:
        return spark.sql(sql).collect()

    print(f"\n{'=' * 60}")
    print("  Vusion Benchmark Results")
    print(f"{'=' * 60}\n")

    for name, query_template in BENCHMARKS:
        query = query_template.format(bronze=BRONZE, silver=SILVER, gold=GOLD)
        result = run_benchmark(execute, name, query, explain_prefix="EXPLAIN EXTENDED")
        print(
            f"  {result['name']}:\n"
            f"    Duration:      {result['median_ms']}ms (median),"
            f" {result['min_ms']}ms (min),"
            f" {result['max_ms']}ms (max)\n"
            f"    Rows:          {result['rows']}\n"
            f"    Files scanned: {result['files_scanned']}\n"
            f"    Est. cost:     ${result['cost_estimate_usd']}\n"
        )

    spark.stop()
    print(f"{'=' * 60}")
    print("  Done")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
