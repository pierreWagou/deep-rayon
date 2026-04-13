"""
Unit tests for the benchmark module.

Tests the run_benchmark helper function using an in-memory DuckDB connection
with a small test table — no dependency on the dbt-built database.
"""

from __future__ import annotations

from collections.abc import Callable, Generator

import duckdb
import pytest

from benchmarks.benchmark_queries import run_benchmark

# Type alias for the executor callable
Executor = Callable[[str], list]


@pytest.fixture
def execute() -> Generator[Executor]:
    """Create a DuckDB executor backed by an in-memory database with test data."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE sales (
            id INTEGER,
            store_id INTEGER,
            product TEXT,
            quantity INTEGER,
            spend DOUBLE
        )
    """)
    con.execute("""
        INSERT INTO sales VALUES
            (1, 10, 'apple',  3, 4.50),
            (2, 10, 'banana', 1, 1.20),
            (3, 20, 'apple',  2, 3.00),
            (4, 20, 'cherry', 5, 7.50),
            (5, 30, 'banana', 1, 1.20)
    """)

    def _execute(sql: str) -> list:
        return con.execute(sql).fetchall()

    yield _execute
    con.close()


SIMPLE_QUERY = (
    "SELECT store_id, SUM(spend) AS total FROM sales GROUP BY store_id ORDER BY total DESC"
)
SINGLE_ROW_QUERY = "SELECT COUNT(*) AS cnt FROM sales"


class TestRunBenchmarkReturnStructure:
    """run_benchmark must return a dict with the expected keys and types."""

    def test_returns_expected_keys(self, execute: Callable[[str], list]):
        result = run_benchmark(execute, "test", SIMPLE_QUERY)
        assert set(result.keys()) == {
            "name",
            "median_ms",
            "min_ms",
            "max_ms",
            "rows",
            "files_scanned",
            "cost_estimate_usd",
            "plan",
        }

    def test_name_is_passed_through(self, execute: Callable[[str], list]):
        result = run_benchmark(execute, "my benchmark", SIMPLE_QUERY)
        assert result["name"] == "my benchmark"

    def test_row_count_matches_query(self, execute: Callable[[str], list]):
        result = run_benchmark(execute, "test", SIMPLE_QUERY)
        assert result["rows"] == 3  # 3 distinct store_ids

    def test_single_row_query(self, execute: Callable[[str], list]):
        result = run_benchmark(execute, "test", SINGLE_ROW_QUERY)
        assert result["rows"] == 1


class TestRunBenchmarkTimings:
    """Timing values must be positive and ordered correctly."""

    def test_timings_are_positive(self, execute: Callable[[str], list]):
        result = run_benchmark(execute, "test", SIMPLE_QUERY)
        assert result["median_ms"] > 0
        assert result["min_ms"] > 0
        assert result["max_ms"] > 0

    def test_min_le_median_le_max(self, execute: Callable[[str], list]):
        result = run_benchmark(execute, "test", SIMPLE_QUERY)
        assert result["min_ms"] <= result["median_ms"] <= result["max_ms"]

    def test_timings_are_rounded(self, execute: Callable[[str], list]):
        result = run_benchmark(execute, "test", SIMPLE_QUERY)
        # round(..., 2) should produce at most 2 decimal places
        for key in ("median_ms", "min_ms", "max_ms"):
            text = str(result[key])
            if "." in text:
                assert len(text.split(".")[1]) <= 2


class TestRunBenchmarkPlan:
    """The execution plan must be captured as a non-empty string."""

    def test_plan_is_nonempty_string(self, execute: Callable[[str], list]):
        result = run_benchmark(execute, "test", SIMPLE_QUERY)
        assert isinstance(result["plan"], str)
        assert len(result["plan"]) > 0


class TestRunBenchmarkEdgeCases:
    """Edge cases: empty results, failing queries."""

    def test_empty_result_set(self, execute: Callable[[str], list]):
        query = "SELECT * FROM sales WHERE id < 0"
        result = run_benchmark(execute, "empty", query)
        assert result["rows"] == 0

    def test_invalid_query_raises(self, execute: Callable[[str], list]):
        with pytest.raises(duckdb.CatalogException):
            run_benchmark(execute, "bad", "SELECT * FROM nonexistent_table")
