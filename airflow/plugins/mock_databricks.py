"""
Mock Databricks operators for local Airflow development.

When running locally (without a Databricks workspace), the real
DatabricksRunNowOperator would fail on every task because there's
no Databricks connection. This plugin monkey-patches the `execute`
method so that tasks log their parameters and succeed immediately.

What you can validate locally:
  - DAG parsing and loading (import errors surface in the UI)
  - Task dependency graph (Graph tab)
  - Schedule, trigger rules, and catchup settings
  - XCom push behavior (mock returns run metadata)
  - Failure callbacks (force-fail a task to test)
  - Task logs (each mock execution logs job_id and params)

The DAG source code is completely unchanged — the same file runs
in production with real Databricks operators.
"""

from __future__ import annotations

import logging
import time

from airflow.plugins_manager import AirflowPlugin
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.sensors.databricks_partition import (
    DatabricksPartitionSensor,
)

log = logging.getLogger(__name__)

# ── Monkey-patch DatabricksRunNowOperator.execute ────────────────────────────


def _mock_run_now_execute(self, context):
    """Replace real Databricks job submission with a local stub."""
    # The operator stores job_id and notebook_params inside self.json
    job_config = getattr(self, "json", {}) or {}
    job_id = job_config.get("job_id", "unknown")
    notebook_params = job_config.get("notebook_params", {})

    log.info(
        "[MOCK] DatabricksRunNowOperator | task=%s | job_id=%s | params=%s",
        self.task_id,
        job_id,
        notebook_params,
    )
    time.sleep(2)  # Simulate execution time

    result = {
        "run_id": 12345,
        "state": "SUCCESS",
        "notebook_params": notebook_params,
        "mock": True,
    }
    log.info("[MOCK] Run completed: %s", result)

    if getattr(self, "do_xcom_push", False):
        return result
    return None


DatabricksRunNowOperator.execute = _mock_run_now_execute

# ── Monkey-patch DatabricksPartitionSensor.execute ───────────────────────────


def _mock_sensor_execute(self, context):
    """Replace real partition check with an immediate success."""
    log.info("[MOCK] DatabricksPartitionSensor | partition ready (simulated)")
    return True


DatabricksPartitionSensor.execute = _mock_sensor_execute


# ── Airflow Plugin Registration ──────────────────────────────────────────────


class MockDatabricksPlugin(AirflowPlugin):
    name = "mock_databricks"
