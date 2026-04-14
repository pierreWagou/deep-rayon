"""
Airflow DAG: deep_rayon_benchmark
Run performance benchmark queries against dbt-built tables on Databricks.

Architecture:
  - No schedule — triggered manually from the Airflow UI or CLI
  - Submits the deep_rayon_benchmark Databricks job (Python wheel task)
  - Runs 4 JOIN-heavy queries and measures duration, files scanned, and cost
  - Typically run after the dbt pipeline completes to measure query performance

Trigger from CLI:
  airflow dags trigger deep_rayon_benchmark
"""

from __future__ import annotations

import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from airflow import DAG

# ── DAG configuration ──────────────────────────────────────────────────────────

DATABRICKS_CONN_ID = "databricks_default"
BENCHMARK_JOB_ID = "{{ var.value.deep_rayon_benchmark_job_id }}"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 0,
    "execution_timeout": pendulum.duration(hours=1),
    "email_on_failure": True,
    "email": ["data-engineering@vusion.com"],
}


# ── Notification callback ──────────────────────────────────────────────────────


def send_notification(status="success", **context):
    """Send benchmark completion notification (placeholder for Slack/Teams)."""
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    dag_id = dag_run.dag_id if dag_run else "unknown"
    execution = dag_run.logical_date if dag_run else "unknown"
    task_id = task_instance.task_id if task_instance else "N/A"
    print(f"[{status.upper()}] DAG: {dag_id} | Task: {task_id} | Execution: {execution}")


def on_failure_callback(context):
    send_notification(status="failure", **context)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="deep_rayon_benchmark",
    description="Run benchmark queries against dbt-built tables on Databricks",
    schedule=None,  # Manual trigger only
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    tags=["databricks", "benchmark", "deep-rayon"],
    doc_md=__doc__,
) as dag:
    # ── Task: run benchmark queries ─────────────────────────────────────────

    run_benchmarks = DatabricksRunNowOperator(
        task_id="run_benchmarks",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=BENCHMARK_JOB_ID,
        do_xcom_push=True,
        wait_for_termination=True,
        polling_period_seconds=30,
    )

    # ── Task: send completion notification ──────────────────────────────────

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_notification,
        op_kwargs={"status": "success"},
    )

    # ── Task dependencies ───────────────────────────────────────────────────

    run_benchmarks >> notify_success
