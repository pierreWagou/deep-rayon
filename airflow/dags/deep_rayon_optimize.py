"""
Airflow DAG: deep_rayon_optimize
Weekly Delta table optimization (OPTIMIZE + Z-ORDER) on Databricks.

Architecture:
  - Decoupled from the daily dbt pipeline to avoid unnecessary compaction overhead
  - Runs weekly (Sunday 05:00 Europe/Paris) — standard cadence for Delta maintenance
  - Submits a standalone Databricks job that calls dbt run-operation
  - Task flow: optimize_tables → notify

At scale (billions of rows), weekly OPTIMIZE is the recommended cadence.
Daily runs would trigger compaction on every build, wasting compute without
meaningful query performance improvement.
"""

from __future__ import annotations

import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from airflow import DAG

# ── DAG configuration ──────────────────────────────────────────────────────────

DATABRICKS_CONN_ID = "databricks_default"
OPTIMIZE_JOB_ID = "{{ var.value.deep_rayon_optimize_job_id }}"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "execution_timeout": pendulum.duration(hours=1),
    "email_on_failure": True,
    "email": ["data-engineering@vusion.com"],
}


# ── Notification callback ──────────────────────────────────────────────────────


def send_notification(status="success", **context):
    """Send pipeline completion notification (placeholder for Slack/Teams)."""
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
    dag_id="deep_rayon_optimize",
    description="Weekly Delta table optimization: OPTIMIZE + Z-ORDER on silver and gold tables",
    schedule="0 5 * * 0",  # Every Sunday at 05:00 Europe/Paris
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    tags=["databricks", "delta", "optimization", "deep-rayon"],
    doc_md=__doc__,
) as dag:
    # ── Task: optimize Delta tables ─────────────────────────────────────────

    optimize_tables = DatabricksRunNowOperator(
        task_id="optimize_tables",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=OPTIMIZE_JOB_ID,
        notebook_params={
            "dbt_command": "run-operation generate_optimization_statements",
            "target": "prod",
        },
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

    optimize_tables >> notify_success
