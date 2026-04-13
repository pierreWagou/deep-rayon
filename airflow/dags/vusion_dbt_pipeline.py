"""
Airflow DAG: vusion_dbt_pipeline
Orchestrates dbt transformations on Databricks with proper task dependencies,
error handling, and monitoring.

Architecture:
  - Airflow is orchestration-only (no dbt/data dependencies)
  - Each task submits a Databricks job and waits for completion
  - Task flow: dbt_run → dbt_test → dbt_docs_generate → optimize_tables → notify

Production: Uses DatabricksRunNowOperator to submit jobs on Databricks.
Local dev: Can be tested with `airflow dags test vusion_dbt_pipeline`.
"""

from __future__ import annotations

import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

# ── DAG configuration ──────────────────────────────────────────────────────────

DATABRICKS_CONN_ID = "databricks_default"
DBT_JOB_ID = "{{ var.value.vusion_dbt_job_id }}"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "execution_timeout": pendulum.duration(hours=2),
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
    dag_id="vusion_dbt_pipeline",
    description="Orchestrate dbt transformations on Databricks: run → test → docs → optimize",
    schedule="0 3 * * *",  # Daily at 3 AM Paris time
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    tags=["dbt", "databricks", "retail", "vusion"],
    doc_md=__doc__,
) as dag:
    # ── Task Group: dbt transformation ──────────────────────────────────────

    with TaskGroup("dbt_transformations", tooltip="dbt run + test + docs") as dbt_group:
        dbt_run = DatabricksRunNowOperator(
            task_id="dbt_run",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=DBT_JOB_ID,
            notebook_params={
                "dbt_command": "run",
                "target": "prod",
            },
            do_xcom_push=True,
            wait_for_termination=True,
            polling_period_seconds=30,
        )

        dbt_test = DatabricksRunNowOperator(
            task_id="dbt_test",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=DBT_JOB_ID,
            notebook_params={
                "dbt_command": "test",
                "target": "prod",
            },
            do_xcom_push=True,
            wait_for_termination=True,
            polling_period_seconds=30,
        )

        dbt_docs = DatabricksRunNowOperator(
            task_id="dbt_docs_generate",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=DBT_JOB_ID,
            notebook_params={
                "dbt_command": "docs generate",
                "target": "prod",
            },
            wait_for_termination=True,
            polling_period_seconds=30,
        )

        dbt_run >> dbt_test >> dbt_docs

    # ── Task: optimize Delta tables ─────────────────────────────────────────

    optimize_tables = DatabricksRunNowOperator(
        task_id="optimize_tables",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DBT_JOB_ID,
        notebook_params={
            "dbt_command": "run-operation generate_optimization_statements",
            "target": "prod",
        },
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

    dbt_group >> optimize_tables >> notify_success
