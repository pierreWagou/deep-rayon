FROM apache/airflow:2.10.5-python3.12

# Install uv for dependency isolation
COPY --from=ghcr.io/astral-sh/uv:0.7.8 /uv /uvx /usr/local/bin/

# Copy dbt project into the image
COPY dbt_project/ /opt/airflow/dbt_project/

# Pre-sync dbt dependencies so DAG tasks start fast
RUN uv sync --project /opt/airflow/dbt_project --no-install-project --quiet

# Install the Databricks provider for Airflow
RUN pip install --no-cache-dir apache-airflow-providers-databricks
