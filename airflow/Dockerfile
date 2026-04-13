FROM apache/airflow:2.10.5-python3.12

# Silence SyntaxWarnings from bundled provider packages (azure, etc.)
ENV PYTHONWARNINGS="ignore::SyntaxWarning"

# Pin the Databricks provider to a version compatible with Airflow 2.10.x.
# Provider 7.x requires Airflow >= 2.11 and would break the base image.
RUN pip install --no-cache-dir 'apache-airflow-providers-databricks==6.13.0'

# The mock plugin (plugins/mock_databricks.py) overrides operator execution
# at runtime so tasks succeed without a real Databricks workspace.
