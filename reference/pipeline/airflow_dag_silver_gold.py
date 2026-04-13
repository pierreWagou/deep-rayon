"""
Airflow DAG to orchestrate Silver and Gold layer transformations using Azure Blob Storage.

This DAG:
1. Runs Silver layer script (customer analytics) - reads/writes from/to Azure Blob Storage
2. Runs Gold layer script (KPI datamart) - reads/writes from/to Azure Blob Storage
3. Handles dependencies and error notifications
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging
import subprocess
import os

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-engineering-team@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'silver_gold_azure_blob_pipeline',
    default_args=default_args,
    description='Orchestrate Silver and Gold layer transformations using Azure Blob Storage',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC (after bronze ingestion)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['azure', 'blob-storage', 'silver', 'gold', 'retail', 'analytics'],
    max_active_runs=1,
)

# Get configuration from Airflow Variables
def get_azure_config(**context):
    """Retrieve Azure Blob Storage configuration from Airflow Variables."""
    config = {
        'storage_account_name': Variable.get('azure_storage_account_name', default_var='yourstorageaccount'),
        'container_bronze': Variable.get('azure_container_bronze', default_var='bronze'),
        'container_silver': Variable.get('azure_container_silver', default_var='silver'),
        'container_gold': Variable.get('azure_container_gold', default_var='gold'),
        'connection_string': Variable.get('azure_storage_connection_string', default_var=''),
        'client_id': Variable.get('azure_client_id', default_var=''),
        'client_secret': Variable.get('azure_client_secret', default_var=''),
        'tenant_endpoint': Variable.get('azure_tenant_endpoint', default_var=''),
    }
    logging.info(f"Using Azure Blob Storage config: Storage Account = {config['storage_account_name']}")
    return config

# Task to get configuration
get_config = PythonOperator(
    task_id='get_azure_config',
    python_callable=get_azure_config,
    dag=dag,
)

# Silver Layer Task - Run Python script with Spark
def run_silver_layer(**context):
    """Execute Silver layer script using Spark on Azure."""
    config = context['ti'].xcom_pull(task_ids='get_azure_config')
    
    # Set environment variables for the script
    env = os.environ.copy()
    env['AZURE_STORAGE_ACCOUNT_NAME'] = config['storage_account_name']
    env['AZURE_CONTAINER_BRONZE'] = config['container_bronze']
    env['AZURE_CONTAINER_SILVER'] = config['container_silver']
    env['AZURE_STORAGE_CONNECTION_STRING'] = config['connection_string']
    env['AZURE_CLIENT_ID'] = config['client_id']
    env['AZURE_CLIENT_SECRET'] = config['client_secret']
    env['AZURE_TENANT_ENDPOINT'] = config['tenant_endpoint']
    
    # Path to the script (adjust based on your deployment)
    script_path = Variable.get('silver_layer_script_path', 
                               default_var='/opt/airflow/dags/pipeline/silver_customer_layer.py')
    
    # Run Spark submit command
    spark_submit_cmd = [
        'spark-submit',
        '--master', 'yarn',  # or 'local[*]' for local execution
        '--deploy-mode', 'client',
        '--conf', 'spark.sql.adaptive.enabled=true',
        '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
        script_path
    ]
    
    logging.info(f"Running Silver layer script: {' '.join(spark_submit_cmd)}")
    
    result = subprocess.run(
        spark_submit_cmd,
        env=env,
        capture_output=True,
        text=True,
        timeout=3600  # 1 hour timeout
    )
    
    if result.returncode != 0:
        logging.error(f"Silver layer script failed: {result.stderr}")
        raise Exception(f"Silver layer script failed with return code {result.returncode}")
    
    logging.info(f"Silver layer script output: {result.stdout}")
    return result.returncode

submit_silver_job = PythonOperator(
    task_id='run_silver_customer_layer',
    python_callable=run_silver_layer,
    dag=dag,
)

# Gold Layer Task - Run Python script with Spark
def run_gold_layer(**context):
    """Execute Gold layer script using Spark on Azure."""
    config = context['ti'].xcom_pull(task_ids='get_azure_config')
    
    # Set environment variables for the script
    env = os.environ.copy()
    env['AZURE_STORAGE_ACCOUNT_NAME'] = config['storage_account_name']
    env['AZURE_CONTAINER_SILVER'] = config['container_silver']
    env['AZURE_CONTAINER_GOLD'] = config['container_gold']
    env['AZURE_STORAGE_CONNECTION_STRING'] = config['connection_string']
    env['AZURE_CLIENT_ID'] = config['client_id']
    env['AZURE_CLIENT_SECRET'] = config['client_secret']
    env['AZURE_TENANT_ENDPOINT'] = config['tenant_endpoint']
    
    # Path to the script (adjust based on your deployment)
    script_path = Variable.get('gold_layer_script_path', 
                               default_var='/opt/airflow/dags/pipeline/gold_datamart_kpis.py')
    
    # Run Spark submit command
    spark_submit_cmd = [
        'spark-submit',
        '--master', 'yarn',  # or 'local[*]' for local execution
        '--deploy-mode', 'client',
        '--conf', 'spark.sql.adaptive.enabled=true',
        '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
        script_path
    ]
    
    logging.info(f"Running Gold layer script: {' '.join(spark_submit_cmd)}")
    
    result = subprocess.run(
        spark_submit_cmd,
        env=env,
        capture_output=True,
        text=True,
        timeout=3600  # 1 hour timeout
    )
    
    if result.returncode != 0:
        logging.error(f"Gold layer script failed: {result.stderr}")
        raise Exception(f"Gold layer script failed with return code {result.returncode}")
    
    logging.info(f"Gold layer script output: {result.stdout}")
    return result.returncode

submit_gold_job = PythonOperator(
    task_id='run_gold_datamart_kpis',
    python_callable=run_gold_layer,
    dag=dag,
)

# Validation task (optional)
def validate_gold_tables(**context):
    """Validate that gold tables were created successfully in Azure Blob Storage."""
    from azure.storage.blob import BlobServiceClient
    
    config = context['ti'].xcom_pull(task_ids='get_azure_config')
    
    try:
        # Initialize Blob Service Client
        if config['connection_string']:
            blob_service_client = BlobServiceClient.from_connection_string(config['connection_string'])
        else:
            # Use Azure AD authentication if connection string not available
            from azure.identity import DefaultAzureCredential
            account_url = f"https://{config['storage_account_name']}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url=account_url, 
                                                   credential=DefaultAzureCredential())
        
        container_client = blob_service_client.get_container_client(config['container_gold'])
        
        # Check if gold layer files exist
        basket_files = list(container_client.list_blobs(name_starts_with='basket_analysis_per_store/'))
        trend_files = list(container_client.list_blobs(name_starts_with='product_trend_per_store/'))
        clients_files = list(container_client.list_blobs(name_starts_with='nb_clients_per_store/'))
        
        logging.info(f"Basket analysis files found: {len(basket_files)}")
        logging.info(f"Product trend files found: {len(trend_files)}")
        logging.info(f"Clients per store files found: {len(clients_files)}")
        
        if len(basket_files) > 0 and len(trend_files) > 0 and len(clients_files) > 0:
            logging.info("✅ Gold tables validation completed - all files exist")
            return True
        else:
            raise Exception("Some gold layer files are missing")
            
    except Exception as e:
        logging.error(f"Validation failed: {str(e)}")
        raise

validate_gold = PythonOperator(
    task_id='validate_gold_tables',
    python_callable=validate_gold_tables,
    dag=dag,
)

# Notification task
def send_completion_notification(**context):
    """Send notification upon pipeline completion."""
    dag_run = context.get('dag_run')
    
    if dag_run.state == 'success':
        message = f"✅ Silver and Gold layer pipeline completed successfully at {datetime.now()}"
    else:
        message = f"❌ Silver and Gold layer pipeline failed at {datetime.now()}"
    
    logging.info(message)
    # Add Slack/Teams webhook call here if needed
    return message

send_notification = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    dag=dag,
)

# Define task dependencies
get_config >> submit_silver_job >> submit_gold_job >> validate_gold >> send_notification


# -----------------------------------------------------------------------------
# Local execution (no Airflow scheduler): run Silver then Gold on local CSV data
# -----------------------------------------------------------------------------
def run_pipeline_locally():
    """Run silver and gold scripts with --local via subprocess."""
    import argparse
    import subprocess
    import sys
    from pathlib import Path

    pipeline_dir = Path(__file__).resolve().parent
    default_data = pipeline_dir.parent / "data"

    parser = argparse.ArgumentParser(
        description="Run Silver + Gold pipeline locally (same logic as DAG tasks, filesystem I/O)."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run outside Airflow on local CSV; writes under output/silver and output/gold.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=default_data,
        help=f"CSV data directory (default: {default_data})",
    )
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Only run the gold layer (silver output must already exist if needed).",
    )
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="Only run the silver layer.",
    )
    args = parser.parse_args()
    if not args.local:
        parser.error("Use --local to run the pipeline on local data (see --help).")

    data_dir = args.data_dir.resolve()
    silver_script = pipeline_dir / "silver_customer_layer.py"
    gold_script = pipeline_dir / "gold_datamart_kpis.py"

    base_cmd = [sys.executable]
    if not args.skip_silver:
        cmd = base_cmd + [
            str(silver_script),
            "--local",
            "--data-dir",
            str(data_dir),
        ]
        print("Running:", " ".join(cmd))
        subprocess.run(cmd, check=True)
    if not args.skip_gold:
        cmd = base_cmd + [
            str(gold_script),
            "--local",
            "--data-dir",
            str(data_dir),
        ]
        print("Running:", " ".join(cmd))
        subprocess.run(cmd, check=True)
    print("Local pipeline finished.")


if __name__ == "__main__":
    run_pipeline_locally()
