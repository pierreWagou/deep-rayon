# Python script for Silver Layer: Customer Analytics
"""
This script creates the silver layer transformations for customer data:
- Client segmentation (RFM-based)
- Customer status (Active/Inactive/Churned)
- Main store preference per client

Reads from Azure Blob Storage (bronze layer) and writes to Azure Blob Storage (silver layer)

Local execution:
  python silver_customer_layer.py --local
  python silver_customer_layer.py --local --data-dir /path/to/data
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = SCRIPT_DIR.parent / "data"


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="Silver customer layer: Azure Blob or local CSV files."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run against local CSV files under --data-dir (no Azure). Writes Parquet under output/silver.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Directory with clients_500k.csv, stores_500k.csv, transactions_500k.csv (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Local silver output root (default: <repo>/output/silver when --local)",
    )
    return parser.parse_args()


_cli = parse_cli_args()
RUN_LOCAL = _cli.local
DATA_DIR = _cli.data_dir.resolve()
LOCAL_SILVER_OUTPUT_ROOT = (
    (_cli.output_dir.resolve() if _cli.output_dir else SCRIPT_DIR.parent / "output" / "silver")
    if RUN_LOCAL
    else None
)

# Configuration from environment variables (Azure mode only)
storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "yourstorageaccount")
container_name_bronze = os.getenv("AZURE_CONTAINER_BRONZE", "bronze")
container_name_silver = os.getenv("AZURE_CONTAINER_SILVER", "silver")
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")

# Paths in blob storage (Azure) or unused when RUN_LOCAL
bronze_path = f"abfss://{container_name_bronze}@{storage_account_name}.dfs.core.windows.net/"
silver_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/"

# COMMAND ----------

# Before PySpark starts the JVM: give Log4j2 a real config (stops "Reconfiguration failed... at 'null'")
_log4j2_xml = SCRIPT_DIR / "spark_conf" / "log4j2.xml"
if _log4j2_xml.is_file():
    _log4j_uri = _log4j2_xml.resolve().as_uri()
    _jvm_pre = (
        "-Dlog4j2.disable.jmx=true "
        "-Dlog4j2.StatusLogger.level=OFF "
        f"-Dlog4j.configurationFile={_log4j_uri} "
        f"-Dlog4j2.configurationFile={_log4j_uri}"
    )
    _jto = os.environ.get("JAVA_TOOL_OPTIONS", "").strip()
    os.environ["JAVA_TOOL_OPTIONS"] = f"{_jto} {_jvm_pre}".strip() if _jto else _jvm_pre

# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# Initialize Spark session
_spark_jvm_opts = "-Dlog4j2.disable.jmx=true"
spark_builder = (
    SparkSession.builder.appName("SilverCustomerLayer")
    .config("spark.driver.extraJavaOptions", _spark_jvm_opts)
    .config("spark.executor.extraJavaOptions", _spark_jvm_opts)
)
if RUN_LOCAL:
    spark_builder = spark_builder.master(os.getenv("SPARK_MASTER", "local[*]"))
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session initialized (local mode)")
else:
    spark = spark_builder \
        .config("spark.hadoop.fs.azure.account.auth.type", "OAuth") \
        .config("spark.hadoop.fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
        .config("spark.hadoop.fs.azure.account.oauth2.client.id", os.getenv("AZURE_CLIENT_ID", "")) \
        .config("spark.hadoop.fs.azure.account.oauth2.client.secret", os.getenv("AZURE_CLIENT_SECRET", "")) \
        .config("spark.hadoop.fs.azure.account.oauth2.client.endpoint", os.getenv("AZURE_TENANT_ENDPOINT", "")) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if connection_string:
        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
            connection_string.split("AccountKey=")[1].split(";")[0] if "AccountKey=" in connection_string else "",
        )
    print("✅ Spark session initialized (Azure)")

# Load Bronze tables
if RUN_LOCAL:
    clients_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(str(DATA_DIR / "clients_500k.csv"))
    )
    stores_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(str(DATA_DIR / "stores_500k.csv"))
    )
    transactions_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(str(DATA_DIR / "transactions_500k.csv"))
    )
    print(f"✅ Bronze tables loaded from local data dir: {DATA_DIR}")
else:
    clients_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        f"{bronze_path}clients/*.csv"
    )
    stores_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        f"{bronze_path}stores/*.csv"
    )
    transactions_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        f"{bronze_path}transactions/*.csv"
    )
    print("✅ Bronze tables loaded successfully from Azure Blob Storage")

# COMMAND ----------

# DBTITLE 1,Calculate RFM Metrics for Client Segmentation
# Calculate Recency, Frequency, and Monetary values for each client
current_date = datetime.now().date()

rfm_metrics = transactions_df \
    .withColumn("transaction_date", F.to_date("date", "yyyy-MM-dd")) \
    .groupBy("client_id") \
    .agg(
        # Recency: Days since last purchase
        F.datediff(F.lit(current_date), F.max("transaction_date")).alias("recency_days"),
        # Frequency: Number of transactions
        F.count("transaction_id").alias("frequency"),
        # Monetary: Total quantity purchased (assuming quantity as proxy for value)
        F.sum("quantity").alias("monetary_value"),
        # Additional metrics
        F.min("transaction_date").alias("first_purchase_date"),
        F.max("transaction_date").alias("last_purchase_date"),
        F.avg("quantity").alias("avg_quantity_per_transaction")
    )

# COMMAND ----------

# DBTITLE 1,Create RFM Scores and Segments
# Calculate RFM scores (1-5 scale, where 5 is best)
# Using percentile-based scoring
rfm_with_scores = rfm_metrics \
    .withColumn(
        "recency_score",
        F.when(F.col("recency_days") <= 30, 5)
         .when(F.col("recency_days") <= 60, 4)
         .when(F.col("recency_days") <= 90, 3)
         .when(F.col("recency_days") <= 180, 2)
         .otherwise(1)
    ) \
    .withColumn(
        "frequency_score",
        F.when(F.col("frequency") >= 20, 5)
         .when(F.col("frequency") >= 10, 4)
         .when(F.col("frequency") >= 5, 3)
         .when(F.col("frequency") >= 2, 2)
         .otherwise(1)
    ) \
    .withColumn(
        "monetary_score",
        F.when(F.col("monetary_value") >= 100, 5)
         .when(F.col("monetary_value") >= 50, 4)
         .when(F.col("monetary_value") >= 25, 3)
         .when(F.col("monetary_value") >= 10, 2)
         .otherwise(1)
    )

# Create RFM segment based on combined scores
rfm_segmented = rfm_with_scores \
    .withColumn(
        "rfm_segment",
        F.when((F.col("recency_score") >= 4) & (F.col("frequency_score") >= 4) & (F.col("monetary_score") >= 4), "Champion")
         .when((F.col("recency_score") >= 3) & (F.col("frequency_score") >= 3) & (F.col("monetary_score") >= 3), "Loyal Customer")
         .when((F.col("recency_score") >= 4) & (F.col("frequency_score") <= 2), "At Risk")
         .when((F.col("recency_score") <= 2) & (F.col("frequency_score") >= 3), "Hibernating")
         .when((F.col("recency_score") <= 2) & (F.col("frequency_score") <= 2), "Lost")
         .when((F.col("recency_score") >= 3) & (F.col("frequency_score") <= 2), "Need Attention")
         .when((F.col("recency_score") <= 3) & (F.col("frequency_score") >= 3), "About to Sleep")
         .otherwise("Potential Loyalist")
    )

# COMMAND ----------

# DBTITLE 1,Calculate Customer Status
# Define customer status based on recency
customer_status = rfm_segmented \
    .withColumn(
        "customer_status",
        F.when(F.col("recency_days") <= 30, "Active")
         .when(F.col("recency_days") <= 90, "Inactive")
         .otherwise("Churned")
    ) \
    .withColumn(
        "customer_lifecycle_stage",
        F.when(F.col("frequency") == 1, "New")
         .when(F.col("customer_status") == "Active", "Active")
         .when(F.col("customer_status") == "Inactive", "Lapsed")
         .otherwise("Churned")
    ) \
    .withColumn(
        "is_repeat_customer",
        F.when(F.col("frequency") > 1, True).otherwise(False)
    )

# COMMAND ----------

# DBTITLE 1,Calculate Main Store Preference per Client
# Find the store where each client shops most frequently
store_preference = transactions_df \
    .groupBy("client_id", "store_id") \
    .agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("quantity").alias("total_quantity")
    ) \
    .withColumn(
        "row_num",
        F.row_number().over(
            Window.partitionBy("client_id")
                  .orderBy(F.desc("transaction_count"), F.desc("total_quantity"))
        )
    ) \
    .filter(F.col("row_num") == 1) \
    .select("client_id", "store_id", "transaction_count", "total_quantity") \
    .withColumnRenamed("store_id", "primary_store_id") \
    .withColumnRenamed("transaction_count", "primary_store_transaction_count")

# Join with stores to get store type
store_preference_with_type = store_preference \
    .join(
        stores_df.select("id", "type", "latitude", "longitude"),
        F.col("primary_store_id") == F.col("id"),
        "left"
    ) \
    .select(
        "client_id",
        "primary_store_id",
        F.col("type").alias("primary_store_type"),
        "primary_store_transaction_count",
        "total_quantity",
        "latitude",
        "longitude"
    )

# Calculate store loyalty (percentage of transactions at primary store)
total_transactions_per_client = transactions_df \
    .groupBy("client_id") \
    .agg(F.count("transaction_id").alias("total_transactions"))

store_loyalty = store_preference_with_type \
    .join(total_transactions_per_client, "client_id", "left") \
    .withColumn(
        "store_loyalty_score",
        (F.col("primary_store_transaction_count") / F.col("total_transactions") * 100).cast("decimal(5,2)")
    ) \
    .withColumn(
        "store_diversity",
        F.lit(None)  # Will be calculated separately if needed
    )

# COMMAND ----------

# DBTITLE 1,Combine All Customer Metrics
# Join all customer metrics together
customer_silver = customer_status \
    .join(store_loyalty, "client_id", "left") \
    .join(clients_df, F.col("client_id") == F.col("id"), "left") \
    .select(
        # Client identifiers
        F.col("client_id"),
        F.col("name").alias("client_name"),
        F.col("job").alias("client_job"),
        F.col("email").alias("client_email"),
        F.col("account_id"),
        
        # RFM Metrics
        "recency_days",
        "frequency",
        "monetary_value",
        "recency_score",
        "frequency_score",
        "monetary_score",
        "rfm_segment",
        
        # Customer Status
        "customer_status",
        "customer_lifecycle_stage",
        "is_repeat_customer",
        "first_purchase_date",
        "last_purchase_date",
        "avg_quantity_per_transaction",
        
        # Store Preference
        "primary_store_id",
        "primary_store_type",
        "primary_store_transaction_count",
        "store_loyalty_score",
        "total_transactions",
        
        # Metadata
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at")
    )

# Write to Silver layer (Azure or local Parquet)
if RUN_LOCAL:
    LOCAL_SILVER_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    output_path = str(LOCAL_SILVER_OUTPUT_ROOT / "customer_silver")
else:
    output_path = f"{silver_path}customer_silver/"

customer_silver.write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(output_path)

if RUN_LOCAL:
    print(f"✅ Silver customer layer written locally: {output_path}")
else:
    print(f"✅ Silver customer layer written to Azure Blob Storage: {output_path}")
print(f"   Total records: {customer_silver.count()}")

# Display Sample Results
print("=== Sample Customer Silver Data ===")
customer_silver.limit(100).show(truncate=False)

# Summary Statistics
print("\n=== Customer Segmentation Distribution ===")
customer_silver.groupBy("rfm_segment").count().orderBy(F.desc("count")).show()

print("\n=== Customer Status Distribution ===")
customer_silver.groupBy("customer_status").count().orderBy(F.desc("count")).show()

print("\n=== Store Type Preference Distribution ===")
customer_silver.groupBy("primary_store_type").count().orderBy(F.desc("count")).show()

# Stop Spark session
spark.stop()
print("\n✅ Silver layer processing completed")
