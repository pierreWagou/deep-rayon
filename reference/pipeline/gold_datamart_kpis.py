# Python script for Gold Layer: KPI Datamart
"""
This script creates the gold layer datamart with the following KPIs:
- Basket analysis per store
- Product trend per store
- Number of clients per store

Reads from Azure Blob Storage (silver layer) and writes to Azure Blob Storage (gold layer)

Local execution (reads CSV from --data-dir as bronze-style sources; no Azure):
  python gold_datamart_kpis.py --local
  python gold_datamart_kpis.py --local --data-dir /path/to/data
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
        description="Gold KPI datamart: Azure Blob (silver Parquet) or local CSV."
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run on local CSV files under --data-dir (no Azure). Writes Parquet under output/gold.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Directory with *_500k.csv files (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Local gold output root (default: <repo>/output/gold when --local)",
    )
    return parser.parse_args()


_cli = parse_cli_args()
RUN_LOCAL = _cli.local
DATA_DIR = _cli.data_dir.resolve()
LOCAL_GOLD_OUTPUT_ROOT = (
    (_cli.output_dir.resolve() if _cli.output_dir else SCRIPT_DIR.parent / "output" / "gold")
    if RUN_LOCAL
    else None
)

# Configuration from environment variables (Azure mode)
storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "yourstorageaccount")
container_name_silver = os.getenv("AZURE_CONTAINER_SILVER", "silver")
container_name_gold = os.getenv("AZURE_CONTAINER_GOLD", "gold")
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")

# Paths in blob storage (Azure only)
silver_path = f"abfss://{container_name_silver}@{storage_account_name}.dfs.core.windows.net/"
gold_path = f"abfss://{container_name_gold}@{storage_account_name}.dfs.core.windows.net/"

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

# Initialize Spark session
_spark_jvm_opts = "-Dlog4j2.disable.jmx=true"
spark_builder = (
    SparkSession.builder.appName("GoldDatamartKPIs")
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

# Load source tables
if RUN_LOCAL:
    transactions_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(str(DATA_DIR / "transactions_500k.csv"))
    )
    stores_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(str(DATA_DIR / "stores_500k.csv"))
    )
    products_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(str(DATA_DIR / "products_500k.csv"))
    )
    clients_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(str(DATA_DIR / "clients_500k.csv"))
    )
    print(f"✅ Tables loaded from local CSV: {DATA_DIR}")
else:
    transactions_df = spark.read.format("parquet").load(f"{silver_path}transactions/*.parquet")
    stores_df = spark.read.format("parquet").load(f"{silver_path}stores/*.parquet")
    products_df = spark.read.format("parquet").load(f"{silver_path}products/*.parquet")
    clients_df = spark.read.format("parquet").load(f"{silver_path}clients/*.parquet")
    print("✅ Silver tables loaded successfully from Azure Blob Storage")

# COMMAND ----------

# DBTITLE 1,Basket Analysis per Store
# Calculate basket metrics aggregated by store
basket_analysis = transactions_df \
    .withColumn("transaction_date", F.to_date("date", "yyyy-MM-dd")) \
    .groupBy("store_id", "transaction_id", "transaction_date") \
    .agg(
        F.sum("quantity").alias("basket_size"),
        F.count("product_id").alias("basket_item_count"),
        F.collect_list("product_id").alias("product_ids")
    ) \
    .groupBy("store_id") \
    .agg(
        # Average basket metrics
        F.avg("basket_size").alias("avg_basket_size"),
        F.avg("basket_item_count").alias("avg_basket_item_count"),
        F.stddev("basket_size").alias("stddev_basket_size"),
        
        # Min/Max basket metrics
        F.min("basket_size").alias("min_basket_size"),
        F.max("basket_size").alias("max_basket_size"),
        
        # Total transactions
        F.count("transaction_id").alias("total_transactions"),
        
        # Basket diversity (average unique products per basket)
        F.avg("basket_item_count").alias("avg_unique_items_per_basket")
    )

# Join with store information
basket_analysis_per_store = basket_analysis \
    .join(
        stores_df.select("id", "type", "latitude", "longitude", "opening", "closing"),
        F.col("store_id") == F.col("id"),
        "left"
    ) \
    .select(
        "store_id",
        "type",
        "latitude",
        "longitude",
        "opening",
        "closing",
        "avg_basket_size",
        "avg_basket_item_count",
        "stddev_basket_size",
        "min_basket_size",
        "max_basket_size",
        "total_transactions",
        "avg_unique_items_per_basket",
        F.current_timestamp().alias("calculated_at")
    )

# COMMAND ----------

# DBTITLE 1,Product Trend per Store
# Calculate product sales trends over time (last 30, 60, 90 days)
current_date = datetime.now().date()
date_30_days_ago = (datetime.now() - timedelta(days=30)).date()
date_60_days_ago = (datetime.now() - timedelta(days=60)).date()
date_90_days_ago = (datetime.now() - timedelta(days=90)).date()

# Calculate product sales in different time periods
product_trends = transactions_df \
    .withColumn("transaction_date", F.to_date("date", "yyyy-MM-dd")) \
    .withColumn(
        "period_30d",
        F.when(F.col("transaction_date") >= F.lit(date_30_days_ago), F.col("quantity")).otherwise(0)
    ) \
    .withColumn(
        "period_60d",
        F.when(F.col("transaction_date") >= F.lit(date_60_days_ago), F.col("quantity")).otherwise(0)
    ) \
    .withColumn(
        "period_90d",
        F.when(F.col("transaction_date") >= F.lit(date_90_days_ago), F.col("quantity")).otherwise(0)
    ) \
    .groupBy("store_id", "product_id") \
    .agg(
        F.sum("period_30d").alias("sales_30d"),
        F.sum("period_60d").alias("sales_60d"),
        F.sum("period_90d").alias("sales_90d"),
        F.sum("quantity").alias("total_sales_all_time"),
        F.count("transaction_id").alias("transaction_count")
    )

# Calculate trend indicators
product_trend_analysis = product_trends \
    .withColumn(
        "trend_30d_vs_60d",
        F.when(F.col("sales_60d") > 0, 
               ((F.col("sales_30d") - (F.col("sales_60d") - F.col("sales_30d"))) / (F.col("sales_60d") - F.col("sales_30d")) * 100)
        )
    ) \
    .withColumn(
        "trend_direction",
        F.when(F.col("sales_30d") > (F.col("sales_60d") - F.col("sales_30d")), "Increasing")
         .when(F.col("sales_30d") < (F.col("sales_60d") - F.col("sales_30d")), "Decreasing")
         .otherwise("Stable")
    ) \
    .withColumn(
        "sales_velocity_30d",
        F.col("sales_30d") / 30.0  # Units per day
    )

# Join with product and store information
product_trend_per_store = product_trend_analysis \
    .join(
        products_df.select("id", "brand", "ean"),
        F.col("product_id") == F.col("id"),
        "left"
    ) \
    .join(
        stores_df.select("id", "type"),
        F.col("store_id") == F.col("product_id"),
        "left"
    ) \
    .select(
        "store_id",
        F.col("type").alias("store_type"),
        "product_id",
        "brand",
        "ean",
        "sales_30d",
        "sales_60d",
        "sales_90d",
        "total_sales_all_time",
        "transaction_count",
        "trend_direction",
        "trend_30d_vs_60d",
        "sales_velocity_30d",
        F.current_timestamp().alias("calculated_at")
    )

# COMMAND ----------

# DBTITLE 1,Number of Clients per Store
# Count unique clients per store with additional metrics
clients_per_store = transactions_df \
    .groupBy("store_id") \
    .agg(
        F.countDistinct("client_id").alias("nb_clients"),
        F.count("transaction_id").alias("total_transactions"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.avg("quantity").alias("avg_quantity_per_transaction")
    ) \
    .withColumn(
        "avg_transactions_per_client",
        F.col("total_transactions") / F.col("nb_clients")
    )

# Join with store information
nb_clients_per_store = clients_per_store \
    .join(
        stores_df.select("id", "type", "latitude", "longitude", "opening", "closing"),
        F.col("store_id") == F.col("id"),
        "left"
    ) \
    .select(
        "store_id",
        "type",
        "latitude",
        "longitude",
        "opening",
        "closing",
        "nb_clients",
        "total_transactions",
        "total_quantity_sold",
        "avg_quantity_per_transaction",
        "avg_transactions_per_client",
        F.current_timestamp().alias("calculated_at")
    )

# Write Gold outputs (Azure or local)
if RUN_LOCAL:
    LOCAL_GOLD_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    basket_output_path = str(LOCAL_GOLD_OUTPUT_ROOT / "basket_analysis_per_store")
    product_trend_output_path = str(LOCAL_GOLD_OUTPUT_ROOT / "product_trend_per_store")
    clients_per_store_output_path = str(LOCAL_GOLD_OUTPUT_ROOT / "nb_clients_per_store")
else:
    basket_output_path = f"{gold_path}basket_analysis_per_store/"
    product_trend_output_path = f"{gold_path}product_trend_per_store/"
    clients_per_store_output_path = f"{gold_path}nb_clients_per_store/"

basket_analysis_per_store.write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(basket_output_path)
if RUN_LOCAL:
    print(f"✅ Basket analysis written locally: {basket_output_path}")
else:
    print(f"✅ Basket analysis written to Azure Blob Storage: {basket_output_path}")
print(f"   Total stores: {basket_analysis_per_store.count()}")

product_trend_per_store.write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(product_trend_output_path)
if RUN_LOCAL:
    print(f"✅ Product trend written locally: {product_trend_output_path}")
else:
    print(f"✅ Product trend written to Azure Blob Storage: {product_trend_output_path}")
print(f"   Total records: {product_trend_per_store.count()}")

nb_clients_per_store.write.format("parquet").mode("overwrite").option("overwriteSchema", "true").save(clients_per_store_output_path)
if RUN_LOCAL:
    print(f"✅ Number of clients per store written locally: {clients_per_store_output_path}")
else:
    print(f"✅ Number of clients per store written to Azure Blob Storage: {clients_per_store_output_path}")
print(f"   Total stores: {nb_clients_per_store.count()}")

# Display Sample Results
print("=== Sample Basket Analysis per Store ===")
basket_analysis_per_store.limit(20).show(truncate=False)

print("\n=== Sample Product Trend per Store ===")
product_trend_per_store.limit(20).show(truncate=False)

print("\n=== Sample Number of Clients per Store ===")
nb_clients_per_store.limit(20).show(truncate=False)

# Summary Statistics
print("\n=== Basket Analysis Summary by Store Type ===")
basket_analysis_per_store.groupBy("type").agg(
    F.avg("avg_basket_size").alias("avg_basket_size"),
    F.avg("avg_basket_item_count").alias("avg_items_per_basket"),
    F.sum("total_transactions").alias("total_transactions")
).show()

print("\n=== Top 10 Stores by Number of Clients ===")
nb_clients_per_store.orderBy(F.desc("nb_clients")).limit(10).show()

print("\n=== Product Trend Distribution ===")
product_trend_per_store.groupBy("trend_direction").count().show()

# Stop Spark session
spark.stop()
print("\n✅ Gold layer processing completed")
