import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from ingestion.spark_session import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

HIVE_METASTORE_URI = os.environ.get(
    "HIVE_METASTORE_URI", "thrift://localhost:9083"
)

def get_catalog_spark_session() -> SparkSession:
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
 
    builder = (
        SparkSession.builder
        .appName("catalog-registration")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI)
        .enableHiveSupport()
    )
 
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ],
    ).getOrCreate()

def register_databases(spark: SparkSession) -> None:
    for db in ["bronze", "silver", "gold"]:
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {db}
            COMMENT 'Unified Commerce Lakehouse — {db} layer'
            LOCATION '/opt/hive/warehouse/{db}.db'
        """)
        print(f"  Database '{db}' registered")
 
 
def register_bronze_tables(spark: SparkSession) -> None:
    print("Registering Bronze tables...")
 
    tables = {
        "shopify_orders": {
            "path": "s3a://bronze/shopify_orders",
            "comment": "Raw Shopify Orders — ingested daily, preserved as-is",
        },
        "amazon_orders": {
            "path": "s3a://bronze/amazon_orders",
            "comment": "Raw Amazon Marketplace Orders — ingested daily",
        },
        "inventory_feed": {
            "path": "s3a://bronze/inventory_feed",
            "comment": "Raw SFTP Inventory Feed — ingested daily",
        },
    }
 
    for table_name, meta in tables.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze.{table_name}
            USING DELTA
            COMMENT '{meta["comment"]}'
            LOCATION '{meta["path"]}'
        """)
        print(f"  bronze.{table_name} registered")
 
 
def register_silver_tables(spark: SparkSession) -> None:
    print("Registering Silver tables...")
 
    tables = {
        "orders": {
            "path": "s3a://silver/orders",
            "comment": "Canonical orders — conformed from Shopify + Amazon, deduplicated",
        },
        "customers": {
            "path": "s3a://silver/customers",
            "comment": "Canonical customer dimension — distinct customer IDs from all channels",
        },
        "products": {
            "path": "s3a://silver/products",
            "comment": "Canonical product dimension — distinct product IDs from all sources",
        },
        "inventory": {
            "path": "s3a://silver/inventory",
            "comment": "Cleaned inventory positions from SFTP feed",
        },
    }
 
    for table_name, meta in tables.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS silver.{table_name}
            USING DELTA
            COMMENT '{meta["comment"]}'
            LOCATION '{meta["path"]}'
        """)
        print(f"  silver.{table_name} registered")
 
 
def register_gold_tables(spark: SparkSession) -> None:
    print("Registering Gold tables...")
 
    tables = {
        "revenue_mart": {
            "path": "s3a://gold/revenue_mart",
            "comment": "Daily revenue by channel and order status — owner: Analytics Team",
        },
        "channel_performance_mart": {
            "path": "s3a://gold/channel_performance_mart",
            "comment": "Monthly Shopify vs Amazon performance comparison — owner: Analytics Team",
        },
        "customer_360_mart": {
            "path": "s3a://gold/customer_360_mart",
            "comment": "Unified customer lifetime metrics — owner: CRM Team",
        },
        "inventory_turnover_mart": {
            "path": "s3a://gold/inventory_turnover_mart",
            "comment": "Stock turnover metrics per product and warehouse — owner: Supply Chain Team",
        },
    }
 
    for table_name, meta in tables.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold.{table_name}
            USING DELTA
            COMMENT '{meta["comment"]}'
            LOCATION '{meta["path"]}'
        """)
        print(f"  gold.{table_name} registered")
 
 
def show_catalog_summary(spark: SparkSession) -> None:
    print("\nCatalog summary:")
    for db in ["bronze", "silver", "gold"]:
        tables = spark.sql(f"SHOW TABLES IN {db}").collect()
        print(f"  {db}: {len(tables)} table(s)")
        for row in tables:
            print(f"    - {row.tableName}")
 
 
def main():
    print("Starting Hive Metastore catalog registration...\n")
    spark = get_catalog_spark_session()
 
    try:
        register_databases(spark)
        register_bronze_tables(spark)
        register_silver_tables(spark)
        register_gold_tables(spark)
        show_catalog_summary(spark)
        print("\nCatalog registration complete.")
    finally:
        spark.stop()
 
 
if __name__ == "__main__":
    main()
 