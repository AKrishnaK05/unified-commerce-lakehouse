from numpy import outer
import os
import sys
import uuid
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.spark_session import bronze_path, get_spark_session
from quality.shopify_contract_validation import ContractViolation, validate_shopify_orders

# Configuration

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "scripts", "output")

SOURCES = {
    "shopify_orders": os.path.join(DATA_DIR, "shopify_orders.csv"),
    "amazon_orders": os.path.join(DATA_DIR, "amazon_orders.csv"),
    "inventory_feed": os.path.join(DATA_DIR, "inventory_feed.csv"),
}

INGESTION_TIMESTAMP = datetime.now(timezone.utc).isoformat()
BATCH_ID = str(uuid.uuid4())

# Ingestion metadat helpers

def add_ingestion_metadata(df, source_system: str):
    return (
        df
        .withColumn("_ingestion_timestamp", F.lit(INGESTION_TIMESTAMP))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_batch_id", F.lit(BATCH_ID))
        .withColumn("_ingestion_date", F.to_date(F.lit(INGESTION_TIMESTAMP)))
    )

# Per-source ingestion functions

def ingest_shopify_orders(spark: SparkSession) -> None:
    print("--- Shopify Orders ---")
    path = SOURCES["shopify_orders"]

    pdf = pd.read_csv(path)
    try:
        report = validate_shopify_orders(pdf)
        print("Contract: PASSED")
        if report["warnings"]:
            for w in report["warnings"]:
                print(f"Contract WARNING: {w}")
    except ContractViolation as e:
        print(f" Contract: FAILED - {e}")
        print(" Skipping Shopify ingestion due to contract violation.")
        return

    df = spark.createDataFrame(pdf)
    df = add_ingestion_metadata(df, source_system="shopify_orders")

    output_path = bronze_path("shopify_orders")
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingestion_date")
        .save(output_path)
    )
    print(f"Written {df.count()} rows to {output_path}")

def ingest_amazon_orders(spark: SparkSession) -> None:
    print("--- Amazon Marketplace Orders ---")
    path = SOURCES["amazon_orders"]

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df = add_ingestion_metadata(df, source_system="amazon_orders")

    output_path = bronze_path("amazon_orders")
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingestion_date")
        .save(output_path)
    )
    print(f"Written {df.count()} rows to {output_path}")

def ingest_inventory_feed(spark: SparkSession) -> None:
    print("--- SFTP Inventory Feed ---")
    path = SOURCES["inventory_feed"]

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df = add_ingestion_metadata(df, source_system="inventory_feed")

    output_path = bronze_path("inventory_feed")
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingestion_date")
        .save(output_path)
    )
    print(f"Written {df.count()} rows to {output_path}")

# Main

def main():
    print(f"Starting Bronze ingestion - batch {BATCH_ID}")
    print(f"Ingestion timestamp: {INGESTION_TIMESTAMP}\n")

    missing = [name for name, path in SOURCES.items() if not os.path.exists(path)]
    if missing:
        print(
            f"Error: Missing generated data files: {missing}\n"
            f"Run: python scripts/generate_synthetic_data.py"
        )
        sys.exit(1)

    spark = get_spark_session("bronze-ingestion")

    try:
        ingest_shopify_orders(spark)
        ingest_amazon_orders(spark)
        ingest_inventory_feed(spark)
        print("\nBronze ingestion complete.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()