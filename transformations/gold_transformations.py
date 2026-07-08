from typing import final
from traceback import print_tb
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from ingestion.spark_session import get_spark_session, gold_path, silver_path

# Gold mart builders

def build_revenue_mart(spark: SparkSession) -> None:
    # Answers: "What was total revenue on any given day, broken down by channel and order status?"
    print("--- Gold: revenue_mart ---")

    orders = spark.read.format("delta").load(silver_path("orders"))

    revenue_mart = (
        orders
        .groupBy("order_date", "channel", "order_status")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("quantity").alias("total_quantity"),
            F.count("order_id").alias("order_count"),
            F.avg("revenue").alias("avg_order_value"),
        )
        .withColumn("order_month", F.date_format(F.col("order_date"), "yyyy-MM"))
        .filter(F.col("total_revenue") >= 0)
    )

    (
        revenue_mart.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_month")
        .save(gold_path("revenue_mart"))
    )
    print(f"Written {revenue_mart.count()} rows to {gold_path('revenue_mart')}")

def build_channel_performance_mart(spark: SparkSession) -> None:
    # Answers: "Which channel is performing better, and how does that compare month over month?"
    print("--- Gold: channel_performance_mart ---")

    orders = spark.read.format("delta").load(silver_path("orders"))

    channel_mart = (
        orders
        .withColumn("order_month", F.date_format(F.col("order_date"), "yyyy-MM"))
        .groupBy("order_month", "channel")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.count("order_id").alias("total_orders"),
            F.avg("revenue").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("order_month", "channel")
    )

    (
        channel_mart.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_month")
        .save(gold_path("channel_performance_mart"))
    )
    print(f"Written {channel_mart.count()} rows to {gold_path('channel_performance_mart')}")

def build_customer_360_mart(spark: SparkSession) -> None:
    # Answers: "Who are our customers, how much have they spent total, how many orders have they placed, and when did they last order?"
    print("--- Gold: customer_360_mart ---")

    orders = spark.read.format("delta").load(silver_path("orders"))
    customers = spark.read.format("delta").load(silver_path("customers"))

    customer_metrics = (
        orders
        .filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.sum("revenue").alias("lifetime_revenue"),
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units_purchased"),
            F.avg("revenue").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("channel").alias("channels_used"),
        )
    )

    customer_360 = (
        customers
        .join(customer_metrics, on="customer_id", how="left")
        .withColumn("order_month", F.date_format(F.col("last_order_date"), "yyyy-MM"))
        .withColumn("lifetime_revenue", F.coalesce(F.col("lifetime_revenue"), F.lit(0.0)))
        .withColumn("total_orders", F.coalesce(F.col("total_orders"), F.lit(0)))
    )

    (
        customer_360.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_month")
        .save(gold_path("customer_360_mart"))
    )
    print(f"Written {customer_360.count()} rows to {gold_path('customer_360_mart')}")

def build_inventory_turnover_mart(spark: SparkSession) -> None:
    # Answers: "How fast is inventory moving? Which products/warehouse have the most/least stock relative to what's being sold?"
    print("--- Gold: inventory_turnover_mart ---")

    inventory = spark.read.format("delta").load(silver_path("inventory"))
    orders = spark.read.format("delta").load(silver_path("orders"))

    units_sold = (
        orders
        .groupBy("product_id")
        .agg(F.sum("quantity").alias("total_units_sold"))
    )

    inventory_turnover = (
        inventory
        .join(units_sold, on="product_id", how="left")
        .withColumn("total_units_sold", F.coalesce(F.col("total_units_sold"), F.lit(0)))
        .withColumn("net_available", F.col("quantity_available") - F.col("quantity_reserved"))
        .withColumn("turnover_ratio", F.when(F.col("quantity_available") > 0, F.col("total_units_sold")/F.col("quantity_available")).otherwise(F.lit(0.0)))
        .withColumn("order_month", F.date_format(F.col("last_updated"), "yyyy-MM"))
        .select(
            "inventory_id", "product_id", "warehouse_id",
            "quantity_available", "quantity_reserved", "net_available",
            "total_units_sold", "turnover_ratio",
            "last_updated", "order_month"
        )
    )

    (
        inventory_turnover.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_month")
        .save(gold_path("inventory_turnover_mart"))
    )
    print(f"Written {inventory_turnover.count()} rows to {gold_path('inventory_turnover_mart')}")

# Main

def main():
    print("Starting Gold mart builds\n")
    spark = get_spark_session("gold_transformations")

    try:
        build_revenue_mart(spark)
        build_channel_performance_mart(spark)
        build_customer_360_mart(spark)
        build_inventory_turnover_mart(spark)
        print("\nGold transformations complete.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()