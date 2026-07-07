from pandas._libs.tslibs.offsets import INVALID_FREQ_ERR_MSG
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__),".."))
from ingestion.spark_session import bronze_path, get_spark_session, silver_path

# Deduplication helper

def deduplicate(df, partition_cols: list[str], order_col: str):
    window = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

# Canonical entity transformations

def build_orders(spark: SparkSession) -> None:
    print("--- Silver: orders ---")

    shopify = spark.read.format("delta").load(bronze_path("shopify_orders"))
    amazon = spark.read.format("delta").load(bronze_path("amazon_orders"))

    shopify_clean = (
        shopify
        .withColumnRenamed("order_id", "order_id")
        .withColumn("customer_id", F.col("customer_id"))
        .withColumn("product_id", F.col("product_id"))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("quantity", F.col("quantity").cast("integer"))
        .withColumn("revenue", F.col("revenue").cast("double"))
        .withColumn("order_status",
            F.when(F.col("order_status").isNull(), F.lit("unknown"))
            .otherwise(F.col("order_status"))
        )
        .withColumn("channel", F.lit("shopify"))
        .select(
            "order_id", "customer_id", "product_id",
            "order_date", "quantity", "revenue", "order_status", "channel"
        )
    )

    amazon_clean = (
        amazon
        .withColumnRenamed("marketplace_order_id", "order_id")
        .withColumnRenamed("sku", "product_id")
        .withColumn("order_date", F.to_date(F.col("order_timestamp")))
        .withColumn("quantity", F.col("quantity").cast("integer"))
        .withColumn("revenue", F.col("revenue").cast("double"))
        .withColumn("order_status", F.lit("unknown"))  # Amazon source has no status field
        .withColumn("channel", F.lit("amazon"))
        .withColumn("customer_id",
            F.when(F.col("customer_id").isNull(), F.lit("unknown"))
             .otherwise(F.col("customer_id"))
        )
        .select(
            "order_id", "customer_id", "product_id",
            "order_date", "quantity", "revenue", "order_status", "channel"
        )
    )

    orders = shopify_clean.unionByName(amazon_clean)

    orders = deduplicate(orders, partition_cols=["order_id", "channel"], order_col="order_date")

    orders = orders.filter(F.col("order_id").isNotNull())

    (
        orders.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_date")
        .save(silver_path("orders"))
    )
    print(f"Written {orders.count()} rows to {silver_path('orders')}")

def build_customers(spark: SparkSession) -> None:
    print("--- Silver: customers ---")

    shopify = spark.read.format("delta").load(bronze_path("shopify_orders"))
    amazon = spark.read.format("delta").load(bronze_path("amazon_orders"))

    shopify_customers = (
        shopify
        .select("customer_id")
        .withColumn("source", F.lit("shopify"))
    )

    amazon_customers = (
        amazon
        .select("customer_id")
        .withColumn("source", F.lit("amazon"))
        .filter(F.col("customer_id").isNotNull())
    )

    customers = (
        shopify_customers.unionByName(amazon_customers)
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .withColumn("_created_at", F.current_timestamp())
    )

    (
        customers.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path("customers"))
    )
    print(f"Written {customers.count()} rows to {silver_path('customers')}")

def build_products(spark: SparkSession) -> None:
    print("--- Silver: products ---")

    shopify = spark.read.format("delta").load(bronze_path("shopify_orders"))
    amazon = spark.read.format("delta").load(bronze_path("amazon_orders"))
    inventory = spark.read.format("delta").load(bronze_path("inventory_feed"))

    shopify_products = (
        shopify
        .select(F.col("product_id"))
        .withColumn("source", F.lit("shopify"))
    )

    amazon_products = (
        amazon
        .select(F.col("sku").alias("product_id"))
        .withColumn("source", F.lit("amazon"))
    )

    inventory_products = (
        inventory
        .select(F.col("product_id"))
        .withColumn("source", F.lit("inventory"))
    )

    products = (
        shopify_products
        .unionByName(amazon_products)
        .unionByName(inventory_products)
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
        .withColumn("_created_at", F.current_timestamp())
    )

    (
        products.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path("products"))
    )
    print(f"Written {products.count()} rows to {silver_path('products')}")

def build_inventory(spark: SparkSession) -> None:
    print("--- Silver: inventory ---")

    inventory = spark.read.format("delta").load(bronze_path("inventory_feed"))

    inventory_clean = (
        inventory
        .withColumn("quantity_available", F.col("quantity_available").cast("integer"))
        .withColumn("quantity_reserved", F.when(F.col("quantity_reserved").isNull(), F.lit(0))
                                          .otherwise(F.col("quantity_reserved").cast("integer")))
        .withColumn("last_updated", F.to_timestamp(F.col("last_updated")))
        .withColumn("order_date", F.to_date(F.col("last_updated")))
        .filter(F.col("inventory_id").isNotNull())
        .dropDuplicates(["inventory_id"])
        .select(
            "inventory_id", "product_id", "warehouse_id",
            "quantity_available", "quantity_reserved",
            "last_updated", "order_date"
        )
    )

    (
        inventory_clean.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_date")
        .save(silver_path("inventory"))
    )
    print(f"Written {inventory_clean.count()} rows to {silver_path('inventory')}")

# Main

def main():
    print("Starting Silver transformations\n")
    spark = get_spark_session("silver-transformations")

    try:
        build_orders(spark)
        build_customers(spark)
        build_products(spark)
        build_inventory(spark)
        print("\nSilver transformations complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()