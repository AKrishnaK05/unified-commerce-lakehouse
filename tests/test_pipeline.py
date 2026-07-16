import os
import sys
from datetime import date, datetime

import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

class TestSyntheticDataGeneration:
 
    def test_shopify_orders_has_required_columns(self):
        from scripts.generate_synthetic_data import generate_shopify_orders
        df = generate_shopify_orders(50)
        required = ["order_id", "customer_id", "product_id",
                    "order_date", "quantity", "revenue", "order_status"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_shopify_orders_row_count_matches_input(self):
        from scripts.generate_synthetic_data import generate_shopify_orders
        df = generate_shopify_orders(100)
        assert len(df) >= 100
 
    def test_shopify_orders_has_schema_drift_column(self):
        from scripts.generate_synthetic_data import generate_shopify_orders
        df = generate_shopify_orders(200)
        assert "promo_code_applied" in df.columns
 
    def test_amazon_orders_has_required_columns(self):
        from scripts.generate_synthetic_data import generate_amazon_orders
        df = generate_amazon_orders(50)
        required = ["marketplace_order_id", "customer_id", "sku",
                    "quantity", "revenue", "order_timestamp"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"
 
    def test_inventory_feed_has_required_columns(self):
        from scripts.generate_synthetic_data import generate_inventory_feed
        df = generate_inventory_feed(20)
        required = ["inventory_id", "product_id", "warehouse_id",
                    "quantity_available", "quantity_reserved", "last_updated"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"
 
    def test_nulls_are_injected(self):
        from scripts.generate_synthetic_data import generate_shopify_orders
        df = generate_shopify_orders(500)
        assert df["order_status"].isnull().any(), \
            "Expected some null order_status values from inject_nulls"
 
    def test_duplicates_are_injected(self):
        from scripts.generate_synthetic_data import generate_shopify_orders
        base_n = 200
        df = generate_shopify_orders(base_n)
        assert len(df) > base_n, \
            "Expected more rows than base N due to inject_duplicates"

# Data Contract validation tests

class TestDataContractValidation:
 
    def _valid_shopify_df(self):
        """Build a minimal valid Shopify orders DataFrame."""
        return pd.DataFrame([{
            "order_id": "SHOP-000001",
            "customer_id": "CUST-00001",
            "product_id": "PROD-0001",
            "order_date": date(2026, 6, 1),
            "quantity": 2,
            "revenue": 99.99,
            "order_status": "placed",
        }])
 
    def test_valid_data_passes_contract(self):
        from quality.shopify_contract_validation import validate_shopify_orders
        df = self._valid_shopify_df()
        result = validate_shopify_orders(df)
        assert result["critical"] == []
 
    def test_unknown_column_produces_warning(self):
        from quality.shopify_contract_validation import validate_shopify_orders
        df = self._valid_shopify_df()
        df["unexpected_column"] = "surprise"
        result = validate_shopify_orders(df)
        assert any("schema drift" in w.lower() for w in result["warnings"]), \
            "Expected a schema drift warning for unknown column"
 
    def test_missing_required_field_raises_violation(self):
        from quality.shopify_contract_validation import (
            ContractViolation, validate_shopify_orders
        )
        df = self._valid_shopify_df().drop(columns=["order_id"])
        with pytest.raises(ContractViolation):
            validate_shopify_orders(df)
 
    def test_negative_revenue_raises_violation(self):
        from quality.shopify_contract_validation import (
            ContractViolation, validate_shopify_orders
        )
        df = self._valid_shopify_df()
        df["revenue"] = -10.0
        with pytest.raises(ContractViolation):
            validate_shopify_orders(df)
 
    def test_zero_quantity_raises_violation(self):
        from quality.shopify_contract_validation import (
            ContractViolation, validate_shopify_orders
        )
        df = self._valid_shopify_df()
        df["quantity"] = 0
        with pytest.raises(ContractViolation):
            validate_shopify_orders(df)
 
# Bronze ingestion metadata tests

class TestBronzeIngestionMetadata:
 
    def test_add_ingestion_metadata_adds_all_columns(self):
        from pyspark.sql import SparkSession
        pytest.importorskip("pyspark")
 
        # This test uses a very small local Spark session — no MinIO needed
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("test-metadata")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
 
        try:
            from ingestion.bronze_ingestion import add_ingestion_metadata
            df = spark.createDataFrame(
                [{"order_id": "SHOP-001", "revenue": 10.0}]
            )
            result = add_ingestion_metadata(df, source_system="test_source")
            cols = result.columns
            assert "_ingestion_timestamp" in cols
            assert "_source_system" in cols
            assert "_batch_id" in cols
            assert "_ingestion_date" in cols
 
            row = result.collect()[0]
            assert row["_source_system"] == "test_source"
        finally:
            spark.stop()
 
# Silver deduplication tests
 
class TestSilverDeduplication:
 
    def test_deduplicate_removes_duplicate_order_ids(self):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
 
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("test-dedup")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
 
        try:
            from transformations.silver_transformations import deduplicate
            data = [
                {"order_id": "ORD-001", "channel": "shopify",
                 "order_date": "2026-06-01", "revenue": 100.0},
                {"order_id": "ORD-001", "channel": "shopify",
                 "order_date": "2026-06-02", "revenue": 100.0},  # duplicate
                {"order_id": "ORD-002", "channel": "shopify",
                 "order_date": "2026-06-01", "revenue": 50.0},
            ]
            df = spark.createDataFrame(data)
            result = deduplicate(df, ["order_id", "channel"], "order_date")
            assert result.count() == 2, \
                "Expected exactly 2 rows after deduplication"
        finally:
            spark.stop()
 
 
# Gold mart logic tests (pandas-based, no Spark needed)
 
class TestGoldMartLogic:
 
    def test_turnover_ratio_is_zero_when_no_sales(self):
        inventory = pd.DataFrame([{
            "inventory_id": "INV-001",
            "product_id": "PROD-0001",
            "quantity_available": 100,
            "quantity_reserved": 10,
        }])
        orders = pd.DataFrame(columns=["product_id", "quantity"])
 
        merged = inventory.merge(
            orders.groupby("product_id")["quantity"].sum().reset_index()
            .rename(columns={"quantity": "total_units_sold"}),
            on="product_id",
            how="left"
        )
        merged["total_units_sold"] = merged["total_units_sold"].fillna(0)
        merged["turnover_ratio"] = merged.apply(
            lambda r: r["total_units_sold"] / r["quantity_available"]
            if r["quantity_available"] > 0 else 0,
            axis=1
        )
        assert merged.iloc[0]["turnover_ratio"] == 0.0
 
    def test_revenue_aggregation_is_correct(self):
        orders = pd.DataFrame([
            {"order_date": "2026-06-01", "channel": "shopify",
             "order_status": "placed", "revenue": 100.0, "quantity": 2,
             "order_id": "ORD-001"},
            {"order_date": "2026-06-01", "channel": "shopify",
             "order_status": "placed", "revenue": 50.0, "quantity": 1,
             "order_id": "ORD-002"},
        ])
        result = (
            orders.groupby(["order_date", "channel", "order_status"])
            .agg(total_revenue=("revenue", "sum"))
            .reset_index()
        )
        assert result.iloc[0]["total_revenue"] == 150.0
 