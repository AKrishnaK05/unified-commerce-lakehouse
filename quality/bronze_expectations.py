# Great Expectations validation suite for the Bronze layer.

from logging import critical
import os
import sys

import great_expectations as gx
import pandas as pd
from deltalake import DeltaTable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio@ak")

STORAGE_OPTIONS = {
    "endpoint_url": MINIO_ENDPOINT,
    "access_key_id": MINIO_ACCESS_KEY,
    "secret_access_key": MINIO_SECRET_KEY,
    "allow_http": "true",
    "aws_s3_allow_unsafe_rename": "true",
}

class BronzeValidationError(Exception):
    """Raised when a Critical Bronze expectation fails."""

def read_bronze_table(table_name: str) -> pd.DataFrame:
    path = f"s3://bronze/{table_name}"
    dt = DeltaTable(path, storage_options=STORAGE_OPTIONS)
    return dt.to_pandas()

def validate_bronze_shopify_orders() -> dict:
    print("Validating bronze.shopify_orders...")
    df = read_bronze_table("shopify_orders")

    context = gx.get_context(mode="ephemeral")
    ds = context.sources.add_pandas("shopify_bronze")
    asset = ds.add_dataframe_asset("shopify_orders")
    batch = asset.build_batch_request(dataframe=df)
    suite = context.add_expectation_suite("bronze_shopify_orders")

    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite,
    )

    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_be_between(
        "revenue", min_value=0, mostly=0.99
    )
    validator.expect_column_values_to_be_between(
        "quantity", min_value=1, mostly=0.99
    )
    for col in ["_ingestion_timestamp", "_source_system", "_batch_id", "_ingestion_date"]:
        validator.expect_column_to_exist(col)
        validator.expect_column_values_to_not_be_null(col)

    results = validator.validate()
    return _parse_results("bronze.shopify_orders", results)

def validate_bronze_amazon_orders() -> dict:
    print("  Validating bronze.amazon_orders...")
    df = read_bronze_table("amazon_orders")
 
    context = gx.get_context(mode="ephemeral")
    ds = context.sources.add_pandas("amazon_bronze")
    asset = ds.add_dataframe_asset("amazon_orders")
    batch = asset.build_batch_request(dataframe=df)
    suite = context.add_expectation_suite("bronze_amazon_orders")
 
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite,
    )
 
    validator.expect_column_values_to_not_be_null("marketplace_order_id")
    validator.expect_column_values_to_be_between(
        "revenue", min_value=0, mostly=0.99
    )
    for col in ["_ingestion_timestamp", "_source_system", "_batch_id", "_ingestion_date"]:
        validator.expect_column_to_exist(col)
        validator.expect_column_values_to_not_be_null(col)
 
    results = validator.validate()
    return _parse_results("bronze.amazon_orders", results)

def validate_bronze_inventory_feed() -> dict:
    print("  Validating bronze.inventory_feed...")
    df = read_bronze_table("inventory_feed")
 
    context = gx.get_context(mode="ephemeral")
    ds = context.sources.add_pandas("inventory_bronze")
    asset = ds.add_dataframe_asset("inventory_feed")
    batch = asset.build_batch_request(dataframe=df)
    suite = context.add_expectation_suite("bronze_inventory_feed")
 
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite,
    )
 
    validator.expect_column_values_to_not_be_null("inventory_id")
    validator.expect_column_values_to_be_between(
        "quantity_available", min_value=0, mostly=0.99
    )
    for col in ["_ingestion_timestamp", "_source_system", "_batch_id", "_ingestion_date"]:
        validator.expect_column_to_exist(col)
        validator.expect_column_values_to_not_be_null(col)
 
    results = validator.validate()
    return _parse_results("bronze.inventory_feed", results)

def _parse_results(table_name: str, results) -> dict:
    critical = []
    warnings = []

    for r in results.results:
        expectation_type = r.expectation_config.expectation_type
        success = r.success
        column = r.expectation_config.kwargs.get("column", "table-level")

        if not success:
            msg = f"{expectation_type} failed on column '{column}'"
            if "not_be_null" in expectation_type or "to_exist" in expectation_type:
                critical.append(msg)
            else:
                warnings.append(msg)

    status = "PASSED" if not critical else "FAILED"
    print(f"{table_name}: {status}"
          f"({len(critical)} critical, {len(warnings)} warnings)")

    return {
        "table": table_name,
        "status": status,
        "critical": critical,
        "warnings": warnings,
    }

def run_bronze_validations() -> list[dict]:
    print("Running Bronze DQ validations...")
    results = [
        validate_bronze_shopify_orders(),
        validate_bronze_amazon_orders(),
        validate_bronze_inventory_feed(),
    ]
 
    critical_failures = [r for r in results if r["status"] == "FAILED"]
    if critical_failures:
        failed_tables = [r["table"] for r in critical_failures]
        raise BronzeValidationError(
            f"Critical Bronze DQ failures on: {failed_tables}"
        )
 
    return results
 
 
if __name__ == "__main__":
    results = run_bronze_validations()
    print("\nBronze validation summary:")
    for r in results:
        print(f"  {r['table']}: {r['status']}")
        if r["warnings"]:
            for w in r["warnings"]:
                print(f"    WARNING: {w}")
 