# Great Expectations validation suite for the Silver layer.

import os
import sys

import great_expectations as gx
import pandas as pd
from deltalake import DeltaTable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

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

class SilverValidationError(Exception):
    """Raised when a Critical Silver expectation fails."""

def read_silver_table(table_name: str) -> pd.DataFrame:
    path = f"s3://silver/{table_name}"
    dt = DeltaTable(path, storage_options=STORAGE_OPTIONS)
    return dt.to_pandas()

def validate_silver_orders() -> dict:
    print("Validating silver.orders...")
    df = read_silver_table("orders")

    context = gx.get_context(mode="ephemeral")
    ds = context.sources.add_pandas("orders_silver")
    asset = ds.add_dataframe_asset("orders")
    batch = asset.build_batch_request(dataframe=df)
    suite = context.add_expectation_suite("silver_orders")

    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite,
    )

    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_be_unique("order_id")
    validator.expect_column_values_to_be_between(
        "revenue", min_value=0, mostly=1.0
    )
    validator.expect_column_values_to_be_between(
        "quantity", min_value=1, mostly=1.0
    )
    validator.expect_column_values_to_be_in_set(
        "channel", ["shopify", "amazon"]
    )
    validator.expect_column_values_to_not_be_null("order_date")

    results = validator.validate()
    return _parse_results("silver.orders", results)

def validate_silver_customers() -> dict:
    """Validate the Silver customers table."""
    print("  Validating silver.customers...")
    df = read_silver_table("customers")
 
    context = gx.get_context(mode="ephemeral")
    ds = context.sources.add_pandas("customers_silver")
    asset = ds.add_dataframe_asset("customers")
    batch = asset.build_batch_request(dataframe=df)
    suite = context.add_expectation_suite("silver_customers")
 
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite,
    )
 
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_unique("customer_id")
    validator.expect_column_values_to_be_in_set(
        "source", ["shopify", "amazon"]
    )
 
    results = validator.validate()
    return _parse_results("silver.customers", results)
 
 
def validate_silver_products() -> dict:
    """Validate the Silver products table."""
    print("  Validating silver.products...")
    df = read_silver_table("products")
 
    context = gx.get_context(mode="ephemeral")
    ds = context.sources.add_pandas("products_silver")
    asset = ds.add_dataframe_asset("products")
    batch = asset.build_batch_request(dataframe=df)
    suite = context.add_expectation_suite("silver_products")
 
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite,
    )
 
    validator.expect_column_values_to_not_be_null("product_id")
    validator.expect_column_values_to_be_unique("product_id")
 
    results = validator.validate()
    return _parse_results("silver.products", results)
 
 
def validate_silver_inventory() -> dict:
    """Validate the Silver inventory table."""
    print("  Validating silver.inventory...")
    df = read_silver_table("inventory")
 
    context = gx.get_context(mode="ephemeral")
    ds = context.sources.add_pandas("inventory_silver")
    asset = ds.add_dataframe_asset("inventory")
    batch = asset.build_batch_request(dataframe=df)
    suite = context.add_expectation_suite("silver_inventory")
 
    validator = context.get_validator(
        batch_request=batch,
        expectation_suite=suite,
    )
 
    validator.expect_column_values_to_not_be_null("inventory_id")
    validator.expect_column_values_to_be_between(
        "quantity_available", min_value=0, mostly=1.0
    )
    # quantity_reserved was null-filled to 0 in Silver — so must never be null here
    validator.expect_column_values_to_not_be_null("quantity_reserved")
 
    results = validator.validate()
    return _parse_results("silver.inventory", results)
 
 
def _parse_results(table_name: str, results) -> dict:
    """Convert GE validation results into our Critical/Warning format."""
    critical = []
    warnings = []
 
    for r in results.results:
        expectation_type = r.expectation_config.expectation_type
        success = r.success
        column = r.expectation_config.kwargs.get("column", "table-level")
 
        if not success:
            msg = f"{expectation_type} failed on column '{column}'"
            # Null/uniqueness/set membership checks on business keys are Critical
            if any(k in expectation_type for k in
                   ["not_be_null", "be_unique", "be_in_set"]):
                critical.append(msg)
            else:
                warnings.append(msg)
 
    status = "PASSED" if not critical else "FAILED"
    print(f"  {table_name}: {status} "
          f"({len(critical)} critical, {len(warnings)} warnings)")
 
    return {
        "table": table_name,
        "status": status,
        "critical": critical,
        "warnings": warnings,
    }
 
 
def run_silver_validations() -> list[dict]:
    """Run all 4 Silver validation suites. Raises on any critical failure."""
    print("Running Silver DQ validations...")
    results = [
        validate_silver_orders(),
        validate_silver_customers(),
        validate_silver_products(),
        validate_silver_inventory(),
    ]
 
    critical_failures = [r for r in results if r["status"] == "FAILED"]
    if critical_failures:
        failed_tables = [r["table"] for r in critical_failures]
        raise SilverValidationError(
            f"Critical Silver DQ failures on: {failed_tables}"
        )
 
    return results
 
 
if __name__ == "__main__":
    results = run_silver_validations()
    print("\nSilver validation summary:")
    for r in results:
        print(f"  {r['table']}: {r['status']}")
        if r["warnings"]:
            for w in r["warnings"]:
                print(f"    WARNING: {w}")
 