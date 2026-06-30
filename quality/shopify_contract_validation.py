# Data Contract validation for the Shopify Orders source.

from dataclasses import field
import os
import pandas as pd
import yaml

CONTRACT_PATH = os.path.join(os.path.dirname(__file__), "..", "docs", "contracts", "shopify_orders_contract.yaml")

class ContractViolation(Exception):
    """Raised when incoming data violates a 'reject'-level contract rule."""

     
def load_contract(path: str = CONTRACT_PATH) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)["contract"]

def validate_shopify_orders(df: pd.DataFrame, contract: dict | None = None) -> dict:
    if contract is None:
        contract = load_contract()

    critical: list[str] = []
    warnings: list[str] = []

    schema = {field["name"]: field for field in contract["schema"]}
    required_fields = [name for name, field in schema.items() if field.get("required")]
    known_fields = set(schema.keys())
    actual_fields = set(df.columns)

    missing_required = [f for f in required_fields if f not in actual_fields]
    if missing_required:
        critical.append(f"Missing required fields: {missing_required}")

    unknown_fields = actual_fields - known_fields
    if unknown_fields:
        warnings.append(f"Unknown fields present (schema drift): {sorted(unknown_fields)}")

    for field_name, field in schema.items():
        if field_name not in df.columns:
            continue

        if field.get("required") and df[field_name].isnull().any():
            n_null = df[field_name].isnull().sum()
            critical.append(f"{n_null} null value(s) in required field '{field_name}'")

        allowed = field.get("allowed_values")
        if allowed:
            invalid = df[field_name].dropna()
            invalid = invalid[~invalid.isin(allowed)]
            if len(invalid) > 0:
                warnings.append(
                    f"{len(invalid)} value(s) in '{field_name}' outside allowed set {allowed}"
                )

    if "quantity" in df.columns:
        invalid_qty = df[df["quantity"] <= 0]
        if len(invalid_qty) > 0:
            critical.append(f"{len(invalid_qty)} row(s) with non-positive quantity")

    if "revenue" in df.columns:
        invalid_rev = df[df["revenue"] < 0]
        if len(invalid_rev) > 0:
            critical.append(f"{len(invalid_rev)} row(s) with negative revenue")

    report = {"critical": critical, "warnings": warnings}

    if critical:
        raise ContractViolation(
            f"Shopify data contract violated with {len(critical)} critical issue(s): {critical}"
        )

    return report


if __name__ == "__main__":
    # Quick manual check against the generated sample data.
    sample_path = os.path.join(
        os.path.dirname(__file__), "..", "scripts", "data", "samples", "shopify_orders_sample.csv"
    )
    df = pd.read_csv(sample_path)
    result = validate_shopify_orders(df)
    print("Validation passed.")
    print(f"Warnings: {result['warnings']}")