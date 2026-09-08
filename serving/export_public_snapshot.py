from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from deltalake import DeltaTable

STORAGE_OPTIONS = {
    "endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
    "access_key_id": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_access_key": os.environ.get("MINIO_SECRET_KEY", "minio@ak"),
    "allow_http": "true",
    "aws_region": "us-east-1",
}


def read_gold(name: str):
    return DeltaTable(f"s3://gold/{name}", storage_options=STORAGE_OPTIONS).to_pandas()


def safe_records(df):
    clean = df.copy()
    for column in clean.columns:
        clean[column] = clean[column].where(clean[column].notna(), None)
    return clean.to_dict(orient="records")


def build_snapshot() -> dict:
    revenue = read_gold("revenue_mart")
    channels = read_gold("channel_performance_mart")
    customers = read_gold("customer_360_mart")
    inventory = read_gold("inventory_turnover_mart")

    total_revenue = float(revenue["total_revenue"].sum()) if not revenue.empty else 0.0
    total_orders = int(revenue["order_count"].sum()) if not revenue.empty else 0
    total_units = int(revenue["total_quantity"].sum()) if not revenue.empty else 0

    return {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "pipeline_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "metrics": {
            "total_revenue": total_revenue,
            "total_orders": total_orders,
            "average_order_value": total_revenue / total_orders if total_orders else 0.0,
            "customers": int(len(customers)),
            "units_sold": total_units,
        },
        "revenue_by_channel": safe_records(channels.groupby("channel", as_index=False)["total_revenue"].sum()) if not channels.empty else [],
        "revenue_trend": safe_records(revenue.groupby("order_date", as_index=False)["total_revenue"].sum()) if not revenue.empty else [],
        "channel_performance": safe_records(channels),
        "top_customers": safe_records(customers.sort_values("lifetime_revenue", ascending=False).head(10)) if not customers.empty else [],
        "inventory_health": safe_records(inventory),
    }


def main() -> None:
    output = Path(os.environ.get("SNAPSHOT_OUTPUT", "ci-output/dashboard_snapshot.json"))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(build_snapshot(), indent=2, default=str), encoding="utf-8")
    print(f"Dashboard snapshot written to {output}")


if __name__ == "__main__":
    main()
