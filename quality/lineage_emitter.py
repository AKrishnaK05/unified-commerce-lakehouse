import json
import os
import uuid
from datetime import datetime, timezone

import requests

MARQUEZ_URL = os.environ.get(
    "MARQUEZ_URL", "http://localhost:5000/api/v1/lineage"
)
NAMESPACE = "unified-commerce-lakehouse"
PRODUCER = "https://github.com/AKrishnaK05/unified-commerce-lakehouse"

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _dataset(name: str, namespace: str = NAMESPACE, fields: list[dict] = None) -> dict:
    ds = {"namespace":namespace, "name":name}
    if fields:
        ds["facets"] = {
            "schema": {
                "_producer": PRODUCER,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
                "fields": fields,
            }
        }
    return ds

def _emit(event: dict) -> bool:
    try:
        response = requests.post(
            MARQUEZ_URL,
            data=json.dumps(event),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.status_code in (200, 201):
            return True
        else:
            print(f"Marquez returned {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"Warning: could not emit lineage event: {e}")
        return False

# Bronze lineage events

def emit_bronze_lineage(run_id: str = None) -> None:
    run_id = run_id or str(uuid.uuid4())
    print("Emitting Bronze lineage events...")

    mappings = [
        {
            "job": "ingest_shopify_orders",
            "input": _dataset("source.shopify_orders_csv"),
            "output": _dataset("bronze.shopify_orders", fields=[
                {"name": "order_id", "type": "STRING"},
                {"name": "customer_id", "type": "STRING"},
                {"name": "product_id", "type": "STRING"},
                {"name": "order_date", "type": "DATE"},
                {"name": "quantity", "type": "INTEGER"},
                {"name": "revenue", "type": "DOUBLE"},
                {"name": "order_status", "type": "STRING"},
                {"name": "_ingestion_timestamp", "type": "STRING"},
                {"name": "_source_system", "type": "STRING"},
                {"name": "_batch_id", "type": "STRING"},
                {"name": "_ingestion_date", "type": "DATE"},
            ]),
        },
        {
            "job": "ingest_amazon_orders",
            "input": _dataset("source.amazon_orders_csv"),
            "output": _dataset("bronze.amazon_orders", fields=[
                {"name": "marketplace_order_id", "type": "STRING"},
                {"name": "customer_id", "type": "STRING"},
                {"name": "sku", "type": "STRING"},
                {"name": "quantity", "type": "INTEGER"},
                {"name": "revenue", "type": "DOUBLE"},
                {"name": "order_timestamp", "type": "TIMESTAMP"},
                {"name": "_ingestion_timestamp", "type": "STRING"},
                {"name": "_source_system", "type": "STRING"},
                {"name": "_batch_id", "type": "STRING"},
                {"name": "_ingestion_date", "type": "DATE"},
            ]),
        },
        {
            "job": "ingest_inventory_feed",
            "input": _dataset("source.inventory_feed_csv"),
            "output": _dataset("bronze.inventory_feed", fields=[
                {"name": "inventory_id", "type": "STRING"},
                {"name": "product_id", "type": "STRING"},
                {"name": "warehouse_id", "type": "STRING"},
                {"name": "quantity_available", "type": "INTEGER"},
                {"name": "quantity_reserved", "type": "INTEGER"},
                {"name": "last_updated", "type": "TIMESTAMP"},
                {"name": "_ingestion_timestamp", "type": "STRING"},
                {"name": "_source_system", "type": "STRING"},
                {"name": "_batch_id", "type": "STRING"},
                {"name": "_ingestion_date", "type": "DATE"},
            ]),
        },
    ]
 
    for m in mappings:
        job_run_id = str(uuid.uuid4())
        event = {
            "eventType": "COMPLETE",
            "eventTime": _now(),
            "run": {
                "runId": job_run_id,
                "facets": {
                    "parent": {
                        "_producer": PRODUCER,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
                        "run": {"runId": run_id},
                        "job": {"namespace": NAMESPACE, "name": "dq_lineage_checkpoint"}
                    }
                }
            },
            "job": {"namespace": NAMESPACE, "name": m["job"]},
            "inputs": [m["input"]],
            "outputs": [m["output"]],
            "producer": PRODUCER,
        }
        success = _emit(event)
        status = "[OK]" if success else "[FAIL]"
        print(f"    {status} {m['job']}")
 
# Silver lineage events
 
def emit_silver_lineage(run_id: str = None) -> None:
    run_id = run_id or str(uuid.uuid4())
    print("  Emitting Silver lineage events...")
 
    mappings = [
        {
            "job": "build_silver_orders",
            "inputs": [
                _dataset("bronze.shopify_orders"),
                _dataset("bronze.amazon_orders"),
            ],
            "output": _dataset("silver.orders", fields=[
                {"name": "order_id", "type": "STRING"},
                {"name": "customer_id", "type": "STRING"},
                {"name": "product_id", "type": "STRING"},
                {"name": "order_date", "type": "DATE"},
                {"name": "quantity", "type": "INTEGER"},
                {"name": "revenue", "type": "DOUBLE"},
                {"name": "order_status", "type": "STRING"},
                {"name": "channel", "type": "STRING"},
            ]),
        },
        {
            "job": "build_silver_customers",
            "inputs": [
                _dataset("bronze.shopify_orders"),
                _dataset("bronze.amazon_orders"),
            ],
            "output": _dataset("silver.customers", fields=[
                {"name": "customer_id", "type": "STRING"},
                {"name": "source", "type": "STRING"},
            ]),
        },
        {
            "job": "build_silver_products",
            "inputs": [
                _dataset("bronze.shopify_orders"),
                _dataset("bronze.amazon_orders"),
                _dataset("bronze.inventory_feed"),
            ],
            "output": _dataset("silver.products", fields=[
                {"name": "product_id", "type": "STRING"},
                {"name": "source", "type": "STRING"},
            ]),
        },
        {
            "job": "build_silver_inventory",
            "inputs": [_dataset("bronze.inventory_feed")],
            "output": _dataset("silver.inventory", fields=[
                {"name": "inventory_id", "type": "STRING"},
                {"name": "product_id", "type": "STRING"},
                {"name": "warehouse_id", "type": "STRING"},
                {"name": "quantity_available", "type": "INTEGER"},
                {"name": "quantity_reserved", "type": "INTEGER"},
                {"name": "last_updated", "type": "TIMESTAMP"},
            ]),
        },
    ]
 
    for m in mappings:
        job_run_id = str(uuid.uuid4())
        event = {
            "eventType": "COMPLETE",
            "eventTime": _now(),
            "run": {
                "runId": job_run_id,
                "facets": {
                    "parent": {
                        "_producer": PRODUCER,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
                        "run": {"runId": run_id},
                        "job": {"namespace": NAMESPACE, "name": "dq_lineage_checkpoint"}
                    }
                }
            },
            "job": {"namespace": NAMESPACE, "name": m["job"]},
            "inputs": m["inputs"],
            "outputs": [m["output"]],
            "producer": PRODUCER,
        }
        success = _emit(event)
        status = "[OK]" if success else "[FAIL]"
        print(f"    {status} {m['job']}")
 
# Gold lineage events
 
def emit_gold_lineage(run_id: str = None) -> None:
    run_id = run_id or str(uuid.uuid4())
    print("  Emitting Gold lineage events...")
 
    mappings = [
        {
            "job": "build_revenue_mart",
            "inputs": [_dataset("silver.orders")],
            "output": _dataset("gold.revenue_mart", fields=[
                {"name": "order_date", "type": "DATE"},
                {"name": "channel", "type": "STRING"},
                {"name": "order_status", "type": "STRING"},
                {"name": "total_revenue", "type": "DOUBLE"},
                {"name": "total_quantity", "type": "INTEGER"},
                {"name": "order_count", "type": "LONG"},
                {"name": "avg_order_value", "type": "DOUBLE"},
                {"name": "order_month", "type": "STRING"},
            ]),
        },
        {
            "job": "build_channel_performance_mart",
            "inputs": [_dataset("silver.orders")],
            "output": _dataset("gold.channel_performance_mart", fields=[
                {"name": "order_month", "type": "STRING"},
                {"name": "channel", "type": "STRING"},
                {"name": "total_revenue", "type": "DOUBLE"},
                {"name": "total_units_sold", "type": "INTEGER"},
                {"name": "total_orders", "type": "LONG"},
                {"name": "avg_order_value", "type": "DOUBLE"},
                {"name": "unique_customers", "type": "LONG"},
            ]),
        },
        {
            "job": "build_customer_360_mart",
            "inputs": [
                _dataset("silver.orders"),
                _dataset("silver.customers"),
            ],
            "output": _dataset("gold.customer_360_mart", fields=[
                {"name": "customer_id", "type": "STRING"},
                {"name": "lifetime_revenue", "type": "DOUBLE"},
                {"name": "total_orders", "type": "LONG"},
                {"name": "total_units_purchased", "type": "INTEGER"},
                {"name": "avg_order_value", "type": "DOUBLE"},
                {"name": "first_order_date", "type": "DATE"},
                {"name": "last_order_date", "type": "DATE"},
                {"name": "channels_used", "type": "LONG"},
            ]),
        },
        {
            "job": "build_inventory_turnover_mart",
            "inputs": [
                _dataset("silver.inventory"),
                _dataset("silver.orders"),
            ],
            "output": _dataset("gold.inventory_turnover_mart", fields=[
                {"name": "inventory_id", "type": "STRING"},
                {"name": "product_id", "type": "STRING"},
                {"name": "warehouse_id", "type": "STRING"},
                {"name": "quantity_available", "type": "INTEGER"},
                {"name": "quantity_reserved", "type": "INTEGER"},
                {"name": "net_available", "type": "INTEGER"},
                {"name": "total_units_sold", "type": "INTEGER"},
                {"name": "turnover_ratio", "type": "DOUBLE"},
            ]),
        },
    ]
 
    for m in mappings:
        job_run_id = str(uuid.uuid4())
        event = {
            "eventType": "COMPLETE",
            "eventTime": _now(),
            "run": {
                "runId": job_run_id,
                "facets": {
                    "parent": {
                        "_producer": PRODUCER,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
                        "run": {"runId": run_id},
                        "job": {"namespace": NAMESPACE, "name": "dq_lineage_checkpoint"}
                    }
                }
            },
            "job": {"namespace": NAMESPACE, "name": m["job"]},
            "inputs": m["inputs"],
            "outputs": [m["output"]],
            "producer": PRODUCER,
        }
        success = _emit(event)
        status = "[OK]" if success else "[FAIL]"
        print(f"    {status} {m['job']}")
 
# Main — emit all lineage events for a full pipeline run
 
def emit_full_pipeline_lineage(run_id: str = None) -> None:
    run_id = run_id or str(uuid.uuid4())
    print(f"Emitting full pipeline lineage (run_id: {run_id})...")
    emit_bronze_lineage(run_id)
    emit_silver_lineage(run_id)
    emit_gold_lineage(run_id)
    print("Lineage emission complete.")
 
 
if __name__ == "__main__":
    emit_full_pipeline_lineage()