from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dq_lineage_checkpoint",
    default_args=default_args,
    description="Run GE data quality checks and emit OpenLineage events",
    schedule_interval=None,
    start_date=datetime(2026, 6, 22),
    catchup=False,
    tags=["dq","lineage","observability", "lakehouse"],
) as dag:

    def run_bronze_dq(**context):
        import sys
        sys.path.insert(0,"/opt/airflow")
        from quality.bronze_expectations import run_bronze_validations
        results = run_bronze_validations()
        print(f"Bronze DQ complete: {len(results)} tables validated")
 
    def run_silver_dq(**context):
        import sys
        sys.path.insert(0,"/opt/airflow")
        from quality.silver_expectations import run_silver_validations
        results = run_silver_validations()
        print(f"Silver DQ complete: {len(results)} tables validated")
 
    def run_gold_dq(**context):
        import sys
        sys.path.insert(0,"/opt/airflow")
        from deltalake import DeltaTable
        import os

        STORAGE_OPTIONS = {
            "endpoint_url": os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
            "access_key_id": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            "secret_access_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
            "allow_http": "true",
            "aws_s3_allow_unsafe_rename": "true",
        }

        gold_tables = [
            "revenue_mart",
            "channel_performance_mart",
            "customer_360_mart",
            "inventory_turnover_mart"
        ]

        for table in gold_tables:
            dt = DeltaTable(f"s3://gold/{table}", storage_options=STORAGE_OPTIONS)
            df = dt.to_pandas()
            assert len(df) > 0, f"Gold table {table} is empty - pipeline may have failed"
            print(f"gold.{table}: {len(df)} rows ✓")

    def emit_lineage_events(**context):
        """Emit OpenLineage events to Marquez for this pipeline run.
        Full implementation in Issue #14."""
        import requests
        import json
        from datetime import datetime, timezone

        marquez_url = "http://lakehouse-marquez:5000/api/v1/lineage"
        run_id = str(context.get("run_id", "manual"))
 
        event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {
                "runId": run_id,
            },
            "job": {
                "namespace": "unified-commerce-lakehouse",
                "name": "full_pipeline_run",
            },
            "inputs": [
                {"namespace": "unified-commerce-lakehouse", "name": "bronze.shopify_orders"},
                {"namespace": "unified-commerce-lakehouse", "name": "bronze.amazon_orders"},
                {"namespace": "unified-commerce-lakehouse", "name": "bronze.inventory_feed"},
            ],
            "outputs": [
                {"namespace": "unified-commerce-lakehouse", "name": "gold.revenue_mart"},
                {"namespace": "unified-commerce-lakehouse", "name": "gold.channel_performance_mart"},
                {"namespace": "unified-commerce-lakehouse", "name": "gold.customer_360_mart"},
                {"namespace": "unified-commerce-lakehouse", "name": "gold.inventory_turnover_mart"},
            ],
            "producer": "https://github.com/AKrishnaK05/unified-commerce-lakehouse",
        }
 
        try:
            response = requests.post(
                marquez_url,
                data=json.dumps(event),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            if response.status_code == 200:
                print(f"Lineage event emitted successfully to Marquez")
            else:
                print(f"Marquez returned {response.status_code}: {response.text}")
        except Exception as e:
            # Non-blocking — lineage failure should not fail the pipeline
            print(f"Warning: could not emit lineage event: {e}")
 
    bronze_dq = PythonOperator(
        task_id="bronze_dq_checks",
        python_callable=run_bronze_dq,
    )
 
    silver_dq = PythonOperator(
        task_id="silver_dq_checks",
        python_callable=run_silver_dq,
    )
 
    gold_dq = PythonOperator(
        task_id="gold_dq_checks",
        python_callable=run_gold_dq,
    )
 
    emit_lineage = PythonOperator(
        task_id="emit_openlineage_events",
        python_callable=emit_lineage_events,
    )
 
    # DQ checks run in parallel, then lineage is emitted once all pass
    [bronze_dq, silver_dq, gold_dq] >> emit_lineage