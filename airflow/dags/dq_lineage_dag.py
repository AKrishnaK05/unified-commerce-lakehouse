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
        """Placeholder for Great Expectations Bronze validation.
        Full implementation in Issue #9."""
        print("Running Bronze DQ checks...")
        # GE checkpoint wiring will land in Issue #9
        print("Bronze DQ: PASSED (placeholder)")
 
    def run_silver_dq(**context):
        """Placeholder for Great Expectations Silver validation.
        Full implementation in Issue #9."""
        print("Running Silver DQ checks...")
        print("Silver DQ: PASSED (placeholder)")
 
    def run_gold_dq(**context):
        """Placeholder for Great Expectations Gold validation.
        Full implementation in Issue #9."""
        print("Running Gold DQ checks...")
        print("Gold DQ: PASSED (placeholder)")

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