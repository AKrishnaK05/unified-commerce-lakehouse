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
        import sys
        sys.path.insert(0, "/opt/airflow")
        from quality.lineage_emitter import emit_full_pipeline_lineage
        run_id = str(context.get("run_id", "manual"))
        emit_full_pipeline_lineage(run_id=run_id)
 
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