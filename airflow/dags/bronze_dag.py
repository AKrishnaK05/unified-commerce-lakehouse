from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import sys
import os
sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="bronze_ingestion",
    default_args=default_args,
    description="Ingest all 3 sources into Bronze Delta tables on MinIO",
    schedule_interval="0 1 * * *",
    start_date=datetime(2026,6,22),
    catchup=False,
    tags=["bronze", "ingestion", "lakehouse"],
) as dag :

    def run_shopify_ingestion(**context):
        from ingestion.bronze_ingestion import ingest_shopify_orders
        from ingestion.spark_session import get_spark_session
        spark = get_spark_session("bronze-shopify")
        try:
            ingest_shopify_orders(spark)
        finally:
            spark.stop()

    def run_amazon_ingestion(**context):
        from ingestion.bronze_ingestion import ingest_amazon_orders
        from ingestion.spark_session import get_spark_session
        spark = get_spark_session("bronze-amazon")
        try:
            ingest_amazon_orders(spark)
        finally:
            spark.stop()

    def run_inventory_ingestion(**context):
        from ingestion.bronze_ingestion import ingest_inventory_feed
        from ingestion.spark_session import get_spark_session
        spark = get_spark_session("bronze-inventory")
        try:
            ingest_inventory_feed(spark)
        finally:
            spark.stop()

    ingest_shopify = PythonOperator(
        task_id="ingest_shopify_orders",
        python_callable=run_shopify_ingestion,
    )

    ingest_amazon = PythonOperator(
        task_id="ingest_amazon_orders",
        python_callable=run_amazon_ingestion,
    )
    
    ingest_inventory = PythonOperator(
        task_id="ingest_inventory_feed",
        python_callable=run_inventory_ingestion,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="silver_transformation",
        wait_for_completion=False,
    )

    [ingest_shopify,ingest_amazon,ingest_inventory] >> trigger_silver