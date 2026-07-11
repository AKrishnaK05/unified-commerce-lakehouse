from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure":False,
}

with DAG(
    dag_id="silver-transformations",
    default_args=default_args,
    description="Build 4 canonical Silver entities from Bronze Delta tables",
    schedule_interval=None,
    start_date=datetime(2026, 6, 22),
    catchup=False,
    tags=["silver", "transformations", "lakehouse"],
) as dag:

    def run_orders(**context):
        from ingestion.spark_session import get_spark_session
        from transformations.silver_transformations import build_orders
        spark = get_spark_session("silver-orders")
        try:
            build_orders(spark)
        finally:
            spark.stop()

    def run_customers(**context):
        from ingestion.spark_session import get_spark_session
        from transformations.silver_transformations import build_customers
        spark = get_spark_session("silver-customers")
        try:
            build_customers(spark)
        finally:
            spark.stop()
 
    def run_products(**context):
        from ingestion.spark_session import get_spark_session
        from transformations.silver_transformations import build_products
        spark = get_spark_session("silver-products")
        try:
            build_products(spark)
        finally:
            spark.stop()
 
    def run_inventory(**context):
        from ingestion.spark_session import get_spark_session
        from transformations.silver_transformations import build_inventory
        spark = get_spark_session("silver-inventory")
        try:
            build_inventory(spark)
        finally:
            spark.stop()

    build_orders_task = PythonOperator(
        task_id="build_orders",
        python_callable=run_orders,
    )

    build_customers_task = PythonOperator(
        task_id="build_customers",
        python_callable=run_customers,
    )
 
    build_products_task = PythonOperator(
        task_id="build_products",
        python_callable=run_products,
    )
 
    build_inventory_task = PythonOperator(
        task_id="build_inventory",
        python_callable=run_inventory,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="gold_transformations",
        wait_for_completion=False,
    )

    build_orders_task >> [build_customers_task, build_products_task, build_inventory_task] >> trigger_gold