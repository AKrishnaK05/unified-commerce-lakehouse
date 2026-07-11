from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="gold_transformations",
    default_args=default_args,
    description="Build 4 Gold business marts from Silver canonical entities",
    schedule_interval=None,
    start_date=datetime(2026, 6, 22),
    catchup=False,
    tags=["gold", "transformations", "lakehouse"],
) as dag:

    def run_revenue(**context):
        from ingestion.spark_session import get_spark_session
        from transformations.gold_transformations import build_revenue_mart
        spark = get_spark_session("gold-revenue")
        try:
            build_revenue_mart(spark)
        finally:
            spark.stop()

    def run_channel(**context):
        from ingestion.spark_session import get_spark_session
        from transformations.gold_transformations import build_channel_performance_mart
        spark = get_spark_session("gold-channel")
        try:
            build_channel_performance_mart(spark)
        finally:
            spark.stop()

    def run_customer_360(**context):
        from ingestion.spark_session import get_spark_session
        from transformations.gold_transformations import build_customer_360_mart
        spark = get_spark_session("gold-customer360")
        try:
            build_customer_360_mart(spark)
        finally:
            spark.stop()
 
    def run_inventory_turnover(**context):
        from ingestion.spark_session import get_spark_session
        from transformations.gold_transformations import build_inventory_turnover_mart
        spark = get_spark_session("gold-inventory")
        try:
            build_inventory_turnover_mart(spark)
        finally:
            spark.stop()

    trigger_dg_lineage = TriggerDagRunOperator(
        task_id="trigger_dq_lineage_dag",
        trigger_dag_id="dq_lineage_checkpoint",
        wait_for_completion=False,
    )

    revenue_task = PythonOperator(
        task_id="build_revenue_mart",
        python_callable=run_revenue
    )

    channel_task = PythonOperator(
        task_id="build_channel_performance_mart", 
        python_callable=run_channel
    )

    customer_task = PythonOperator(
        task_id="build_customer_360_mart", 
        python_callable=run_customer_360
    )

    inventory_task = PythonOperator(
        task_id="build_inventory_turnover_mart", 
        python_callable=run_inventory_turnover
    )

    [revenue_task, channel_task, customer_task, inventory_task] >> trigger_dg_lineage