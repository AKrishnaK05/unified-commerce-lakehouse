# Shared Spark session factory, configured for Delta Lake on MinIO.

import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio@ak")

def get_spark_session(app_name: str = "unified-commerce-lakehouse") -> SparkSession:
    # Resolve workspace root and config paths dynamically
    workspace_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    log4j2_path = os.path.join(workspace_root, "conf", "log4j2.properties").replace("\\", "/")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if os.path.exists(log4j2_path):
        extra_ops = f'"-Dlog4j2.configurationFile=file:///{log4j2_path}"'
        builder = builder.config("spark.driver.extraJavaOptions", extra_ops)

    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ],
    ).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def bronze_path(table_name: str) -> str:
    return f"s3a://bronze/{table_name}"

def silver_path(table_name: str) -> str:
    return f"s3a://silver/{table_name}"

def gold_path(table_name: str) -> str:
    return f"s3a://gold/{table_name}"