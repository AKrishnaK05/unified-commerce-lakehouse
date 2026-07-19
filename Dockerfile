FROM apache/airflow:2.9.3

# Switch to root to install system dependencies
USER root

# Install Java (required for PySpark/Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark can find it
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch back to airflow user for pip installs
USER airflow

# Copy project requirements
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies including PySpark + Delta
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Pre-download the Spark JARs for hadoop-aws and aws-java-sdk
# so the first DAG run doesn't need internet access
RUN python -c "\
from delta import configure_spark_with_delta_pip; \
from pyspark.sql import SparkSession; \
builder = SparkSession.builder.master('local[1]').appName('jar-prefetch'); \
configure_spark_with_delta_pip(builder, extra_packages=[\
    'org.apache.hadoop:hadoop-aws:3.3.4',\
    'com.amazonaws:aws-java-sdk-bundle:1.12.262'\
]).getOrCreate().stop()\
" || true
# The '|| true' means if the prefetch fails (e.g. no internet in build),
# the image still builds — JARs will download on first DAG run instead.