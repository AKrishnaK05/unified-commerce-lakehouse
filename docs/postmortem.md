# Postmortem - Unified Commerce Lakehouse

*Two real problems I hit, what caused them, how I fixed them, and what they taught me.*

---

## Incident 1: Hive Metastore Crash on First `terraform apply`

### What happened

On first `terraform apply`, the `lakehouse-hive-metastore` container crashed with exit code 1 approximately 40 seconds after creation and stayed in "Exited" status. All other containers were healthy. Re-running `terraform apply` didn't help - the container kept crashing.

### Timeline

- `terraform apply` completed, all 10 containers reported created
- `docker ps -a` showed `lakehouse-hive-metastore` as `Exited (1) About a minute ago`
- `docker logs lakehouse-hive-metastore` showed a JDBC connection failure: `Connection refused: lakehouse-hive-postgres:5432`
- Waited 30 seconds, ran `docker start lakehouse-hive-metastore`
- Second attempt succeeded - Hive Metastore came up and stayed up

### Root cause

Terraform's `depends_on` tells it the *creation order* of resources - Hive Metastore container is created after its Postgres container. But "container created" ≠ "Postgres is ready to accept connections." The Postgres container starts its process immediately but takes 5-10 seconds to finish initializing and begin accepting TCP connections. Hive Metastore started its JDBC connection attempt during that gap and failed.

Docker's restart policy (`restart = "on-failure"`) kicked in after 10 seconds and retried. By that point, Postgres was fully ready, and the second attempt succeeded. The container appeared healthy from then on.

### Fix applied

Documented as a known limitation rather than implementing the full fix, because Docker's restart policy was recovering it automatically within ~10 seconds and it wasn't blocking for local development.

### The proper fix (not implemented - time constraint)

A healthcheck-based `depends_on` in Terraform that polls Postgres until it accepts a TCP connection before creating the Hive Metastore container:

```hcl
resource "docker_container" "hive_postgres" {
  name = "lakehouse-hive-postgres"
  ...
  healthcheck {
    test         = ["CMD-SHELL", "pg_isready -U hive -d metastore_db"]
    interval     = "5s"
    timeout      = "5s"
    retries      = 10
    start_period = "10s"
  }
}

resource "docker_container" "hive_metastore" {
  depends_on = [docker_container.hive_postgres]
  # Terraform would wait for the healthcheck to pass before creating this
}
```

The Docker provider's `depends_on` respects healthchecks - it won't create the dependent container until the dependency's healthcheck returns `healthy`. This would eliminate the race condition entirely.

### What it taught me

`depends_on` in Terraform declares ordering, not readiness. This distinction - "resource exists" vs "resource is ready to serve traffic" - is fundamental to distributed systems and applies far beyond Terraform. In production Kubernetes deployments, the same problem appears as init containers and readiness probes. The underlying pattern is identical: you need an active health signal, not just a process lifecycle signal.

---

## Incident 2: Airflow DAGs Visible but Tasks Failing Silently

### What happened

After building the 4 Airflow DAGs and placing them in `airflow/dags/`, all 4 appeared correctly in the Airflow UI with no parse errors. When I triggered `bronze_ingestion`, the `ingest_shopify_orders` task turned red immediately after starting - status "Failed", exit code 1. No error message was visible in the Airflow task logs UI, just "Process exited with return code 1."

### Timeline

- DAGs placed in `airflow/dags/`, visible in Airflow UI within 30 seconds ✅
- Triggered `bronze_ingestion` DAG manually
- `ingest_shopify_orders` task failed in ~3 seconds - too fast for Spark to have even started
- Airflow task logs showed only: `[2026-07-19 14:xx:xx] ERROR - Process exited with return code 1`
- `docker exec lakehouse-airflow-scheduler bash -c "python -c 'from pyspark.sql import SparkSession'"` → `ModuleNotFoundError: No module named 'pyspark'`

### Root cause

The stock `apache/airflow:2.9.3` image has no PySpark installed. When the DAG task tried to `import pyspark`, Python couldn't find the module and the process exited immediately with code 1. The error was brief enough that Airflow's log capture didn't surface the actual Python exception - it only recorded the exit code.

The reason this wasn't caught earlier: the DAGs were *parsed* correctly by Airflow (parsing only reads the DAG file structure, not the imports inside task callables - those only execute at task runtime). So "DAG visible in UI" does not mean "DAG tasks will succeed."

### Fix applied

Built a custom Airflow Docker image (`Dockerfile` in repo root) extending `apache/airflow:2.9.3` with:
- Java 11 (OpenJDK headless) - required by PySpark's JVM dependency
- All project Python dependencies from `requirements.txt` including `pyspark==3.5.1` and `delta-spark==3.2.0`
- Pre-fetched the `hadoop-aws` and `aws-java-sdk-bundle` JARs during image build

Updated `infrastructure/airflow.tf` to build this custom image via Terraform's `docker_image` `build` block instead of pulling the stock image.

Added `PYTHONPATH=/opt/airflow` and `MINIO_ENDPOINT=http://lakehouse-minio:9000` as environment variables - the MinIO endpoint fix was critical because inside Docker, `localhost:9000` refers to the container itself, not the host machine where MinIO runs.

Verified fix:
```
docker exec lakehouse-airflow-scheduler python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[1]').appName('test').getOrCreate(); print('Spark OK:', spark.version); spark.stop()"
→ Spark OK: 3.5.1
```

Triggered `bronze_ingestion` DAG - all tasks turned green.

### What it taught me

Three things:

1. **"It parses" ≠ "it runs."** Airflow separates DAG parsing (structural validation) from task execution. A DAG can show in the UI with zero parse errors and still fail at runtime because its task callables have dependencies that don't exist in the execution environment. Always test actual task execution, not just UI visibility.

2. **Container networking is not localhost.** When code runs inside Docker, `localhost` refers to that container. Services on other containers are reachable by their container name on the shared Docker network. This is a fundamental Docker networking concept that will appear in every containerized system - always think about which network namespace code is running in.

3. **Build images that match your runtime dependencies.** Using a stock image for a job that needs PySpark is like using a plain Python image for a job that needs NumPy - obvious in hindsight, but easy to miss when you're focused on the application code. The fix (a Dockerfile) is simple; the lesson is to think about the execution environment from day one, not as an afterthought.