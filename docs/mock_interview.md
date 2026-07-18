# Mock Interview Q&A - Unified Commerce Lakehouse

10 questions a Data Engineer interviewer is likely to ask about this
project, with honest, defensible answers.

---

## Q1: Walk me through what you built.

CartCo, a multi-channel retailer, had siloed reporting - Shopify, Amazon, and its warehouse system each reported independently, so there was no single trusted revenue number. I built a medallion lakehouse to solve that.

Three layers: Bronze (raw ingestion - data lands as-is from 3 sources), Silver (cleaned and conformed - 3 source tables become 4 canonical entities: orders, customers, products, inventory), Gold (business aggregations - 4 marts: Revenue, Channel Performance, Customer 360, Inventory Turnover).

The platform is 10 Docker containers provisioned by Terraform, orchestrated by Airflow, with Great Expectations for data quality, OpenLineage and Marquez for lineage, and Hive Metastore for cataloging.

---

## Q2: Why did you choose Delta Lake over Apache Iceberg?

Both are valid open table format choices - the spec explicitly accepted either. I chose Delta Lake because it has tighter native PySpark integration (the `delta-spark` package configures directly into the SparkSession with minimal extra JARs), and there's significantly more community documentation for beginners. For a solo 5-week build, reducing the "how do I configure this" surface area was more important than Iceberg's engine-agnosticism. The decision is documented and defended in ADR-001, including that migrating to Iceberg later is possible via Delta's `CONVERT TO ICEBERG` command.

---

## Q3: How does your deduplication logic work, and why didn't you use dropDuplicates()?

I used a window function: `row_number() OVER (PARTITION BY order_id, channel ORDER BY order_date DESC)`, then kept only rank=1. `dropDuplicates()` is non-deterministic when true duplicates exist - it picks any row arbitrarily, which means different runs could produce different results. The window function approach is deterministic: it always keeps the most recent record per key. That distinction matters for reproducibility and debugging. Documented in ADR-005.

---

## Q4: What is a data contract and how did you implement one?

A data contract is a schema-as-code agreement between a data producer (the Shopify source team) and a consumer (the Bronze ingestion pipeline) that defines what valid data looks like - required fields, types, allowed values, and an owner. I implemented it as a YAML file (`docs/contracts/shopify_orders_contract.yaml`) validated by a Python function using Great Expectations before any Shopify data is written to Bronze. Missing required fields are Critical (block the write), unknown extra columns are Warning (logged but non-blocking). The concrete test case: the `promo_code_applied` column I deliberately injected as schema drift is correctly flagged as a warning, not a critical failure.

---

## Q5: Why did you choose Airflow over Dagster or Prefect?

Airflow has the strongest name recognition in Data Engineer job descriptions. Dagster's asset-based model is architecturally cleaner for a lakehouse (assets map directly to Delta tables), and I genuinely considered it - but for a portfolio project where recruiter name-recognition matters, Airflow was the more strategically sound choice. Documented in ADR-002, including honest acknowledgment of Dagster's advantages.

---

## Q6: Explain your partition strategy and why it differs across layers.

Different partitions for different access patterns. Bronze is partitioned by `_ingestion_date` - because Bronze's job is traceability and replay ("show me everything that arrived on this date"), and late-arriving records should land in today's ingestion partition regardless of their business date. Silver is partitioned by business/order date - because Silver consumers filter by "orders placed in March," not "orders ingested in March." Gold is partitioned by `order_month` (yyyy-MM string) - because Gold queries are period-based ("Q1 revenue"), and month-level granularity avoids the small-files problem of day-level partitioning. Documented in ADR-003.

---

## Q7: What data quality issues did you encounter and how did you handle them?

I deliberately injected 4 types of messiness into the synthetic data: nulls in non-critical fields, ~2% duplicate records (simulating API retries), late-arriving records (backdated 1-3 days), and schema drift (an unexpected `promo_code_applied` column on ~1% of Shopify rows).

- **Nulls:** filled with "unknown" (string fields) or 0 (numeric fields) in Silver - not dropped, to preserve records
- **Duplicates:** removed via window-function deduplication in Silver
- **Late arrivals:** handled naturally by the ingestion date vs business date partition split - they land in the right Bronze partition by ingestion date, then get their correct business date in Silver
- **Schema drift:** caught by the Data Contract validator as a warning; excluded from Silver per the schema evolution policy (ADR-005)

---

## Q8: How does lineage work in your pipeline?

The DQ/Lineage checkpoint DAG (the 4th DAG, triggered after Gold) calls `quality/lineage_emitter.py`, which POSTs structured OpenLineage events to Marquez's API at `http://localhost:5000/api/v1/lineage`. Each event describes one job (e.g., `build_silver_orders`) with its inputs (`bronze.shopify_orders`, `bronze.amazon_orders`) and outputs (`silver.orders`), plus column-level schema facets for each dataset. Marquez stores these events in its Postgres backend and visualizes them as a lineage graph in its UI. If Marquez is unreachable, the lineage step logs a warning and doesn't fail the pipeline - lineage is observability, not a pipeline dependency.

---

## Q9: What was the hardest technical problem you hit?

The Hive Metastore startup race condition. On first `terraform apply`, Hive Metastore would crash with exit code 1 because it tried to connect to its Postgres backend before Postgres had finished starting up. Terraform's `depends_on` tells it the creation order, but doesn't wait for Postgres to be *ready to accept connections* - only for the container process to exist. The container came back up automatically via Docker's restart policy, so the practical impact was a ~10-second delay on first boot. The proper fix is a healthcheck-based `depends_on` that waits for a successful Postgres connection before starting Hive. Documented as a known limitation - I understand the root cause, I have the fix, I chose not to implement it because it's not blocking for local dev.

---

## Q10: What would you do differently if you started over?

Three things:

1. **Build a custom Airflow Docker image with PySpark pre-installed from day one.** Right now, pipeline stages run from local Python - the Airflow DAGs exist and parse correctly, but they can't trigger Spark tasks inside Docker without the right dependencies in the worker image. I deferred this because it's complex, but it means the Airflow orchestration is demonstrable, not fully production-wired.

2. **Start with the infrastructure in Week 1, not in parallel with scaffolding.** I ended up adding bucket creation to MinIO after the Bronze ingestion was already written, because I didn't realize MinIO's Terraform provider handled bucket creation separately from the container. Building infra first and verifying each service before writing pipeline code would have avoided a few back-and-forth iterations.

3. **Write the ADRs inline with the work, not after.** I wrote all 5 ADRs at the end. They're good because I had the real context fresh, but writing them incrementally - one per decision, when the decision was made - would have produced better ADRs and served as a forcing function to think through trade-offs before committing to them.