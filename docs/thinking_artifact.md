# How I Built a Production-Grade Data Lakehouse in 5 Weeks — Architecture Decisions, Trade-offs, and What I'd Do Differently

*A deep-dive into the engineering decisions behind the Unified Commerce Lakehouse — a medallion architecture built with PySpark, Delta Lake, Airflow, and Terraform.*

---

## The Problem

CartCo, a multi-channel retailer, sells through its own storefront, Amazon's marketplace, and manages warehouse inventory through daily file drops. On paper, simple. In practice: three systems, three revenue numbers, and zero trust in any of them.

The classic "every team has a different total" problem. The solution — a unified data lakehouse — is also classic. What isn't always clear is *why* certain architectural choices get made, and *what actually goes wrong* when you build one from scratch as a solo engineer in five weeks.

This post covers the non-obvious decisions: the ones where I had real alternatives, made real trade-offs, and can now explain exactly why I chose what I chose.

---

## The Architecture in One Sentence

Three synthetic retail sources flow through a Bronze → Silver → Gold medallion architecture, stored as Delta Lake tables on MinIO, orchestrated by Apache Airflow, validated by Great Expectations, with lineage tracked by OpenLineage to Marquez, cataloged in Hive Metastore, and the entire platform provisioned by Terraform.

That sentence contains eight technology choices. Every single one was a real decision with real alternatives. Here are the five that mattered most.

---

## Decision 1: Delta Lake Over Iceberg — And Why It Was Close

The B1 spec said "Iceberg or Delta — pick one and defend it." This is a genuinely contested space in 2026, not a clear winner.

**What I evaluated:**
Apache Iceberg is engine-agnostic from the ground up — you can query an Iceberg table from Spark, Flink, Trino, DuckDB, or Athena with equal first-class support. Delta Lake is Spark-first, with other engines (DuckDB, Trino) supported but feeling like an afterthought until relatively recently.

**Why I chose Delta:**
Two reasons. First, this project uses PySpark for all processing — there's no polyglot compute layer here, so engine-agnosticism doesn't give me anything. Second, the `delta-spark` package configuration is significantly simpler than the equivalent Iceberg setup with Spark: fewer JARs, cleaner session configuration, more tutorials written by people who actually got it working.

For a solo 5-week build, "fewer ways for the setup to go wrong" is a legitimate technical criterion, not a cop-out. I documented this honestly in ADR-001.

**What I'd change:** If this project were going to production and the team used multiple compute engines (a common real-world scenario), I'd choose Iceberg. The engine-agnosticism advantage is real and becomes important at scale.

---

## Decision 2: The Partition Strategy Isn't Obvious

Most lakehouse tutorials partition everything by date. That's incomplete advice — which date? For what purpose?

I use three different partition schemes across three layers, and each one is correct for that layer's access pattern:

**Bronze: partitioned by `_ingestion_date`** — the date the data *arrived*, not the date on the record. Bronze's job is traceability. If I need to replay last Tuesday's ingestion, I should be able to read exactly last Tuesday's partition. Late-arriving records (records with an older business date that show up in a later batch) land in the *current ingestion* partition — which is correct, because Bronze is about "what arrived when," not "what happened when."

**Silver: partitioned by `order_date`** — the actual business date of the transaction. By Silver, late-arriving records have been handled and their business dates are cleaned. Silver consumers ask "show me March orders" — they should get March orders, not March-arriving orders. Using ingestion date at Silver would force a full scan for any business-date filter.

**Gold: partitioned by `order_month` (yyyy-MM string)** — period-based, not day-based. Gold mart queries are almost universally "last quarter" or "this month" style. Day-level partitioning would create hundreds of tiny files over time — the "small files problem" that degrades query performance. Month-level is the right granularity for analytics.

This is documented in ADR-003, but the insight worth remembering is: **partition key should match the most common filter on that layer's data, not just "date."**

---

## Decision 3: Window Functions for Deduplication — Not dropDuplicates()

The synthetic data generator deliberately injects ~2% duplicate rows, simulating API retry/resend behavior from upstream systems. Every production pipeline sees this. The question is how you handle it.

PySpark's `dropDuplicates()` is the obvious answer. It's also non-deterministic when true duplicates exist — if two rows are identical, Spark picks one arbitrarily, and different runs can produce different results. For a pipeline that runs on a schedule, non-determinism in deduplication is a debugging nightmare.

My Silver deduplication uses a window function:

```python
window = Window.partitionBy("order_id", "channel").orderBy(F.col("order_date").desc())
df.withColumn("_rank", F.row_number().over(window)) \
  .filter(F.col("_rank") == 1) \
  .drop("_rank")
```

This is deterministic: it always keeps the most recent record per (order_id, channel) key. The same input always produces the same output. More importantly, it's *explicit* — anyone reading the code understands exactly which duplicate is being kept and why.

The test for this:

```python
def test_deduplicate_removes_duplicate_order_ids(self):
    data = [
        {"order_id": "ORD-001", "channel": "shopify", "order_date": "2026-06-01"},
        {"order_id": "ORD-001", "channel": "shopify", "order_date": "2026-06-02"},  # duplicate
        {"order_id": "ORD-002", "channel": "shopify", "order_date": "2026-06-01"},
    ]
    result = deduplicate(df, ["order_id", "channel"], "order_date")
    assert result.count() == 2
```

Documented in ADR-005 (schema evolution policy), since it's part of the overall data handling philosophy.

---

## Decision 4: Terraform for Local Infrastructure — Not Just docker-compose

The spec required Infrastructure as Code (Terraform or Pulumi). Most tutorials point at Terraform managing cloud resources (AWS, GCP, Azure). This project uses Terraform to manage *local Docker infrastructure* instead.

Why? Because the spec's own named example of what Terraform should provision was "MinIO + Airflow + Spark + Marquez" — a local-first stack. And using real AWS means real billing risk for a student project with a public GitHub repo.

The Terraform `docker` provider manages containers, networks, and volumes declaratively. The `minio` provider creates buckets and access policies inside a running MinIO instance. The result: `terraform apply` boots 10 containers on a shared Docker network, creates 3 MinIO buckets with correct permissions, and wires everything together. `terraform destroy` removes everything cleanly.

This is genuinely more rigorous than a `docker-compose.yml` because Terraform maintains state — it knows what it created, can update resources in-place when configuration changes, and plans changes before applying them. Running `terraform plan` before `terraform apply` is the IaC equivalent of a dry run.

**The one real problem I hit:** Hive Metastore sometimes crashed on first startup because it tried to connect to its Postgres metadata backend before Postgres finished initializing. Terraform's `depends_on` tells it the *creation order*, but doesn't wait for a service to be *ready to accept connections* — only for the container process to exist. The proper fix is a healthcheck-based `depends_on` that polls Postgres until it's accepting connections. I documented this as a known limitation rather than implementing the fix, because Docker's restart policy recovered it automatically within ~10 seconds and it wasn't blocking for local development. But I understand the root cause and the fix — that distinction matters in an interview.

---

## Decision 5: Data Contracts — The Simplest Real Governance You Can Add

A data contract is an agreement between a data producer and a consumer about what the data will look like. In practice: a YAML file that says "these fields are required, these are the allowed values, this is the owner," and a validation function that checks incoming data against it.

I implemented this for the Shopify source in `docs/contracts/shopify_orders_contract.yaml`, validated by a Python function using Great Expectations before any data reaches Bronze.

The interesting design decision was the severity tiering:

- **Missing required fields** → Critical (block the write)
- **Unknown extra columns** → Warning (log, don't block)

The unknown-column-as-warning approach is deliberate. If Shopify's API quietly adds a `promo_code_applied` field to 1% of records (which my synthetic generator does, to test this), blocking the entire pipeline over an unexpected column would be worse than logging it and moving on. The contract's `evolution_policy` section defines this behavior:

```yaml
evolution_policy:
  unknown_fields: "warn"
  missing_required_fields: "reject"
```

The test that confirms this:

```python
def test_unknown_column_produces_warning(self):
    df = valid_shopify_df()
    df["unexpected_column"] = "surprise"
    result = validate_shopify_orders(df)
    assert any("schema drift" in w.lower() for w in result["warnings"])
```

This approach — validate before Bronze write, warn on schema drift, reject on missing required fields — is the simplest governance pattern that actually works in production. It costs almost nothing to implement and gives you immediate visibility into upstream schema changes before they silently corrupt your Silver layer.

---

## What I'd Do Differently

**1. Build a custom Airflow Docker image from day one.**
The Airflow DAGs work — they parse correctly, appear in the UI, have the right task dependencies. But triggering Spark tasks from within Docker-based Airflow requires PySpark and Java to be installed in the Airflow worker image. I didn't build a custom image, so pipeline stages run from local Python rather than from inside Airflow. This is the single biggest gap between "demonstrable" and "production-wired."

**2. Write ADRs as decisions are made, not after.**
I wrote all 5 ADRs at the end of the project. They're accurate because the context was fresh, but writing them incrementally — one at the moment of each decision — would have forced sharper thinking at decision time, not just better documentation after the fact.

**3. Hive Metastore healthcheck in Terraform.**
The race condition between Hive Metastore and its Postgres backend is a known, solvable problem. I documented it as a known limitation rather than fixing it. The right answer is a Terraform `depends_on` with a connection healthcheck, not relying on Docker's restart policy.

---

## The Real Lesson

The hardest part of building a lakehouse isn't the Spark code or the Delta configuration. It's the invisible decisions: which date to partition on, which duplicates to keep, what makes a contract violation blocking vs non-blocking, how to make infrastructure reproducible without cloud billing risk.

None of these decisions have universally correct answers. They all depend on the layer's purpose, the access patterns, and the engineering context. The ADRs in `docs/adr/` document every one of mine. Read them — not because my choices are the only valid ones, but because the reasoning process is the part worth stealing.

---

*GitHub: [github.com/AKrishnaK05/unified-commerce-lakehouse](https://github.com/AKrishnaK05/unified-commerce-lakehouse)*
*Demo: [Loom walkthrough](https://www.loom.com/share/eab7b362b5b14c0aa1417ef8455f9f43)*