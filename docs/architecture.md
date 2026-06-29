# Architecture

This document describes the system architecture at two C4 levels: Container (deployment view) and Component (pipeline internals view).

## Container diagram

![Container diagram](diagrams/container_diagram.svg)

The system runs as 9 containers on a shared Docker network (`lakehouse-network`), provisioned via Terraform (see [Issue #15](https://github.com/AKrishnaK05/unified-commerce-lakehouse/issues/15)):

- **Airflow** (scheduler + webserver) — orchestrates all pipeline DAGs
- **PySpark** — runs within Airflow task execution, performs all Bronze/Silver/Gold transformations
- **MinIO** — S3-compatible object storage; holds Delta Lake tables for all 3 layers
- **Hive Metastore** (+ its own Postgres) — catalogs every Bronze/Silver/Gold table
- **Great Expectations** — runs validation checkpoints at the Bronze and Silver layer boundaries
- **Marquez** (+ its own Postgres) — receives OpenLineage events from Airflow, visualizes lineage
- **Grafana** — dashboards pipeline health
- **Postgres** (Airflow's own instance) — Airflow's internal metadata store

Each service that needs its own metadata store runs a dedicated Postgres instance, rather than sharing one — this keeps failure domains separate (a Hive Metastore schema issue can't corrupt Airflow's DAG history, for example).

## Component diagram

![Component diagram](diagrams/component_diagram.svg)

Zooms into the data pipeline itself — what runs inside Airflow's task execution:

1. **Bronze DAG** — ingests all 3 synthetic sources via `ingestion/`. The Shopify source passes through the **Data Contract** validation gate (schema-as-code check) before being written. All 3 sources land as raw Delta tables in MinIO, partitioned by ingestion date.
2. **Great Expectations: Bronze checks** — schema and basic null/type validation runs immediately after Bronze write.
3. **Silver DAG** — `transformations/silver` cleans, deduplicates, and conforms the 3 raw sources into canonical entities (customers, products, orders, inventory), partitioned by business date.
4. **Great Expectations: Silver checks** — business-rule validation (no negative revenue, valid dates, referential integrity).
5. **Gold DAG** — `transformations/gold` builds the 4 business marts: Revenue, Channel Performance, Customer 360, Inventory Turnover — partitioned by month.
6. **DQ/Lineage checkpoint DAG** — runs alongside the other 3, emitting OpenLineage events to Marquez at each layer transition.

This gives 4 DAGs total (within the spec's 3-5 DAG requirement), each independently testable before being wired together.

## Design notes

- **Storage is MinIO, not real AWS S3** — see [ADR-001](adr/) for the full reasoning (cost, account risk, spec equivalence).
- **IaC is Terraform targeting local Docker infrastructure**, not cloud resources — this is the spec's own named example stack ("MinIO + Airflow + Spark + Marquez"), not a workaround.
- **Catalog is Hive Metastore**, chosen for native fit with the existing Spark + Delta Lake stack.

These diagrams reflect the infrastructure as actually provisioned and running (Issue #15), not an aspirational plan — every container shown here exists and has been verified working via `terraform apply` from a clean state.
