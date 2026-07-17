# ADR-002: Orchestrator Choice - Apache Airflow

**Date:** 28 June 2026\
**Status:** Accepted\
**Author:** Adwaid Krishna K

---

## Context

The pipeline requires a workflow orchestrator to schedule and monitor the
Bronze → Silver → Gold → DQ/Lineage chain, handle task dependencies,
implement retry policies, and provide operational visibility into pipeline
health. Three options were evaluated.

---

## Decision

**Apache Airflow** was chosen as the pipeline orchestrator.

---

## Consequences

### Positive
- **Industry recognition** - Airflow appears in the majority of Data
  Engineer job descriptions as of 2026; demonstrating it directly on a
  portfolio project signals role-readiness to recruiters
- **DAG model** maps cleanly onto the Bronze → Silver → Gold dependency
  chain - each layer is a separate DAG, triggered by its predecessor
  on success
- **Retry policies, SLA monitoring, task-level logging** - built-in,
  no additional tooling needed
- **TriggerDagRunOperator** - allows clean inter-DAG dependencies
  without coupling DAGs into a single monolithic file

### Negative
- **Steep local setup** - Airflow requires a webserver, scheduler, and
  metadata database running simultaneously; mitigated here by
  Terraform-provisioned Docker containers
- **PySpark inside Airflow tasks** requires PySpark and Java to be
  installed in the Airflow worker environment - in this project,
  pipeline stages are run locally via Python directly; a production
  deployment would require a custom Airflow Docker image with PySpark
  pre-installed

---

## Alternatives Considered

### Dagster
**Rejected.** Dagster's asset-based model is architecturally cleaner for
a lakehouse (assets map directly to Delta tables), and it has strong
momentum in the data engineering community. Rejected because Airflow has
significantly stronger name recognition in job descriptions at this career
stage, making it the more strategically valuable choice for a portfolio
project.

### Prefect
**Rejected.** Prefect is simpler to set up locally than Airflow and has
a cleaner Python API. Rejected for the same reason as Dagster - weaker
recruiter name recognition than Airflow for Data Engineer roles.

---

## Notes

The DAG chain implemented is: `bronze_ingestion` → `silver_transformations`
→ `gold_transformations` → `dq_lineage_checkpoint`, connected via
`TriggerDagRunOperator` with `wait_for_completion=False` to allow each
stage to run independently while maintaining a clear dependency order.