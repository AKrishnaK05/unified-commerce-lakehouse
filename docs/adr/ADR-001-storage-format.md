# ADR-001: Storage Format — Delta Lake

**Date:** 24 June 2026\
**Status:** Accepted\
**Author:** Adwaid Krishna K

---

## Context

The Unified Commerce Lakehouse project requires an open table format for
storing data across Bronze, Silver, and Gold layers on object storage
(MinIO). The format must support ACID transactions, schema evolution, and
time travel — capabilities that raw Parquet/CSV cannot provide. Three
options were evaluated.

---

## Decision

**Delta Lake** was chosen as the open table format for all 3 medallion layers.

---

## Consequences

### Positive
- **ACID transactions** — concurrent reads and writes are safe; partial
  writes don't corrupt the table
- **Schema evolution** — new columns can be added upstream without
  breaking existing downstream consumers; handled via Delta's
  `mergeSchema` option
- **Time travel** — any previous version of a table can be queried via
  `VERSION AS OF` or `TIMESTAMP AS OF`; useful for debugging and replay
- **Native PySpark integration** — `delta-spark` package integrates
  directly with the existing SparkSession with no additional configuration
  beyond registering the catalog extension
- **OPTIMIZE + ZORDER** — Delta's compaction and data-skipping commands
  reduce query time on large tables; relevant for Gold marts once data
  volumes grow

### Negative
- **Spark dependency** — Delta Lake is tightly coupled to the Spark
  ecosystem; reading Delta tables from a non-Spark engine (e.g. DuckDB,
  Trino) requires the `delta-rs` or `delta-kernel` library, which adds
  complexity if the project ever needs a lightweight query layer
- **Less engine-agnostic than Iceberg** — Apache Iceberg is designed to
  be engine-neutral from the ground up; Delta's ecosystem, while
  expanding, is historically Spark-first

---

## Alternatives Considered

### Apache Iceberg
**Rejected.** Iceberg is the more engine-agnostic choice and equally
valid per the project spec. It was rejected for this project specifically
because Delta Lake has tighter native PySpark integration (fewer JARs,
cleaner session configuration) and significantly more documentation and
community examples for beginners. Given this is a 5-week solo build,
reducing the "how do I configure this" surface area was prioritized over
future engine portability.

### PostgreSQL
**Rejected.** PostgreSQL would be sufficient for this data volume but
doesn't demonstrate open table format skills — which is the explicit
goal of the B1 problem statement. A lakehouse architecture built on a
transactional RDBMS is not a lakehouse; it's a data warehouse, and a
limited one at that.

### Raw Parquet on MinIO
**Rejected.** Parquet without an open table format provides no ACID
guarantees, no schema evolution tracking, and no time travel. Each of
these is required by the B1 spec's technical direction.

---

## Notes for Future Reference

- If this project ever scales to a production multi-engine environment,
  migrating from Delta to Iceberg is possible via Delta's `CONVERT TO
  ICEBERG` command (as of Delta 3.x) — the decision is not irreversible
- Schema evolution policy for this project is documented separately in
  ADR-005