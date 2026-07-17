# ADR-003: Partition Strategy

**Date:** 28 June 2026\
**Status:** Accepted\
**Author:** Adwaid Krishna K

---

## Context

Delta Lake tables on object storage (MinIO) benefit significantly from
partitioning - it allows Spark to skip entire partitions when filtering,
reducing scan cost for analytics queries. The choice of partition key
differs by layer because each layer has a different primary consumer and
query pattern.

---

## Decision

| Layer | Partition key | Type |
|---|---|---|
| Bronze | `_ingestion_date` | date |
| Silver | `order_date` (business date) | date |
| Gold | `order_month` (yyyy-MM) | string |

---

## Consequences

### Bronze - partitioned by `_ingestion_date`
**Why:** Bronze's primary use case is traceability and replay - "show me
everything that arrived on a given day." Partitioning by ingestion date
makes this query pattern fast. Using business date would be wrong here
because late-arriving records (records with an older business date that
arrive in a later batch) would scatter across old partitions, making
replay and debugging harder.

### Silver - partitioned by `order_date` (business date)
**Why:** Silver is the trusted, cleaned layer. By this point, late-arriving
records have been handled and their business dates are correct. Silver
consumers typically filter by "orders placed in March" - business date
partitioning makes this fast. Ingestion date would cause a full scan for
any business-date filter.

### Gold - partitioned by `order_month`
**Why:** Gold mart queries are almost universally period-based ("Q1
revenue," "last month's channel performance"). Month-level partitioning
gives the right granularity - day-level would create too many small
partitions as data accumulates; year-level would be too coarse for
monthly reporting.

### Negative consequences
- Two different date concepts (ingestion date vs business date) must be
  understood by anyone querying Bronze vs Silver - documented explicitly
  in `docs/catalog.md` to prevent confusion
- Gold's `order_month` is a string (`yyyy-MM`), not a date - this is a
  deliberate trade-off: string partitions sort correctly lexicographically
  and are more human-readable in the MinIO console than integer-encoded
  months

---

## Alternatives Considered

### Partition Bronze by business date
**Rejected.** Late-arriving records (a known data quality issue in this
project's synthetic data) would write into old partitions, complicating
replay and making the ingestion date → business date distinction
invisible at the storage layer.

### Partition Gold by day instead of month
**Rejected.** At the data volumes this project handles (and at most
real-world data volumes for a single retailer), day-level Gold partitions
would create hundreds of tiny files over time, degrading query performance
via the "small files problem." Month is the right balance.