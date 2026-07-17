# ADR-004: Ingestion Tool Choice - Custom Python

**Date:** 28 June 2026\
**Status:** Accepted\
**Author:** Adwaid Krishna K

---

## Context

The Bronze ingestion layer needs to read from 3 source systems (Shopify
API mock, Amazon API mock, SFTP CSV drop) and write to Delta tables on
MinIO. The choice is between a managed ingestion framework (Airbyte,
Kafka Connect, Fivetran) and custom Python ingestion scripts.

---

## Decision

**Custom Python ingestion scripts** were chosen over managed ingestion
frameworks.

---

## Consequences

### Positive
- **Full control over ingestion metadata** - custom scripts add the exact
  `_ingestion_timestamp`, `_source_system`, `_batch_id`, `_ingestion_date`
  columns the Bronze layer design requires; managed frameworks have their
  own metadata conventions that may conflict
- **Data Contract integration** - the Shopify contract validation gate
  (Issue #13) runs as a step inside the custom ingestion function, before
  the Bronze write; this is difficult to achieve cleanly with a managed
  framework without custom connectors
- **No additional infrastructure** - Airbyte requires its own Docker
  deployment (additional containers, UI, connector configuration); custom
  Python avoids this complexity on a 5-week solo project
- **Demonstrates ingestion engineering skills** - writing ingestion code
  from scratch shows understanding of the problem; using Airbyte would
  abstract it away

### Negative
- **More code to maintain** - each new source requires a new Python
  function; a managed framework would handle this via configuration
- **No built-in connector library** - real Shopify/Amazon connectors
  (which this project uses in mocked form) would need to be written from
  scratch in production; Airbyte has production-ready connectors for both

---

## Alternatives Considered

### Airbyte
**Rejected** for this project scope. Airbyte is a strong production
choice for teams ingesting from many SaaS sources without engineering
resources to write connectors. For a 5-week solo project with 3 synthetic
sources and a specific Bronze metadata schema requirement, the additional
infrastructure and configuration overhead outweighs the benefits.
Would be the right choice if this project scaled to 10+ sources.

### Kafka Connect
**Rejected.** Kafka Connect is a streaming ingestion tool - appropriate
for real-time event pipelines, not the daily batch ingestion pattern this
project implements. Kafka itself is deferred to the project's stretch/bonus
list (see Initial Design Doc).

### Fivetran / Stitch
**Rejected.** Managed SaaS connectors - not self-hostable, require
external accounts, and abstract away the ingestion engineering entirely.
Not appropriate for a portfolio project where demonstrating ingestion
knowledge is a goal.

---

## Notes

If this project were extended to production, the ingestion layer would
be a natural candidate for replacement with Airbyte or a similar managed
framework once the number of sources exceeds ~5 and connector maintenance
becomes burdensome.