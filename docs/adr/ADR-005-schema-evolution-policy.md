# ADR-005: Schema Evolution Policy

**Date:** 28 June 2026\
**Status:** Accepted\
**Author:** Adwaid Krishna K

---

## Context

Upstream source systems may change their schemas over time - adding new
fields, renaming existing ones, or changing data types. The lakehouse
needs a clear, documented policy for how each layer responds to schema
changes, so that evolution is handled consistently rather than
case-by-case.

This policy was motivated by a concrete example already present in this
project: the Shopify source deliberately produces a `promo_code_applied`
column that is not in the original schema definition, simulating a real
upstream schema addition.

---

## Decision

Schema evolution is handled differently per layer, matching each layer's
contract with its consumers:

| Layer | Policy | Mechanism |
|---|---|---|
| Bronze | **Accept all** - new fields are preserved as-is | Delta `mergeSchema=true` on write |
| Silver | **Controlled** - new fields tolerated but not promoted until reviewed | Data Contract + manual ADR update |
| Gold | **Strict** - schema is locked to the mart definition; upstream changes do not automatically propagate | Explicit mart rebuild required |

---

## Layer-by-layer rationale

### Bronze - Accept all
Bronze's job is raw preservation. If an upstream API adds a field, Bronze
should capture it - losing data at ingestion is worse than storing
unexpected data. Delta Lake's `mergeSchema=true` option handles this
automatically: new columns are added to the table schema without requiring
a manual migration.

The Data Contract (Issue #13) runs before the Bronze write and logs
unknown fields as warnings, providing visibility without blocking ingestion.
This is the `evolution_policy.unknown_fields: warn` setting in
`docs/contracts/shopify_orders_contract.yaml`.

### Silver - Controlled
Silver's canonical entity tables have a defined schema that downstream
Gold marts depend on. Unknown fields from Bronze are not automatically
promoted to Silver - they are logged as warnings at the contract layer,
then reviewed before a decision is made to either:
1. Update the Data Contract and Silver transformation to include the new field, or
2. Explicitly exclude it from Silver (documented in an ADR update)

This protects Gold mart stability while ensuring schema additions are a
deliberate engineering decision, not an accident.

### Gold - Strict
Gold marts are the business-facing layer with the most downstream
consumers (BI tools, reports, dashboards). Schema changes here are
breaking changes. New fields from Silver are only added to Gold marts
via an explicit code change in `transformations/gold_transformations.py`,
reviewed and merged through a PR.

---

## Consequences

### Positive
- Bronze never loses upstream data due to schema changes
- Gold schemas are stable and predictable for downstream consumers
- Schema additions are visible (logged as warnings) from day one

### Negative
- Silver and Gold require manual intervention for each schema addition -
  this is intentional friction, not a gap
- Teams must monitor Bronze warning logs to know when upstream schemas
  have changed - requires operational discipline

---

## Concrete example: `promo_code_applied`

The Shopify source's synthetic generator adds `promo_code_applied` to ~1%
of rows (Issue #4 - deliberate schema drift injection).

Under this policy:
1. **Bronze** - `promo_code_applied` is stored as-is via `mergeSchema=true`
2. **Data Contract** - flags it as a warning (`Unknown fields present: ['promo_code_applied']`)
3. **Silver** - not included in the canonical `orders` table until a
   decision is made to add it (currently: excluded, decision documented here)
4. **Gold** - unaffected; `promo_code_applied` never reaches the marts

Decision on `promo_code_applied`: **exclude from Silver for now.** The
field represents a marketing dimension (promo code tracking) that would
require a dedicated promotions dimension table in Silver and corresponding
mart changes in Gold - out of scope for the current project phase.