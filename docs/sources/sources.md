# Source Systems

This project ingests data from 3 synthetic (mocked) retail source systems, simulating a realistic multi-channel retailer ("CartCo"). All sources are generated locally — see [Issue #4](https://github.com/AKrishnaK05/unified-commerce-lakehouse/issues/4) — not pulled from real external APIs.

## Source 1 — Shopify Orders API (mock)

| Field | Description |
|---|---|
| **Purpose** | E-commerce order transactions from CartCo's own storefront |
| **Owner** | E-Commerce Team |
| **Ingestion pattern** | API-based batch |
| **Refresh frequency** | Daily |
| **Special handling** | Validated against a [Data Contract](contracts/shopify_orders_contract.yaml) before being written to Bronze (project's stretch goal — Issue #13) |

**Schema:**

| Column | Type | Notes |
|---|---|---|
| `order_id` | string | Primary identifier |
| `customer_id` | string | Foreign reference, conformed into Silver `customers` |
| `product_id` | string | Foreign reference, conformed into Silver `products` |
| `order_date` | date | Business date — used for Silver partitioning |
| `quantity` | integer | |
| `revenue` | decimal | |
| `order_status` | string | e.g. placed, shipped, cancelled |

---

## Source 2 — Amazon Marketplace API (mock)

| Field | Description |
|---|---|
| **Purpose** | Marketplace sales transactions |
| **Owner** | Marketplace Team |
| **Ingestion pattern** | API-based batch |
| **Refresh frequency** | Daily |

**Schema:**

| Column | Type | Notes |
|---|---|---|
| `marketplace_order_id` | string | Primary identifier; conformed with Shopify's `order_id` into a single canonical `order_id` in Silver |
| `customer_id` | string | Foreign reference |
| `sku` | string | Conformed with Shopify's `product_id` into canonical `products` |
| `quantity` | integer | |
| `revenue` | decimal | |
| `order_timestamp` | timestamp | Business date — used for Silver partitioning |

---

## Source 3 — SFTP CSV Drop (mock inventory feed)

| Field | Description |
|---|---|
| **Purpose** | Simulated distributor/warehouse inventory feed, delivered as a daily file drop |
| **Owner** | Supply Chain Team |
| **Ingestion pattern** | File-based batch |
| **Refresh frequency** | Daily |

**Schema:**

| Column | Type | Notes |
|---|---|---|
| `inventory_id` | string | Primary identifier |
| `product_id` | string | Foreign reference, conformed into Silver `products` |
| `warehouse_id` | string | |
| `quantity_available` | integer | |
| `quantity_reserved` | integer | |
| `last_updated` | timestamp | |

---

## Data quality notes

All 3 sources will have realistic, deliberately injected messiness in their generated data (see Issue #4):
- Null values in non-critical fields
- Duplicate records (simulating retry/resend behavior)
- Late-arriving records (timestamps from prior days appearing in a later batch)
- Minor schema drift (an occasional unexpected field or type variation)

This is intentional — it gives the Bronze/Silver layers (Issues #5, #6) real deduplication, null-handling, and schema-evolution work to do, rather than processing already-clean data.

## Why these 3 sources

Per the project's Scope Boundaries (locked in the Initial Design Doc and Decision Log), the B1 spec permits 2-3 source connectors. These 3 were chosen to demonstrate 2 distinct ingestion patterns (API-based and file-based) while keeping scope realistic for a 5-week solo build. A 4th source (Kafka streaming) exists in the spec's bonus list but is explicitly out of core scope.
