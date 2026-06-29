# Synthetic Data Generation

`scripts/generate_synthetic_data.py` generates the 3 mock source datasets described in [docs/sources/sources.md](sources/sources.md).

## Running it

```bash
pip install -r requirements.txt
python scripts/generate_synthetic_data.py
```

## Output

| Output | Location | Tracked in Git? |
|---|---|---|
| Full datasets | `scripts/output/*.csv` | No — regenerate locally as needed |
| Small samples (20 rows) | `scripts/data/samples/*_sample.csv` | Yes — for review without running the script |

## Volumes

| Dataset | Row count |
|---|---|
| Shopify Orders | 500 |
| Amazon Marketplace | 300 |
| SFTP Inventory Feed | 50 |

Adjust via the `N_SHOPIFY_ORDERS`, `N_AMAZON_ORDERS`, `N_INVENTORY_RECORDS` constants at the top of the script.

## Reproducibility

Generation is seeded (`SEED = 42`) so re-running produces datasets of the same shape and characteristics — useful for consistent local testing during development of the Bronze/Silver/Gold layers.

## Deliberately injected data-quality issues

The generator doesn't produce clean data on purpose — each source gets realistic messiness injected, so later layers (Bronze ingestion, Silver cleaning) have genuine problems to solve rather than processing already-perfect data:

| Issue | Where | Simulates |
|---|---|---|
| Nulls | Shopify `order_status`, Amazon `customer_id`, Inventory `quantity_reserved` | Incomplete upstream records |
| Duplicates | All 3 sources (~2% of rows) | Retry/resend behavior from an upstream API |
| Late-arriving records | Shopify `order_date`, Amazon `order_timestamp` | Records that arrive a few days after their business date |
| Schema drift | Shopify only — `promo_code_applied` column, populated for ~1% of rows | An upstream API quietly adding a new field |

These map directly to the cleaning work required in [Issue #6](https://github.com/AKrishnaK05/unified-commerce-lakehouse/issues/6) (dedup, null handling) and the reasoning behind [ADR-005](adr/) (schema evolution policy).
