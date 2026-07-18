# Test Report - Unified Commerce Lakehouse

**Date:** 28 June 2026
**Test run:** `pytest tests/ -v`
**Result: 16 passed, 0 failed, 0 errors (14.59s)**

CI badge: ![CI](https://github.com/AKrishnaK05/unified-commerce-lakehouse/actions/workflows/ci.yml/badge.svg)

---

## Test Results

```
tests/test_pipeline.py::TestSyntheticDataGeneration::test_shopify_orders_has_required_columns PASSED
tests/test_pipeline.py::TestSyntheticDataGeneration::test_shopify_orders_row_count_matches_input PASSED
tests/test_pipeline.py::TestSyntheticDataGeneration::test_shopify_orders_has_schema_drift_column PASSED
tests/test_pipeline.py::TestSyntheticDataGeneration::test_amazon_orders_has_required_columns PASSED
tests/test_pipeline.py::TestSyntheticDataGeneration::test_inventory_feed_has_required_columns PASSED
tests/test_pipeline.py::TestSyntheticDataGeneration::test_nulls_are_injected PASSED
tests/test_pipeline.py::TestSyntheticDataGeneration::test_duplicates_are_injected PASSED
tests/test_pipeline.py::TestDataContractValidation::test_valid_data_passes_contract PASSED
tests/test_pipeline.py::TestDataContractValidation::test_unknown_column_produces_warning PASSED
tests/test_pipeline.py::TestDataContractValidation::test_missing_required_field_raises_violation PASSED
tests/test_pipeline.py::TestDataContractValidation::test_negative_revenue_raises_violation PASSED
tests/test_pipeline.py::TestDataContractValidation::test_zero_quantity_raises_violation PASSED
tests/test_pipeline.py::TestBronzeIngestionMetadata::test_add_ingestion_metadata_adds_all_columns PASSED
tests/test_pipeline.py::TestSilverDeduplication::test_deduplicate_removes_duplicate_order_ids PASSED
tests/test_pipeline.py::TestGoldMartLogic::test_turnover_ratio_is_zero_when_no_sales PASSED
tests/test_pipeline.py::TestGoldMartLogic::test_revenue_aggregation_is_correct PASSED

16 passed in 14.59s
```

---

## Coverage by component

| Component | Tests | What's covered |
|---|---|---|
| Synthetic data generation | 7 | Required columns, row counts, messiness injection (nulls, duplicates, schema drift) |
| Data Contract validation | 5 | Valid data passes, unknown columns warn, missing required fields fail, negative revenue fails, zero quantity fails |
| Bronze ingestion metadata | 1 | All 4 metadata columns (_ingestion_timestamp, _source_system, _batch_id, _ingestion_date) added correctly |
| Silver deduplication | 1 | Window function keeps exactly 1 row per (order_id, channel) key |
| Gold mart logic | 2 | Turnover ratio is 0 when no sales; revenue aggregation sums correctly |

---

## What's tested

- **Unit tests (pandas-based, 14 tests)** - test business logic in isolation without needing a running Spark cluster or MinIO. Fast, run in <3 seconds.
- **Integration tests (Spark-based, 2 tests)** - spin up a local `SparkSession(master="local[1]")` to test Bronze metadata addition and Silver deduplication with real Spark semantics. Run in ~12 seconds on first execution (JVM startup).

---

## What's not tested

| Area | Why not tested here | How it's validated instead |
|---|---|---|
| MinIO connectivity | Requires running Docker containers | Verified manually via MinIO console + bronze_ingestion.py run |
| Airflow DAG parsing | Requires Airflow environment | Verified via Airflow UI (DAGs visible = no parse errors) |
| Great Expectations suites | Requires Delta tables on MinIO | Verified manually by running quality/bronze_expectations.py and quality/silver_expectations.py |
| Hive Metastore registration | Requires running Hive container | Verified manually by running quality/catalog_registration.py |
| OpenLineage/Marquez events | Requires running Marquez container | Verified manually by running quality/lineage_emitter.py and checking Marquez UI |
| Full end-to-end pipeline | Slow (~5 min with Spark) | Verified manually after each layer was built |

---

## CI configuration

Tests run automatically on every push and pull request to `main` via
GitHub Actions (`.github/workflows/ci.yml`):

- **OS:** Ubuntu latest
- **Python:** 3.11
- **Java:** 11 (Temurin) - required for PySpark
- **Trigger:** push to main, pull request to main

---

## How to run tests locally

```bash
# Activate venv first
.\venv\Scripts\Activate.ps1  # Windows
source venv/bin/activate       # Mac/Linux

# Run all tests
pytest tests/ -v

# Run a specific test class
pytest tests/test_pipeline.py::TestDataContractValidation -v

# Run with coverage (if pytest-cov installed)
pytest tests/ --cov=. --cov-report=term-missing
```