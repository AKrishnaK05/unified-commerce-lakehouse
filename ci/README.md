# CI Demo Pipeline

The GitHub Actions demo workflow runs the existing Bronze → Silver → Gold pipeline on an ephemeral Ubuntu runner.

It starts a temporary MinIO container, creates the Bronze/Silver/Gold buckets, installs the same Python dependencies used locally, generates synthetic source data, runs the existing ingestion and transformation entry points, runs the Bronze/Silver data-quality checks, and exports the Gold marts as workflow artifacts.

This is intentionally separate from the full Terraform/Airflow local environment. The local stack remains the reference environment for the complete orchestration and observability architecture; CI provides a lightweight, reproducible execution path for demos and automated runs.

## Current output

Each successful run publishes:

- `revenue_mart.csv`
- `channel_performance_mart.csv`
- `customer_360_mart.csv`
- `inventory_turnover_mart.csv`
- `pipeline_run.json`

The runner is ephemeral, so these artifacts are not a persistent dashboard data source. The next integration step is to add a small export/serving layer that writes the Gold metrics and run metadata to a persistent PostgreSQL database consumed by the public dashboard API.
