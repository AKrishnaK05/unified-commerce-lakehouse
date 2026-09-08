# Public dashboard data

`dashboard_snapshot.json` is generated automatically by the GitHub Actions demo pipeline after a successful Bronze → Silver → Gold run.

The dashboard can fetch this file from the `main` branch to display the latest pipeline output without exposing MinIO or Spark.
