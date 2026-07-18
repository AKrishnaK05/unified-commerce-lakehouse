# Resume Bullets — Unified Commerce Lakehouse

**Unified Commerce Lakehouse** | PySpark · Delta Lake · Apache Airflow · Great Expectations · OpenLineage · Terraform · MinIO · Python 
 
- Built a production-grade medallion lakehouse (Bronze→Silver→Gold) unifying 3 retail data sources (Shopify, Amazon, SFTP) into 4 analytics-ready business marts using PySpark and Delta Lake on MinIO, orchestrated by 4 Airflow DAGs
- Implemented automated data quality validation (Great Expectations) at Bronze and Silver boundaries, schema-as-code data contracts, and column-level lineage tracking (OpenLineage + Marquez) across 11 pipeline tables
- Provisioned the entire 10-container platform (Airflow, Hive Metastore, Marquez, Grafana) declaratively using Terraform, enabling one-command reproducible deployment from a clean clone
- Maintained 16/16 unit and integration tests with green GitHub Actions CI on every commit; documented 5 Architecture Decision Records covering storage, orchestration, partitioning, ingestion, and schema evolution