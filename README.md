# Unified Commerce Lakehouse

A production-grade Medallion Lakehouse (Bronze → Silver → Gold) that unifies order and marketplace data from three simulated retail channels into a single trustworthy source of truth.

**Internship Project - Segment 2: Data Platform & Pipeline Engineering**\
**Problem Statement:** B1 - Unified Commerce Lakehouse\
**Author:** Adwaid Krishna K\
**Target Roles:** Data Engineer, Analytics Engineer, Big Data Engineer

---

## Problem

A multi-channel retailer ("CartCo") sells through its own storefront, a third-party marketplace, and receives daily distributor data via file drops. Each system reports independently — there is no single trusted number for revenue or channel performance. This project builds the data platform that solves that.

## Architecture

Bronze → Silver → Gold medallion architecture:

- **Bronze** — raw data from 3 synthetic sources (Shopify, Amazon, SFTP inventory feed), preserved as-is, partitioned by ingestion date
- **Silver** — cleaned, deduplicated, conformed into canonical entities (customers, products, orders, inventory), partitioned by business date
- **Gold** — business-ready marts: Revenue, Channel Performance, Customer 360, Inventory Turnover, partitioned by month

Full architecture diagrams: [`docs/diagrams/`](docs/diagrams/)

## Tech Stack

| Component | Choice |
|---|---|
| Processing | PySpark |
| Table format | Delta Lake |
| Storage | MinIO (S3-compatible) |
| Orchestration | Apache Airflow |
| Data quality | Great Expectations |
| Lineage | OpenLineage + Marquez |
| Catalog | Hive Metastore |
| Monitoring | Grafana |
| Infrastructure as Code | Terraform |

Full reasoning for each choice: [`docs/adr/`](docs/adr/) *(ADRs land in Issue #10)*

## Repository Structure

```
.
├── docs/
│   ├── adr/            # Architecture Decision Records
│   ├── diagrams/        # C4 architecture diagrams
│   ├── contracts/       # Data contracts (schema-as-code)
│   └── sources/         # Source system documentation
├── ingestion/           # Bronze-layer ingestion code
├── transformations/     # Silver/Gold transformation logic
├── airflow/dags/        # Airflow DAG definitions
├── quality/             # Great Expectations suites
├── tests/               # Unit and integration tests
├── infrastructure/      # Terraform IaC
└── scripts/             # Synthetic data generators, utility scripts
```

## Getting Started

### Prerequisites
- Docker Desktop (running)
- Terraform >= 1.0
### Quickstart
 
```bash
cd infrastructure
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your own local passwords/keys (see comments in the file)
 
terraform init
terraform apply
```
 
This provisions the entire local platform:
 
| Service | URL | Purpose |
|---|---|---|
| MinIO Console | http://localhost:9001 | S3-compatible object storage |
| Airflow | http://localhost:8080 | Pipeline orchestration |
| Marquez | http://localhost:3000 | Data lineage visualization |
| Grafana | http://localhost:3001 | Pipeline monitoring |
| Hive Metastore | `localhost:9083` (Thrift, no UI) | Metadata catalog |
 
To tear everything down: `terraform destroy` (from inside `infrastructure/`).

## Status

🚧 Under active development as part of a 5-week data engineering internship (22 Jun – 26 Jul 2026). See [Project Board](https://github.com/AKrishnaK05/unified-commerce-lakehouse/projects) for live progress.

## Documentation

- [Architecture Decision Records](docs/adr/)
- [Data Source Documentation](docs/sources/)
- [Data Contracts](docs/contracts/)
