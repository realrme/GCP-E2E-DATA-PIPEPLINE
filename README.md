# NTT End-to-End Data Pipeline (DE - AE - DA)

This repository contains the structured components for an end-to-end data pipeline running on Google Cloud Platform (GCP) and orchestrated locally or via Cloud Composer using Apache Airflow.

## Architecture Overview

```mermaid
graph TD
    subgraph Local / Docker Compose
        CSV[transactions.csv] -->|Loads into| DB[(PostgreSQL Source)]
        Airflow[Apache Airflow Orchestration]
    end

    subgraph GCP Environment
        IngestionCR[Batch Ingestion Cloud Run]
        ArtifactRegistry[Artifact Registry Container App]
        
        subgraph BigQuery Data Warehouse
            Bronze[Bronze Layer Native Table]
            Silver[Silver Layer Native Table]
            Gold[Gold Layer Native Table & View]
        end
    end

    subgraph BI Layer
        Looker[Looker Studio Visualization]
    end

    DB -->|Extracted by| IngestionCR
    IngestionCR -->|Loads raw data| Bronze
    Bronze -->|dbt transformation normalize| Silver
    Silver -->|dbt transformation aggregate| Gold
    Gold -->|Exposed to| Looker

    Airflow -->|1. Triggers| IngestionCR
    Airflow -->|2. Triggers| Bronze
    Airflow -->|3. Triggers| Silver
    
    Terraform[Terraform IaC] -.->|Provisions| IngestionCR
    Terraform -.->|Provisions| ArtifactRegistry
    Terraform -.->|Provisions| BigQuery
```

## Repository Structure

- `local_db/`: Simulated source transactional database setup (PostgreSQL and CSV data).
- `terraform/`: Infrastructure as Code (IaC) to provision BigQuery datasets, Cloud Run, Artifact Registry, and IAM configuration.
- `ingestion/`: Python-based ingestion microservice containerized and run on Cloud Run.
- `dbt_pipeline/`: DBT models for BigQuery (Bronze, Silver, Gold layers).
- `airflow/`: Orchestration DAGs to schedule and coordinate the pipeline.

## Getting Started

*(Detailed setup, local testing, and GCP deployment commands will be documented here as implementation progresses)*
