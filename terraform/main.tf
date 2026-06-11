terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ==============================================================================
# DATA LINEAGE API (OpenLineage / Dataplex)
# Enables the Dataplex Data Lineage API so OpenLineage events emitted by
# Airflow (apache-airflow-providers-openlineage) and dbt (dbt-ol wrapper)
# are ingested and visualised in GCP Console → Dataplex → Catalog → Lineage.
# ==============================================================================
resource "google_project_service" "datalineage" {
  project            = var.project_id
  service            = "datalineage.googleapis.com"
  disable_on_destroy = false
}

# ==============================================================================
# BIGQUERY DATASETS (Bronze, Silver, Gold)
# NOTE: Source database is simulated locally via Docker Compose (postgres_source).
#       No Cloud SQL instance is provisioned — local Docker Postgres is used instead.
# ==============================================================================
resource "google_bigquery_dataset" "bronze" {
  dataset_id  = var.bq_dataset_bronze
  location    = var.region
  description = "Raw/Ingested transactions data layer"

  labels = {
    layer       = "bronze"
    environment = "dev"
  }
}

resource "google_bigquery_dataset" "silver" {
  dataset_id  = var.bq_dataset_silver
  location    = var.region
  description = "Normalized/Cleaned transactions data layer"

  labels = {
    layer       = "silver"
    environment = "dev"
  }
}

resource "google_bigquery_dataset" "gold" {
  dataset_id  = var.bq_dataset_gold
  location    = var.region
  description = "Aggregated reporting views and tables layer"

  labels = {
    layer       = "gold"
    environment = "dev"
  }
}
