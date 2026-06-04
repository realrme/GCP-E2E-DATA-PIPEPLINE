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
# ARTIFACT REGISTRY
# ==============================================================================
# resource "google_artifact_registry_repository" "pipeline_repo" {
#   location      = var.region
#   repository_id = var.repository_id
#   description   = "Docker repository for ingestion microservices"
#   format        = "DOCKER"
# }

# ==============================================================================
# BIGQUERY DATASETS (Bronze, Silver, Gold)
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

# ==============================================================================
# CLOUD RUN JOB (BATCH INGESTION MICROSERVICE)
# ==============================================================================
# resource "google_cloud_run_v2_job" "ingestion_job" {
#   name     = var.cloud_run_job_name
#   location = var.region
# 
#   template {
#     template {
#       containers {
#         # Real ingestion service image
#         image = "${var.region}-docker.pkg.dev/${var.project_id}/${var.repository_id}/${var.cloud_run_job_name}:latest"
# 
#         env {
#           name  = "GCP_PROJECT_ID"
#           value = var.project_id
#         }
#         env {
#           name  = "BQ_DATASET_BRONZE"
#           value = var.bq_dataset_bronze
#         }
#       }
#     }
#   }
# 
#   depends_on = [google_artifact_registry_repository.pipeline_repo]
# }
