variable "project_id" {
  type        = string
  description = "The GCP project ID where resources will be deployed"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP Region for resource deployment"
}

variable "repository_id" {
  type        = string
  default     = "data-pipeline-repo"
  description = "Artifact Registry Repository name for ingestion microservice container"
}

variable "cloud_run_job_name" {
  type        = string
  default     = "batch-ingestion-job"
  description = "Cloud Run Job / Service name"
}

variable "bq_dataset_bronze" {
  type        = string
  default     = "bronze_transactions"
  description = "BigQuery Bronze dataset"
}

variable "bq_dataset_silver" {
  type        = string
  default     = "silver_transactions"
  description = "BigQuery Silver dataset"
}

variable "bq_dataset_gold" {
  type        = string
  default     = "gold_transactions"
  description = "BigQuery Gold dataset"
}
