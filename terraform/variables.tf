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

variable "db_instance_name" {
  type        = string
  default     = "transactions-db-instance"
  description = "Cloud SQL Instance Name"
}

variable "db_name" {
  type        = string
  default     = "transactions_db"
  description = "Cloud SQL Database Name"
}

variable "db_user" {
  type        = string
  default     = "postgres"
  description = "Cloud SQL Database User"
}

variable "db_password" {
  type        = string
  default     = "postgres_password"
  sensitive   = true
  description = "Cloud SQL Database Password"
}
