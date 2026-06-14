variable "project_id" {
  type        = string
  description = "The GCP project ID where resources will be deployed"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP Region for resource deployment"
}

variable "bq_dataset_bronze" {
  type        = string
  default     = "bronze_transactions"
  description = "BigQuery Bronze dataset — raw ingested data from local Postgres"
}

variable "bq_dataset_silver" {
  type        = string
  default     = "silver_transactions"
  description = "BigQuery Silver dataset — normalized/star-schema data"
}

variable "bq_dataset_gold" {
  type        = string
  default     = "gold_transactions"
  description = "BigQuery Gold dataset — aggregated reporting views"
}
