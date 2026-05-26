output "artifact_registry_repo_url" {
  value       = "Placeholder for artifact registry URL"
  description = "The URL of the Artifact Registry repository"
}

output "cloud_run_job_name" {
  value       = var.cloud_run_job_name
  description = "The name of the Cloud Run Job"
}

output "bq_datasets" {
  value = {
    bronze = var.bq_dataset_bronze
    silver = var.bq_dataset_silver
    gold   = var.bq_dataset_gold
  }
  description = "The BigQuery datasets created"
}
