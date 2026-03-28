output "project_id" {
  value       = var.project_id
  description = "GCP project id"
}

output "region" {
  value       = var.region
  description = "Primary region"
}

output "bucket_name" {
  value       = google_storage_bucket.mlops_bucket.name
  description = "Bucket name for data and model artifacts"
}

output "workflow_service_account_email" {
  value       = google_service_account.workflow_sa.email
  description = "Workflow service account email"
}

output "process_data_uri" {
  value       = module.functions["process_data"].uri
  description = "HTTP URI for process_data function"
}

output "train_model_uri" {
  value       = module.functions["train_model"].uri
  description = "HTTP URI for train_model function"
}

output "predict_online_uri" {
  value       = module.functions["predict_online"].uri
  description = "HTTP URI for predict_online function"
}

output "batch_predict_uri" {
  value       = module.functions["batch_predict"].uri
  description = "HTTP URI for batch_predict function"
}
