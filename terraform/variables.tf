variable "project_id" {
  description = "GCP project id"
  type        = string
  default     = "cheffy-483719"
}

variable "region" {
  description = "Primary region for cloud resources"
  type        = string
  default     = "us-east1"
}

variable "bucket_location" {
  description = "GCS bucket location"
  type        = string
  default     = "US"
}

variable "bucket_prefix" {
  description = "Prefix used when bucket_name is not explicitly provided"
  type        = string
  default     = "ie7343-mlops-lab5"
}

variable "bucket_name" {
  description = "Optional explicit bucket name. Leave empty to autogenerate."
  type        = string
  default     = ""
}

variable "force_destroy_bucket" {
  description = "Allow bucket deletion even when non-empty"
  type        = bool
  default     = true
}

variable "workflow_service_account_id" {
  description = "Account id for the workflow service account"
  type        = string
  default     = "lab5-workflow-sa"
}
