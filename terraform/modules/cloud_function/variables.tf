variable "project_id" {
  description = "GCP project id"
  type        = string
}

variable "name" {
  description = "Cloud Function name"
  type        = string
}

variable "location" {
  description = "Function region"
  type        = string
}

variable "source_dir" {
  description = "Local directory containing function source files"
  type        = string
}

variable "source_bucket" {
  description = "GCS bucket to store packaged function source"
  type        = string
}

variable "runtime" {
  description = "Function runtime"
  type        = string
  default     = "python310"
}

variable "entry_point" {
  description = "Function entry point"
  type        = string
}

variable "timeout" {
  description = "Function timeout in seconds"
  type        = number
  default     = 60
}

variable "memory" {
  description = "Function memory allocation (e.g., 256M, 512M)"
  type        = string
  default     = "256M"
}

variable "env_vars" {
  description = "Environment variables passed to the function"
  type        = map(string)
  default     = {}
}

variable "allow_unauthenticated" {
  description = "Whether to grant allUsers run.invoker"
  type        = bool
  default     = false
}

variable "ingress_settings" {
  description = "Ingress setting for function service"
  type        = string
  default     = "ALLOW_ALL"
}

variable "service_account" {
  description = "Service account email for the Cloud Function"
  type        = string
  default     = ""
}
