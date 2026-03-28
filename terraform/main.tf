terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "random_id" "bucket_suffix" {
  byte_length = 2
}

locals {
  services = [
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudfunctions.googleapis.com",
    "eventarc.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "run.googleapis.com",
    "storage.googleapis.com",
    "workflows.googleapis.com",
  ]

  effective_bucket_name = var.bucket_name != "" ? var.bucket_name : format(
    "%s-%s-%s",
    var.bucket_prefix,
    var.project_id,
    random_id.bucket_suffix.hex,
  )

  function_configs = {
    process_data = {
      name        = "process-data"
      source_dir  = abspath("${path.module}/../src/data_processing")
      entry_point = "process_data"
      timeout     = 60
      memory      = "256M"
    }
    train_model = {
      name        = "train-model"
      source_dir  = abspath("${path.module}/../src/training")
      entry_point = "train_model"
      timeout     = 540
      memory      = "512M"
    }
    predict_online = {
      name        = "predict-online"
      source_dir  = abspath("${path.module}/../src/serving")
      entry_point = "predict_online"
      timeout     = 60
      memory      = "256M"
    }
    batch_predict = {
      name        = "batch-predict"
      source_dir  = abspath("${path.module}/../src/serving")
      entry_point = "batch_predict"
      timeout     = 540
      memory      = "512M"
    }
  }
}

resource "google_project_service" "required" {
  for_each           = toset(local.services)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "mlops_bucket" {
  name                        = local.effective_bucket_name
  location                    = var.bucket_location
  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy_bucket

  depends_on = [google_project_service.required]
}

resource "google_service_account" "workflow_sa" {
  account_id   = var.workflow_service_account_id
  display_name = "Workflow service account for lab5"

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "workflow_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.workflow_sa.email}"
}

resource "google_service_account" "functions_sa" {
  account_id   = "lab5-functions-sa"
  display_name = "Service account for Cloud Functions"

  depends_on = [google_project_service.required]
}

# Grant Cloud Functions service account permission to read/write bucket
data "google_project" "current" {
  project_id = var.project_id
}

resource "google_storage_bucket_iam_member" "functions_bucket_access" {
  bucket = google_storage_bucket.mlops_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.functions_sa.email}"
}

module "functions" {
  for_each = local.function_configs

  source = "./modules/cloud_function"

  project_id      = var.project_id
  name            = each.value.name
  location        = var.region
  source_dir      = each.value.source_dir
  source_bucket   = google_storage_bucket.mlops_bucket.name
  runtime         = "python310"
  entry_point     = each.value.entry_point
  timeout         = each.value.timeout
  memory          = each.value.memory
  service_account = google_service_account.functions_sa.email
  env_vars = {
    GCS_BUCKET = google_storage_bucket.mlops_bucket.name
  }

  depends_on = [google_project_service.required]
}
