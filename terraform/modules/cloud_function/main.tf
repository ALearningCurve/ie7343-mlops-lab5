terraform {
  required_providers {
    archive = {
      source = "hashicorp/archive"
    }
    google = {
      source = "hashicorp/google"
    }
  }
}

data "archive_file" "source" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${path.module}/${var.name}.zip"
}

resource "google_storage_bucket_object" "source" {
  name   = "functions/${var.name}-${data.archive_file.source.output_md5}.zip"
  bucket = var.source_bucket
  source = data.archive_file.source.output_path
}

resource "google_cloudfunctions2_function" "this" {
  name     = var.name
  location = var.location

  build_config {
    runtime     = var.runtime
    entry_point = var.entry_point

    source {
      storage_source {
        bucket = var.source_bucket
        object = google_storage_bucket_object.source.name
      }
    }
  }

  service_config {
    timeout_seconds                = var.timeout
    available_memory               = var.memory
    ingress_settings               = var.ingress_settings
    all_traffic_on_latest_revision = true
    environment_variables          = var.env_vars
    service_account_email          = var.service_account != "" ? var.service_account : null
  }
}

resource "google_cloud_run_service_iam_member" "public_invoker" {
  count = var.allow_unauthenticated ? 1 : 0

  project  = var.project_id
  location = var.location
  service  = google_cloudfunctions2_function.this.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
