output "name" {
  description = "Cloud Function name"
  value       = google_cloudfunctions2_function.this.name
}

output "uri" {
  description = "Cloud Function URI"
  value       = google_cloudfunctions2_function.this.service_config[0].uri
}
