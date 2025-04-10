resource "google_project_service" "dataproc_service" {
  project = var.GCP_PROJECT_ID
  service = "dataproc.googleapis.com"
}

resource "google_service_account" "dataproc_sa" {
  account_id   = "dataproc-sa"
  display_name = "Dataproc Service Account"
}

resource "google_project_iam_member" "dataproc_gcs_access_iam" {
  project = var.GCP_PROJECT_ID
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
  
  depends_on = [ google_service_account.dataproc_sa ]
}

resource "google_project_iam_member" "dataproc_bigquery_access_iam" {
  project = var.GCP_PROJECT_ID
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
  
  depends_on = [ google_service_account.dataproc_sa ]
}

resource "google_project_iam_member" "dataproc_dataproc_worker_iam" {
  project = var.GCP_PROJECT_ID
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
  
  depends_on = [ google_service_account.dataproc_sa ]
}

data "local_file" "spark_job_files" {
  for_each = { for file in fileset("${path.module}/../spark/jobs", "*.py") : file => file }
  filename = "${path.module}/../spark/jobs/${each.value}"
}

resource "google_storage_bucket_object" "spark_job_objects" {
  for_each = data.local_file.spark_job_files
  name = "spark/jobs/${each.key}"
  bucket = google_storage_bucket.data_bucket.name
  source = data.local_file.spark_job_files[each.key].filename

  depends_on = [ google_storage_bucket.data_bucket ]
}
  