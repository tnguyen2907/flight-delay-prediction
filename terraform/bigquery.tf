resource "google_bigquery_dataset" "flight_delay_pred_dataset" {
  dataset_id = "flight_delay_pred_dataset"
  friendly_name = "Flight Delay Prediction Dataset"
  location   = "us-east1"
  is_case_insensitive = true
  delete_contents_on_destroy = true

  depends_on = [ google_storage_bucket.data_bucket ]
}

resource "google_bigquery_table" "external_table" {
  dataset_id = google_bigquery_dataset.flight_delay_pred_dataset.dataset_id
  table_id   = "external_gcs"
  deletion_protection = false

  external_data_configuration {
    autodetect = true
    source_format = "PARQUET"
    source_uris = [ "${google_storage_bucket.data_bucket.url}/processed/data_*.parquet" ]
  }

  depends_on = [ google_bigquery_dataset.flight_delay_pred_dataset ]
}

resource "google_bigquery_table" "dashboard_table" {
  dataset_id = google_bigquery_dataset.flight_delay_pred_dataset.dataset_id
  table_id   = "dashboard"
  deletion_protection = false

  schema = file("dashboard_schema.json")

  time_partitioning {
    field = "FlightDate"
    type  = "DAY"
  }

  clustering = [ "AirlineName", "OriginCity", "DestinationCity", "DepartureWeatherCondition" ]

  depends_on = [ google_bigquery_dataset.flight_delay_pred_dataset ]
}
