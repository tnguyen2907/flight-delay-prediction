terraform {
  backend "gcs" {
    bucket = "flight-delay-pred-tfstate"
  }

  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}
  
provider "google-beta" {
  project = var.GCP_PROJECT_ID
  region  = var.GCP_REGION
}

provider "google" {
  project = var.GCP_PROJECT_ID
  region  = var.GCP_REGION
}

provider "aws" {
  region = var.AWS_REGION
}

data "aws_caller_identity" "current" {}

resource "google_storage_bucket" "data_bucket" {
  name          = "flight-delay-pred-data"
  location      = var.GCP_REGION
  force_destroy = true
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}
