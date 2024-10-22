terraform {
  backend "gcs" {
    bucket = "flight-delay-pred-tfstate"
  }
}

provider "google-beta" {
  project = var.GCP_PROJECT_ID
  region  = var.REGION
}

provider "google" {
  project = var.GCP_PROJECT_ID
  region  = var.REGION
}
