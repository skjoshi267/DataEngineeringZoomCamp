terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

provider "google" {
    credentials = "./nyc-taxi-data-service.json"
  project     = "nyc-taxi-data-448816"
  region      = "europe-west1"
}

resource "google_storage_bucket" "nyc-taxi-data" {
  name          = "nyc-taxi-data-20250113"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "nyc-taxi-dataset" {
  dataset_id = "nyc_taxi_data_20250113"
  project    = "nyc-taxi-data-448816"
  location   = "EU"
}
