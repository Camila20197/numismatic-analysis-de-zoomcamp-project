terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.6.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}


resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = var.storage_class 

  lifecycle_rule {
    condition {
      age = 30 
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.bq_dataset_name
  location                   = var.location
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "external_raw_banknotes" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "raw_banknotes"

  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    
    source_uris = [
      "gs://${var.gcs_bucket_name}/${var.gcs_clean_path}/clean_banknotes_*.csv"
    ]
    csv_options {
      quote             = "\""
      skip_leading_rows = 1
    }
  }
}