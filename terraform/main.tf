# 1. Declare the provider requirements
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# 2. Configure the Google Cloud Provider plugin
provider "google" {
  project = var.project_id
  region  = var.region
}

# 3. Create the Google Cloud Storage (GCS) Bucket (Our Data Lake)
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = "EU" # Multi-region storage for resiliency (or use "US")
  force_destroy = true # Allows Terraform to delete the bucket even if it contains data files

  storage_class = "STANDARD"

  uniform_bucket_level_access = true # Enforce uniform access control (no ACLs, only IAM policies)

  # Enforce security best-practices: block all public access to our raw data
  public_access_prevention = "enforced"

  # Automatically transition or delete data to control cloud costs
  lifecycle_rule {
    condition {
      age = 30 # Days
    }
    action {
      type = "Delete" # Automatically deletes raw files older than 30 days
    }
  }
}

# Create a BigQuery Dataset (The Warehouse Container)
resource "google_bigquery_dataset" "ishak_tobbi_test_dataset" {
  dataset_id                  = "ishak_tobbi_dataset"
  project                     = var.project_id
  location                    = var.region
  description                 = "Data warehouse storage for New York Taxi infrastructure data"

  # Crucial for development: allows Terraform to destroy the dataset later if needed
  delete_contents_on_destroy = true
}

# 🔗 Link the GCS Parquet files to BigQuery using an External Table
resource "google_bigquery_table" "external_yellow_taxi_table" {
  dataset_id = google_bigquery_dataset.ishak_tobbi_test_dataset.dataset_id
  table_id   = "external_yellow_tripdata"
  project    = var.project_id

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    # Points directly to the file pattern in your GCS bucket
    source_uris = [
      "gs://${var.bucket_name}/raw/ny_taxi/*.parquet"
    ]
  }
}