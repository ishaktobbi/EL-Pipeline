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

# GCP - Configure the Google Cloud Provider plugin
provider "google" {
  project = var.project_id
  region  = var.region
}

# GCP - Create the Google Cloud Storage (GCS) Bucket (Our Data Lake)
resource "google_storage_bucket" "data_lake" {
  name          = var.main_bucket_name
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

# BQ - Create a BigQuery Dataset (The Warehouse Container)
resource "google_bigquery_dataset" "ishak_tobbi_test_dataset" {
  dataset_id                  = var.main_dataset_name
  project                     = var.project_id
  location                    = var.region
  description                 = "Data warehouse storage for New York Taxi infrastructure data"

  # Crucial for development: allows Terraform to destroy the dataset later if needed
  delete_contents_on_destroy = true
}

# 1. Dedicated Operations/Control Dataset
resource "google_bigquery_dataset" "control_dataset" {
  dataset_id                  = var.control_dataset_name
  friendly_name               = "Control & Audit Metadata"
  description                 = "Tracks pipeline state and ingestion handshakes"
  location                    = var.region # MUST match your analytical dataset location!
  delete_contents_on_destroy = true
}

# 2. Ingestion Partition Control Table
resource "google_bigquery_table" "ingestion_partition_control" {
  dataset_id          = google_bigquery_dataset.control_dataset.dataset_id
  table_id            = var.watermark_table_name
  project             = var.project_id
  friendly_name       = "Ingestion Partition Control"
  description         = "Tracks Hive partition states between Airflow ingestion and dbt transformation"
  deletion_protection = false

  # CLUSTER BY PARTITION_KEY
  clustering = ["partition_key"]

  schema = file("${path.module}/schemas/ingestion_partition_control.json")

#   schema = <<EOF
#   [
#     {
#       "name": "partition_key",
#       "type": "STRING",
#       "mode": "REQUIRED",
#       "description": "Partition key (e.g., '2026-08' or '2024-03')"
#     },
#     {
#       "name": "status",
#       "type": "STRING",
#       "mode": "REQUIRED",
#       "description": "'LOADED' (by Airflow) or 'TRANSFORMED' (by dbt)"
#     },
#     {
#       "name": "last_ingested_at",
#       "type": "TIMESTAMP",
#       "mode": "NULLABLE",
#       "description": "When Airflow landed Parquet files in GCS"
#     },
#     {
#       "name": "last_transformed_at",
#       "type": "TIMESTAMP",
#       "mode": "NULLABLE",
#       "description": "When dbt finished transforming this partition"
#     }
#   ]
#   EOF
}


















# # DBT - Enable the Artifact Registry API inside your Google Project
# resource "google_project_service" "artifact_registry_api" {
#   project            = var.project_id
#   service            = "artifactregistry.googleapis.com"
#   disable_on_destroy = false  # when terraform destroy is run, the API will not be disabled automatically, only what is created by terraform will be destroyed, not the API itself
# }

# # DBT - Create the Docker Artifact Registry Locker
# resource "google_artifact_registry_repository" "dbt_images" {
#   depends_on = [ google_project_service.artifact_registry_api ]
#   location      = var.region
#   repository_id = "dbt-images"
#   description   = "Docker repository for isolated dbt images managed by Airflow"
#   format        = "DOCKER"

#   # Optional: Cleanup policies to keep only the latest images and save costs
#   cleanup_policies {
#     id     = "keep-latest-5-versions"
#     action = "KEEP"   # we keep the last 5 imaes built. 
#     most_recent_versions {
#       keep_count = 5
#     }
#   }
# }
# # DBT - Output the Repository URL so you can use it in your build scripts
# output "artifact_registry_repo_url" {
#   value       = "${google_artifact_registry_repository.dbt_images.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.dbt_images.repository_id}"
#   description = "The base URL for pushing your dbt Docker images"
# }

# # 2. Grant your Kubernetes/Airflow Service Account permission to pull from this locker
# resource "google_artifact_registry_repository_iam_member" "viewer" {
#   location   = google_artifact_registry_repository.dbt_repo.location
#   repository = google_artifact_registry_repository.dbt_repo.name
#   role       = "roles/artifactregistry.reader"
#   member     = "serviceAccount:${var.airflow_gcp_service_account}"
# }

# #  🔗 Link the GCS Parquet files to BigQuery using an External Table
# resource "google_bigquery_table" "external_yellow_taxi_table" {
#   dataset_id = google_bigquery_dataset.ishak_tobbi_dataset.dataset_id
#   table_id   = "external_yellow_tripdata"
#   project    = var.project_id

#   deletion_protection = false # turn off the shield for this table, so we can delete it if we need to

#   external_data_configuration {
#     autodetect    = true
#     source_format = "PARQUET"

#     # Points directly to the file pattern in your GCS bucket
#     source_uris = [
#       "gs://${var.bucket_name}/raw/ny_taxi/*.parquet"
#     ]
#   }
# }