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