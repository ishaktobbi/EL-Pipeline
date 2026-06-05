variable "project_id" {
  description = "The unique ID of your Google Cloud Project"
  type        = string
  default     = "elpipeline" 
}

variable "region" {
  description = "The geographical region to deploy resources into"
  type        = string
  default     = "europe-west3" # Frankfurt region (or use us-central1 if preferred)
}

variable "bucket_name" {
  description = "The globally unique name for your GCS Data Lake bucket"
  type        = string
  default     = "ishak-tobbi-data-lake-2026" # Must be completely unique across all of GCP
}