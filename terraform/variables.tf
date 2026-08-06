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

variable "main_bucket_name" {
  description = "The globally unique name for your GCS Data Lake bucket"
  type        = string
  default     = "ishak-tobbi-data-lake-2026" # Must be completely unique across all of GCP
}
variable "main_dataset_name" {
  description = "The name for your BigQuery dataset (the data warehouse container)"
  type        = string
  default     = "ishak_tobbi_dataset"
}
variable "control_dataset_name" {
  description = "The name for your control dataset (used for tracking pipeline state and ingestion handshakes)"
  type        = string
  default     = "control"
}
variable "watermark_table_name" {
  description = "The name for your watermark table (used for tracking pipeline state and ingestion handshakes)"
  type        = string
  default     = "ingestion_partition_control"
}