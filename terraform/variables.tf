variable "project" {
  description = "Project ID de Google Cloud"
  type        = string
}

variable "region" {
  description = "Region for GCP resources"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Ubicación del Bucket y BigQuery"
  type        = string
  default     = "US"
}

variable "storage_class" {
  description = "Storage class type for your bucket"
  type        = string
  default     = "STANDARD"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset that raw data will be written to"
  type        = string
  default     = "numismatic_data"
}

variable "gcs_bucket_name" {
  description = "Mi Bucket de Storage"
  type        = string
}