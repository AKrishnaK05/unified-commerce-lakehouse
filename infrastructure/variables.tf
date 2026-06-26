variable "minio_root_user" {
    description = "MinIO root/admin username"
    type = string
    default = "minioadmin"
}

variable "minio_root_password" {
    description = "MinIO root/admin password"
    type = string
    sensitive = true
}