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

variable "airflow_postgres_password" {
    description = "Password for Airflow's metadata Postgres database"
    type = string
    sensitive = true
}

variable "airflow_fernet_key" {
    description = "Fernet key Airflow uses to encrypt sensitive data (connections, variables) in its metadata DB"
    type = string
    sensitive = true
}

variable "airflow_admin_password" {
    description = "Password for the Airflow webserver admin user"
    type = string
    sensitive = true
}