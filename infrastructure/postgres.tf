resource "docker_image" "postgres" {
    name = "postgres:16"
}

resource "docker_volume" "airflow_postgres_data" {
    name = "lakehouse-airflow-postgres-data"
}

resource "docker_container" "airflow_postgres" {
    name = "lakehouse-airflow-postgres"
    image = docker_image.postgres.image_id

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    env = [
        "POSTGRES_USER=airflow",
        "POSTGRES_PASSWORD=${var.airflow_postgres_password}",
        "POSTGRES_DB=airflow"
    ]

    volumes {
        volume_name = docker_volume.airflow_postgres_data.name
        container_path = "/var/lib/postgresql/data"
    }
}