locals {
    airflow_common_env = [
        "AIRFLOW__CORE__EXECUTOR=LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${var.airflow_postgres_password}@lakehouse-airflow-postgres:5432/airflow",
        "AIRFLOW__CORE__FERNET_KEY=${var.airflow_fernet_key}",
        "AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True"
    ]
}

resource "docker_image" "airflow" {
    name = "apache/airflow:2.9.3"
}

resource "docker_container" "airflow_init" {
    name = "lakehouse-airflow-init"
    image = docker_image.airflow.image_id
    must_run = false

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    env = local.airflow_common_env

    command = [
        "bash", "-c", "airflow db migrate && airflow users create --username admin --password ${var.airflow_admin_password} --firstname Admin --lastname User --role Admin --email admin@example.com"
    ]

    depends_on = [docker_container.airflow_postgres]
}

resource "docker_volume" "airflow_dags" {
    name = "lakehouse-airflow-dags"
}

resource "docker_container" "airflow_scheduler" {
    name = "lakehouse-airflow-scheduler"
    image = docker_image.airflow.image_id

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    env = local.airflow_common_env
    command = ["scheduler"]

    volumes {
        host_path = abspath("${path.module}/../airflow/dags")
        container_path = "/opt/airflow/dags"
    }

    depends_on = [docker_container.airflow_init]
}

resource "docker_container" "airflow_webserver" {
    name = "lakehouse-airflow-webserver"
    image = docker_image.airflow.image_id

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    env = local.airflow_common_env
    command = ["webserver"]

    ports {
        internal = 8080
        external = 8080
    }

    volumes {
        host_path = abspath("${path.module}/../airflow/dags")
        container_path = "/opt/airflow/dags"
    }

    depends_on = [docker_container.airflow_init]
}