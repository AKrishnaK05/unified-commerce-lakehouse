locals {
    airflow_common_env = [
        "AIRFLOW__CORE__EXECUTOR=LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:${var.airflow_postgres_password}@lakehouse-airflow-postgres:5432/airflow",
        "AIRFLOW__CORE__FERNET_KEY=${var.airflow_fernet_key}",
        "AIRFLOW__CORE__LOAD_EXAMPLES=False",
        "AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True",
        "MINIO_ENDPOINT=http://lakehouse-minio:9000",
        "MINIO_ACCESS_KEY=${var.minio_root_user}",
        "MINIO_SECRET_KEY=${var.minio_root_password}",
        "PYTHONPATH=/opt/airflow"
    ]
}

resource "terraform_data" "airflow_image_build" {
    triggers_replace = [
        filemd5("${path.module}/../Dockerfile"),
        filemd5("${path.module}/../requirements.txt")
    ]

    provisioner "local-exec" {
        command     = "docker build -t lakehouse-airflow-custom:latest ."
        working_dir = abspath("${path.module}/..")
    }
}

resource "docker_image" "airflow" {
    name         = "lakehouse-airflow-custom:latest"
    keep_locally = true

    depends_on = [terraform_data.airflow_image_build]
}

resource "docker_container" "airflow_init" {
    name     = "lakehouse-airflow-init"
    image    = docker_image.airflow.image_id
    must_run = false

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    env = local.airflow_common_env

    command = [
        "bash", "-c",
        "airflow db migrate && airflow users create --username admin --password ${var.airflow_admin_password} --firstname Admin --lastname User --role Admin --email admin@example.com"
    ]

    depends_on = [docker_container.airflow_postgres]
}

resource "docker_volume" "airflow_dags" {
    name = "lakehouse-airflow-dags"
}

resource "docker_container" "airflow_scheduler" {
    name  = "lakehouse-airflow-scheduler"
    image = docker_image.airflow.image_id

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    env     = local.airflow_common_env
    command = ["scheduler"]

    volumes {
        host_path      = abspath("${path.module}/../airflow/dags")
        container_path = "/opt/airflow/dags"
    }

    volumes {
        host_path      = abspath("${path.module}/..")
        container_path = "/opt/airflow"
    }

    restart    = "on-failure"
    depends_on = [docker_container.airflow_init]
}

resource "docker_container" "airflow_webserver" {
    name  = "lakehouse-airflow-webserver"
    image = docker_image.airflow.image_id

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    env     = local.airflow_common_env
    command = ["bash", "-c", "rm -f /opt/airflow/airflow-webserver.pid && exec airflow webserver"]

    ports {
        internal = 8080
        external = 8080
    }

    volumes {
        host_path      = abspath("${path.module}/../airflow/dags")
        container_path = "/opt/airflow/dags"
    }

    volumes {
        host_path      = abspath("${path.module}/..")
        container_path = "/opt/airflow"
    }

    restart    = "on-failure"
    depends_on = [docker_container.airflow_init]
}
