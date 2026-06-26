terraform {
    required_version = ">= 1.0"

    required_providers {
        docker = {
            source = "kreuzwerker/docker"
            version = "~> 3.0"
        }
        minio = {
            source = "aminueza/minio"
            version = "~> 2.0"
        }
    }
}

provider "docker" {}

resource "docker_network" "lakehouse_net" {
    name = "lakehouse-network"
}

# ---- MinIO ----

resource "docker_image" "minio" {
    name = "minio/minio:latest"
}

resource "docker_container" "minio" {
    name = "lakehouse-minio"
    image = docker_image.minio.image_id

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    ports{
        internal = 9000 #S3 API port
        external = 9000
    }
    ports{
        internal = 9001 #Web Console port
        external = 9001
    }

    env = [
        "MINIO_ROOT_USER=${var.minio_root_user}",
        "MINIO_ROOT_PASSWORD=${var.minio_root_password}"
    ]

    command = ["server", "/data", "--console-address", ":9001"]

    volumes {
        volume_name = docker_volume.minio_data.name
        container_path = "/data"
    }
}

resource "docker_volume" "minio_data" {
    name = "lakehouse-minio-data"
}