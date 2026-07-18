terraform {
    required_version = ">= 1.0"

    required_providers {
        docker = {
            source = "kreuzwerker/docker"
            version = "~> 3.0"
        }
        time = {
            source  = "hashicorp/time"
            version = "~> 0.9"
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

resource "time_sleep" "wait_for_minio" {
  depends_on = [docker_container.minio]
  create_duration = "10s"
}

resource "docker_image" "minio_mc" {
  name = "minio/mc:latest"
}

resource "docker_container" "minio_create_buckets" {
  name     = "lakehouse-minio-setup"
  image    = docker_image.minio_mc.image_id
  must_run = false

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }

  entrypoint = [
    "/bin/sh",
    "-c",
    "mc alias set myminio http://lakehouse-minio:9000 ${var.minio_root_user} ${var.minio_root_password} && mc mb --ignore-existing myminio/bronze && mc mb --ignore-existing myminio/silver && mc mb --ignore-existing myminio/gold"
  ]

  depends_on = [
    docker_container.minio,
    time_sleep.wait_for_minio
  ]
}

