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
        time = {
            source  = "hashicorp/time"
            version = "~> 0.9"
        }
    }
}

provider "docker" {}

provider "minio" {
  minio_server   = "localhost:9000"
  minio_user     = var.minio_root_user
  minio_password = var.minio_root_password
  minio_ssl      = false
}

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

resource "minio_s3_bucket" "bronze" {
  bucket = "bronze"

  depends_on = [time_sleep.wait_for_minio]
}

resource "minio_s3_bucket" "silver" {
  bucket = "silver"

  depends_on = [time_sleep.wait_for_minio]
}

resource "minio_s3_bucket" "gold" {
  bucket = "gold"

  depends_on = [time_sleep.wait_for_minio]
}
