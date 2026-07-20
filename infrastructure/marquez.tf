resource "docker_image" "postgres_marquez" {
  name = "postgres:14"
}

resource "docker_volume" "marquez_postgres_data" {
  name = "lakehouse-marquez_postgres_data"
}

resource "docker_container" "marquez_postgres" {
  name  = "lakehouse-marquez_postgres"
  image = docker_image.postgres_marquez.image_id

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }

  env = [
    "POSTGRES_USER=marquez",
    "POSTGRES_PASSWORD=${var.marquez_postgres_password}",
    "POSTGRES_DB=marquez"
  ]

  volumes {
    volume_name    = docker_volume.marquez_postgres_data.name
    container_path = "/var/lib/postgresql/data"
  }
}

resource "docker_image" "marquez" {
  name = "marquezproject/marquez:0.46.0"
}

resource "docker_container" "marquez" {
  name  = "lakehouse-marquez"
  image = docker_image.marquez.image_id

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }

  ports {
    internal = 5000 # HTTP API
    external = 5000
  }

  env = [
    "MARQUEZ_PORT=5000",
    "MARQUEZ_ADMIN_PORT=5001",
    "JAVA_OPTS=-Ddw.db.url=jdbc:postgresql://lakehouse-marquez_postgres:5432/marquez -Ddw.db.user=marquez -Ddw.db.password=${var.marquez_postgres_password}"
  ]

  depends_on = [docker_container.marquez_postgres]
}

resource "docker_image" "marquez_web" {
  name = "marquezproject/marquez-web:0.46.0"
}

resource "docker_container" "marquez_web" {
  name  = "lakehouse-marquez-web"
  image = docker_image.marquez_web.image_id

  networks_advanced {
    name = docker_network.lakehouse_net.name
  }

  ports {
    internal = 3000 # Web UI
    external = 3000
  }

  env = [
    "MARQUEZ_HOST=lakehouse-marquez",
    "MARQUEZ_PORT=5000"
  ]

  depends_on = [docker_container.marquez]
}