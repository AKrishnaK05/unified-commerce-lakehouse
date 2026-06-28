resource "docker_image" "grafana" {
    name = "grafana/grafana:11.1.0"
}

resource "docker_volume" "grafana_data" {
    name = "lakehouse-grafana-data"
}

resource "docker_container" "grafana" {
    name = "lakehouse-grafana"
    image = docker_image.grafana.image_id

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    ports {
        internal = 3000
        external = 3001
    }

    env = [
        "GF_SECURITY_ADMIN_USER=admin",
        "GF_SECURITY_ADMIN_PASSWORD=${var.grafana_admin_password}"
    ]

    volumes {
        volume_name = docker_volume.grafana_data.name
        container_path = "/var/lib/grafana"
    }
}