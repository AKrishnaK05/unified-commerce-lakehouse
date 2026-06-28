resource "docker_image" "postgres_hive" {
    name = "postgres:16"
}

resource "docker_volume" "hive_postgres_data" {
    name = "lakehouse-hive-postgres-data"
}

resource "docker_container" "hive_postgres" {
    name = "lakehouse-hive-postgres"
    image = docker_image.postgres_hive.image_id

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    env = [
        "POSTGRES_USER=hive",
        "POSTGRES_PASSWORD=${var.hive_metastore_postgres_password}",
        "POSTGRES_DB=metastore_db",
        "POSTGRES_HOST_AUTH_METHOD=trust"
    ]

    volumes {
        volume_name = docker_volume.hive_postgres_data.name
        container_path = "/var/lib/postgresql/data"
    }
}

resource "docker_image" "hive_metastore" {
    name = "apache/hive:3.1.3"
}

resource "docker_container" "hive_metastore" {
    name = "lakehouse-hive-metastore"
    image = docker_image.hive_metastore.image_id

    networks_advanced {
        name = docker_network.lakehouse_net.name
    }

    ports {
        internal = 9083
        external = 9083
    }

    env = [
        "SERVICE_NAME=metastore",
        "DB_DRIVER=postgres",
        "IS_RESUME=true",
        "SERVICE_OPTS=-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://lakehouse-hive-postgres:5432/metastore_db -Djavax.jdo.option.ConnectionUserName=hive -Djavax.jdo.option.ConnectionPassword=${var.hive_metastore_postgres_password}"
    ]

    depends_on = [docker_container.hive_postgres]
}

