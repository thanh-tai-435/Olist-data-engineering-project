terraform {
  required_providers {
    coder = {
      source  = "coder/coder"
      version = "~> 0.23"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "coder" {}
provider "docker" {}

data "coder_workspace" "me" {}
data "coder_workspace_owner" "me" {}

resource "coder_agent" "main" {
  arch = "amd64"
  os   = "linux"
  startup_script = <<-EOT
    # Start code-server (redirect output to avoid pipe warning)
    nohup code-server --bind-addr 0.0.0.0:13337 --auth none > /tmp/code-server.log 2>&1 &
    # Install extensions in background after code-server starts
    sleep 5
    code-server --install-extension ms-python.python > /tmp/ext-install.log 2>&1 || true
    code-server --install-extension ms-toolsai.jupyter >> /tmp/ext-install.log 2>&1 || true
    echo "Workspace ready!"
  EOT
}

resource "coder_app" "code-server" {
  agent_id     = coder_agent.main.id
  slug         = "code-server"
  display_name = "VS Code"
  url          = "http://localhost:13337/?folder=/workspace"
  icon         = "/icon/code.svg"
  subdomain    = false
}

resource "docker_image" "workspace" {
  name         = "olist-workspace:latest"
  keep_locally = true
  build {
    context    = "${path.module}"
    dockerfile = "Dockerfile"
  }
  triggers = {
    dockerfile = filesha256("${path.module}/Dockerfile")
  }
}

resource "docker_container" "workspace" {
  count   = data.coder_workspace.me.start_count
  image   = docker_image.workspace.image_id
  name    = "coder-${data.coder_workspace_owner.me.name}-${lower(data.coder_workspace.me.name)}"
  command = ["sh", "-c", coder_agent.main.init_script]

  env = [
    "CODER_AGENT_TOKEN=${coder_agent.main.token}",
    "ICEBERG_REST_URI=http://iceberg-rest:8181",
    "MLFLOW_TRACKING_URI=http://mlflow:5000",
    "PREFECT_API_URL=http://prefect-server:4200/api",
    "REDPANDA_BROKERS=redpanda:9092",
  ]

  networks_advanced {
    name = "olist-lakehouse_lakehouse"
  }

  volumes {
    container_path = "/workspace"
    volume_name    = docker_volume.workspace_home.name
    read_only      = false
  }
}

resource "docker_volume" "workspace_home" {
  name = "coder-${data.coder_workspace_owner.me.name}-${lower(data.coder_workspace.me.name)}-home"
}
