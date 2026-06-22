terraform {
  required_providers {
    coder = {
      source  = "coder/coder"
      version = "~> 2.18"
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
    # Start code-server
    nohup code-server --bind-addr 0.0.0.0:13337 --auth none > /tmp/code-server.log 2>&1 &
    # IPv4-only TCP proxy: workaround Docker IPv6 DNS causing 502 in Coder apps
    cat > /tmp/proxy.py << 'PYEOF'
import socket, threading

def pipe(src, dst):
    try:
        while True:
            data = src.recv(4096)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        src.close()
        dst.close()

def forward(client, dst_host, dst_port):
    try:
        info = socket.getaddrinfo(dst_host, dst_port, socket.AF_INET, socket.SOCK_STREAM)
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.connect(info[0][4])
        threading.Thread(target=pipe, args=(client, srv), daemon=True).start()
        threading.Thread(target=pipe, args=(srv, client), daemon=True).start()
    except Exception:
        client.close()

def listen(local_port, dst_host, dst_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', local_port))
    s.listen(50)
    while True:
        client, _ = s.accept()
        threading.Thread(target=forward, args=(client, dst_host, dst_port), daemon=True).start()

for lport, host, dport in [
    (15000, 'mlflow',           5000),   # MLflow
    (14200, 'prefect-server',   4200),   # Prefect
    (18080, 'redpanda-console', 8080),   # Redpanda Console
    (18090, 'spark-master',     8090),   # Spark Master UI
    (18082, 'trino',            8080),   # Trino  (internal port 8080)
    (18501, 'streamlit',        8501),   # Streamlit BI
]:
    threading.Thread(target=listen, args=(lport, host, dport), daemon=True).start()

import time
while True:
    time.sleep(3600)
PYEOF
    nohup python3 /tmp/proxy.py > /tmp/proxy.log 2>&1 &
    # Install extensions in background
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

resource "coder_app" "mlflow" {
  agent_id     = coder_agent.main.id
  slug         = "mlflow"
  display_name = "MLflow"
  url          = "https://panel-photographic-symposium-institute.trycloudflare.com"
  icon         = "https://avatars.githubusercontent.com/u/39938107"
  external     = true
}

resource "coder_app" "prefect" {
  agent_id     = coder_agent.main.id
  slug         = "prefect"
  display_name = "Prefect"
  url          = "https://lexmark-clock-minds-qui.trycloudflare.com"
  icon         = "https://avatars.githubusercontent.com/u/54695496"
  external     = true
}

resource "coder_app" "redpanda" {
  agent_id     = coder_agent.main.id
  slug         = "redpanda"
  display_name = "Redpanda Console"
  url          = "https://modules-camcorders-offline-gif.trycloudflare.com"
  icon         = "https://avatars.githubusercontent.com/u/73219787"
  external     = true
}

resource "coder_app" "spark" {
  agent_id     = coder_agent.main.id
  slug         = "spark"
  display_name = "Spark UI"
  url          = "http://localhost:18090"
  icon         = "https://spark.apache.org/images/spark-logo-trademark.png"
  subdomain    = false
}

resource "coder_app" "trino" {
  agent_id     = coder_agent.main.id
  slug         = "trino"
  display_name = "Trino"
  url          = "http://localhost:18082"
  icon         = "https://avatars.githubusercontent.com/u/74037631"
  subdomain    = false
}

resource "coder_app" "streamlit" {
  agent_id     = coder_agent.main.id
  slug         = "streamlit"
  display_name = "Streamlit BI"
  url          = "http://localhost:18501"
  icon         = "https://streamlit.io/images/brand/streamlit-mark-color.png"
  subdomain    = false
}

resource "coder_app" "marquez" {
  agent_id     = coder_agent.main.id
  slug         = "marquez"
  display_name = "Marquez Lineage"
  url          = "https://educators-mixer-quad-stocks.trycloudflare.com"
  icon         = "https://avatars.githubusercontent.com/u/60117271"
  external     = true
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
