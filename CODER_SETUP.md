# Coder Setup – Remote Dev Environment cho Team

Coder biến mỗi thành viên có một **workspace riêng chạy trên server**, truy cập qua trình duyệt hoặc VS Code từ bất kỳ mạng nào — không cần cài Python, dbt, hay Spark trên máy cá nhân.

**Kiến trúc:**
```
Thành viên (bất kỳ mạng) → Cloudflare CDN → Cloudflare Tunnel → olist-cloudflared → olist-coder → Workspace container
```

---

## Yêu cầu

| Thứ | Ghi chú |
|-----|---------|
| Domain (*.yourteam.com) | Phải quản lý DNS trên Cloudflare. Miễn phí nếu đã dùng Cloudflare. |
| Cloudflare account | Free tier là đủ |
| Máy host chạy Docker | RAM tối thiểu 4 GB (8 GB recommended cho 2–3 người) |

> **Không có domain?** Xem mục [Phương án B – ngrok](#ph%C6%B0%C6%A1ng-%C3%A1n-b--ngrok-kh%C3%B4ng-c%E1%BA%A7n-domain) ở cuối file.

---

## Bước 1 – Chuẩn bị Cloudflare Tunnel

### 1a. Vào Cloudflare Zero Trust

1. Đăng nhập [dash.cloudflare.com](https://dash.cloudflare.com)
2. Chọn tên miền → **Zero Trust** (thanh bên trái)
3. Nếu là lần đầu: chọn plan **Free** → xác nhận

### 1b. Tạo Tunnel

1. **Networks → Tunnels → Create a tunnel**
2. Chọn **Cloudflared** → đặt tên tunnel (ví dụ: `olist-coder`)
3. Ở bước **Install connector** → chọn tab **Docker** → **copy token** (chuỗi dài)
4. **Lưu token vào `.env`:**

```bash
CLOUDFLARE_TUNNEL_TOKEN=eyJhIjoiXXXXX...  # dán vào đây
```

### 1c. Cấu hình Public Hostname

Ở bước tiếp theo trong wizard (hoặc sau khi tạo xong → **Edit tunnel → Public Hostname**):

| Field | Giá trị |
|-------|---------|
| Subdomain | `coder` |
| Domain | `yourteam.com` |
| Type | `HTTP` |
| URL | `coder:7080` |

> URL dùng tên container (`coder`) vì cloudflared và coder chạy cùng Docker network `lakehouse`.

Nếu muốn wildcard app proxy (để port-forward từ workspace ra trình duyệt):

| Field | Giá trị |
|-------|---------|
| Subdomain | `*.coder` |
| Domain | `yourteam.com` |
| Type | `HTTP` |
| URL | `coder:7080` |

### 1d. Cập nhật `.env`

```bash
CLOUDFLARE_TUNNEL_TOKEN=<token từ bước 1b>
CODER_ACCESS_URL=https://coder.yourteam.com
CODER_WILDCARD_ACCESS_URL=https://*.coder.yourteam.com  # bỏ qua nếu không dùng wildcard
```

---

## Bước 2 – Khởi động Coder

```bash
# Lần đầu (hoặc sau khi reset DB) – khởi tạo database coder trước
docker compose up -d postgres

# Chờ postgres ready, rồi start coder stack
docker compose --profile coder up -d

# Xem logs
docker compose --profile coder logs -f coder cloudflared
```

**Kiểm tra:**
- Local: [http://localhost:7080](http://localhost:7080)
- Team: `https://coder.yourteam.com`

---

## Bước 3 – Tạo Admin Account

Lần đầu truy cập Coder sẽ hiện form tạo tài khoản admin:

1. Điền **email** và **password** cho admin
2. Tên organization: `olist-team`

---

## Bước 4 – Tạo Workspace Template

Template định nghĩa môi trường mỗi workspace. Template mẫu dưới đây cài sẵn toàn bộ lakehouse tools.

### 4a. Tạo file template

Tạo thư mục `coder/templates/olist-workspace/`:

```bash
mkdir -p coder/templates/olist-workspace
```

**`coder/templates/olist-workspace/main.tf`:**

```hcl
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
  arch           = "amd64"
  os             = "linux"
  startup_script = <<-EOT
    set -e
    # Cài VS Code Server extension
    code-server --install-extension ms-python.python 2>/dev/null || true
    code-server --install-extension ms-toolsai.jupyter 2>/dev/null || true
    code-server --install-extension dbt-labs.dbt-power-user 2>/dev/null || true
    echo "Workspace ready!"
  EOT
}

# VS Code trong trình duyệt
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
  count = data.coder_workspace.me.start_count
  image = docker_image.workspace.image_id
  name  = "coder-${data.coder_workspace_owner.me.name}-${lower(data.coder_workspace.me.name)}"

  command = ["sh", "-c", coder_agent.main.init_script]

  env = [
    "CODER_AGENT_TOKEN=${coder_agent.main.token}",
    # Lakehouse services (chạy trên cùng Docker host)
    "ICEBERG_REST_URI=http://iceberg-rest:8181",
    "MLFLOW_TRACKING_URI=http://mlflow:5000",
    "PREFECT_API_URL=http://prefect-server:4200/api",
    "REDPANDA_BROKERS=redpanda:9092",
  ]

  # Join lakehouse network để reach các services
  networks_advanced {
    name = "olist-lakehouse_lakehouse"
  }

  volumes {
    container_path = "/workspace"
    volume_name    = docker_volume.workspace_home.name
    read_only      = false
  }

  # Mount source code (read-only để không conflict giữa các workspace)
  # volumes {
  #   container_path = "/workspace/src"
  #   host_path      = "/path/to/olist-data-engineering-project"
  #   read_only      = true
  # }
}

resource "docker_volume" "workspace_home" {
  name = "coder-${data.coder_workspace_owner.me.name}-${lower(data.coder_workspace.me.name)}-home"
}
```

**`coder/templates/olist-workspace/Dockerfile`:**

```dockerfile
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/opt/conda/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget git vim build-essential \
    ca-certificates gnupg lsb-release \
    && rm -rf /var/lib/apt/lists/*

# Miniconda (Python 3.11)
RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -b -p /opt/conda \
    && rm /tmp/miniconda.sh \
    && conda install -y python=3.11

# Data engineering tools
RUN pip install --no-cache-dir \
    dbt-duckdb dbt-spark pyiceberg[s3,pyarrow] \
    prefect pandas pyarrow duckdb \
    confluent-kafka mlflow \
    streamlit anthropic \
    boto3 s3fs fsspec

# VS Code Server (code-server)
RUN curl -fsSL https://code-server.dev/install.sh | sh

EXPOSE 13337

CMD ["sleep", "infinity"]
```

### 4b. Push template lên Coder

```bash
# Cài Coder CLI
curl -L https://coder.com/install.sh | sh

# Đăng nhập
coder login https://coder.yourteam.com

# Push template
coder templates push olist-workspace \
  --directory coder/templates/olist-workspace \
  --yes
```

---

## Bước 5 – Thêm thành viên

### Qua UI (dễ nhất)

1. Vào `https://coder.yourteam.com` → **Admin → Users → Invite User**
2. Nhập email thành viên
3. Thành viên nhận link đặt mật khẩu

### Qua CLI

```bash
coder users create \
  --username hoanganh \
  --email hoanganh@team.com \
  --password TempPass123!
```

---

## Bước 6 – Thành viên tạo Workspace

Sau khi login vào `https://coder.yourteam.com`:

1. **Create Workspace**
2. Chọn template `olist-workspace`
3. Đặt tên workspace (ví dụ: `hoanganh-dev`)
4. Click **Create Workspace** → đợi ~2 phút build image lần đầu
5. Click **VS Code** → mở VS Code trong trình duyệt, đã có đủ tools

Hoặc kết nối từ VS Code Desktop:
```bash
coder config-ssh
# Sau đó VS Code: Remote-SSH → Connect to coder.<workspace-name>
```

---

## Quản lý

### Xem trạng thái

```bash
# Tất cả workspaces
coder workspaces list

# Logs Coder server
docker compose --profile coder logs -f coder

# Logs tunnel
docker compose --profile coder logs -f cloudflared
```

### Dừng workspace khi không dùng (tiết kiệm RAM)

Coder có auto-stop:
- UI → Workspace settings → **Auto-stop after idle** (ví dụ: 1 hour)

### Reset toàn bộ

```bash
docker compose --profile coder down
docker volume rm olist-lakehouse_coder_data
# Xoá database coder trong postgres nếu cần
docker compose --profile coder up -d
```

---

## Phương án B – ngrok (không cần domain)

Nếu chưa có domain Cloudflare, dùng ngrok thay thế:

### 1. Đăng ký ngrok

Đăng ký tại [ngrok.com](https://ngrok.com) → lấy **authtoken**

### 2. Thêm vào `.env`

```bash
NGROK_AUTHTOKEN=your_ngrok_authtoken_here
CODER_ACCESS_URL=  # để trống, ngrok URL tự generate
```

### 3. Thêm service ngrok vào docker-compose.yml

```yaml
  ngrok:
    image: ngrok/ngrok:latest
    container_name: olist-ngrok
    restart: unless-stopped
    profiles: ["coder"]
    command: http coder:7080 --log=stdout
    environment:
      NGROK_AUTHTOKEN: ${NGROK_AUTHTOKEN}
    networks:
      - lakehouse
```

### 4. Lấy public URL

```bash
docker compose --profile coder logs ngrok | grep "url="
# → url=https://abcd-1234.ngrok-free.app
```

### 5. Cập nhật Coder access URL

```bash
# Set CODER_ACCESS_URL và restart coder
CODER_ACCESS_URL=https://abcd-1234.ngrok-free.app docker compose --profile coder up -d coder
```

> **Hạn chế ngrok free:** URL thay đổi mỗi lần restart. Dùng ngrok static domain (free 1 domain) hoặc ngrok paid để có URL cố định.

---

## Ports tham chiếu

| Service | Port | Ghi chú |
|---------|------|---------|
| Coder UI | 7080 | Local only, team dùng qua tunnel |
| Cloudflared | — | Không expose port, tunnel outbound |
| Workspace VS Code | 13337 | Proxy qua Coder, không expose trực tiếp |

---

## Troubleshooting

**Cloudflared không kết nối được:**
```bash
docker compose --profile coder logs cloudflared
# Kiểm tra CLOUDFLARE_TUNNEL_TOKEN có đúng không
```

**Coder không reach postgres:**
```bash
docker compose --profile coder logs coder | grep "error"
# Đảm bảo postgres đã tạo database "coder" (chạy init.sql)
```

**Workspace build lỗi:**
```bash
coder workspaces show <workspace-name>
# Xem build logs trong Coder UI → Workspace → Build Log
```

**Docker socket permission denied (Linux host):**
```bash
# Thêm user vào docker group trên host
sudo usermod -aG docker $USER
# Restart Docker daemon
sudo systemctl restart docker
```
