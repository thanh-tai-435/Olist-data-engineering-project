# Prefect – Hướng dẫn Setup Chi tiết

## Kiến trúc Prefect trong project này

```
┌─────────────────────────────────────────────────────────────┐
│                    Prefect Server (port 4200)                │
│            Web UI · REST API · Flow run scheduler            │
│            Backend: PostgreSQL (database: prefect)           │
└──────────────────────────┬──────────────────────────────────┘
                           │  giao tiếp qua HTTP API
                           │
┌──────────────────────────▼──────────────────────────────────┐
│              Prefect Worker (custom Docker image)            │
│         Pool: default-process-pool (type: Process)           │
│         Runs flows trong prefect/flows/                      │
│         Có sẵn: pandas · pyiceberg · dbt · kafka · mlflow   │
└──────────────────────────────────────────────────────────────┘
```

**Nguyên tắc hoạt động:**
- **Server** biết lịch chạy, lưu logs, hiển thị UI – nhưng KHÔNG chạy code
- **Worker** kéo jobs từ Server, thực sự execute flow code
- **Work Pool** là hàng đợi kết nối Server ↔ Worker. Pool `default-process-pool` dùng type **Process** – mỗi flow run là 1 subprocess trên máy worker

---

## Mục lục

1. [Kiểm tra services đang chạy](#1-kiểm-tra-services-đang-chạy)
2. [Tạo Work Pool](#2-tạo-work-pool)
3. [Cấu trúc flow files](#3-cấu-trúc-flow-files)
4. [Flow 1 – Bronze Batch Ingestion](#4-flow-1--bronze-batch-ingestion)
5. [Flow 2 – dbt Silver & Gold Transforms](#5-flow-2--dbt-silver--gold-transforms)
6. [Flow 3 – Full Pipeline Orchestrator](#6-flow-3--full-pipeline-orchestrator)
7. [Deploy flows](#7-deploy-flows)
8. [Chạy flow từ UI](#8-chạy-flow-từ-ui)
9. [Chạy flow từ CLI](#9-chạy-flow-từ-cli)
10. [Đặt lịch tự động (Schedule)](#10-đặt-lịch-tự-động-schedule)
11. [Monitoring & Logs](#11-monitoring--logs)
12. [Xử lý lỗi thường gặp](#12-xử-lý-lỗi-thường-gặp)

---

## 1. Kiểm tra services đang chạy

```bash
# Đảm bảo cả 2 service healthy trước khi làm gì
docker compose ps prefect-server prefect-worker
```

Kết quả mong đợi:

```
NAME                   STATUS          PORTS
olist-prefect-server   Up (healthy)    0.0.0.0:4200->4200/tcp
olist-prefect-worker   Up              
```

Kiểm tra API:

```bash
curl -s http://localhost:4200/api/health
# {"status":"healthy"}
```

Mở UI: **http://localhost:4200**

---

## 2. Tạo Work Pool

> Worker đã được cấu hình chờ pool tên `default-process-pool`. Pool này phải tồn tại trước khi worker có thể nhận jobs.

### Cách A – Tạo qua UI (dễ nhất)

1. Mở http://localhost:4200
2. Sidebar trái → **Work Pools**
3. Click **+** (Create Work Pool)
4. Điền:
   - **Name:** `default-process-pool`
   - **Type:** `Process` (chọn từ dropdown)
5. Click **Create**

### Cách B – Tạo qua CLI bên trong worker container

```bash
docker exec olist-prefect-worker \
  prefect work-pool create default-process-pool --type process
```

### Xác nhận worker đã kết nối

```bash
docker logs olist-prefect-worker --tail 20
```

Phải thấy dòng:

```
Worker 'PrefectWorker ...' started!
```

Trên UI → **Work Pools** → **default-process-pool** → tab **Workers** → thấy 1 worker **ONLINE**.

---

## 3. Cấu trúc flow files

Các flow đặt trong `prefect/flows/` – được mount vào `/app/flows/` trong worker container:

```
prefect/flows/
├── bronze_ingestion.py   ← Ingest CSV → Bronze Iceberg
├── dbt_transforms.py     ← dbt Silver + Gold
└── full_pipeline.py      ← Orchestrator (chạy cả 3 bước)
```

> **Lưu ý:** Worker đọc file từ `/app/flows/`. Khi sửa flow file trên máy local, không cần rebuild image – volume mount tự cập nhật. Chỉ cần re-deploy (Bước 7).

---

## 4. Flow 1 – Bronze Batch Ingestion

**File:** `prefect/flows/bronze_ingestion.py`

**Chức năng:**
- Đọc 10 file CSV từ `data/raw/`
- Tạo namespace `bronze` trên Iceberg REST catalog
- Ghi từng file vào Bronze Iceberg table trên R2
- Tạo summary artifact hiển thị số rows trên Prefect UI

**Tasks:**

| Task | Retry | Mô tả |
|---|---|---|
| `ensure-bronze-namespace` | 2 lần | Tạo namespace `bronze` nếu chưa có |
| `ingest-csv-to-iceberg` | 1 lần | Đọc CSV → PyArrow → Iceberg append |

**Params flow có thể nhận:**

| Param | Default | Mô tả |
|---|---|---|
| `data_root` | `/app/data/raw` | Đường dẫn thư mục chứa CSV |

---

## 5. Flow 2 – dbt Silver & Gold Transforms

**File:** `prefect/flows/dbt_transforms.py`

**Gồm 2 sub-flows riêng biệt:**

### `silver-transform`
- Chạy `dbt run --select staging intermediate`
- Chạy `dbt test --select staging intermediate`
- Tạo artifact với dbt output log

### `gold-transform`
- Chạy `dbt run --select marts`
- Chạy `dbt test --select marts`
- Tạo artifact với dbt output log

**Params:**

| Param | Default | Mô tả |
|---|---|---|
| `full_refresh` | `false` | Nếu `true`: xóa và rebuild toàn bộ table |

> **Yêu cầu:** `dbt/` project phải có sẵn tại `/app/dbt/` trong worker (volume mount từ `./dbt`). Nếu dbt project chưa tồn tại, flow sẽ fail.

---

## 6. Flow 3 – Full Pipeline Orchestrator

**File:** `prefect/flows/full_pipeline.py`

**Chức năng:** Gọi lần lượt 3 flow theo đúng thứ tự:

```
bronze_ingestion_flow()
    ↓
silver_transform_flow()
    ↓
gold_transform_flow()
```

**Params:**

| Param | Default | Mô tả |
|---|---|---|
| `full_refresh` | `false` | Truyền xuống cả Silver và Gold dbt |
| `skip_bronze` | `false` | `true` = bỏ qua Bronze, chỉ chạy dbt |

**Dùng khi:**
- Lần đầu setup: `full_pipeline_flow()` – chạy hết
- Hàng ngày: `full_pipeline_flow(skip_bronze=True)` – chỉ transform, không re-ingest
- Reset toàn bộ: `full_pipeline_flow(full_refresh=True)`

---

## 7. Deploy flows

> Deploy = đăng ký flow với Prefect Server để Server biết flow này tồn tại, chạy ở đâu, và có thể trigger.

### Cách A – Deploy từ bên trong worker container (đơn giản nhất)

```bash
# Vào shell của worker
docker exec -it olist-prefect-worker bash

# Bên trong container:
cd /app/flows

# Deploy bronze ingestion (chạy thủ công)
prefect deploy bronze_ingestion.py:bronze_ingestion_flow \
  --name "bronze-batch-ingestion" \
  --pool default-process-pool

# Deploy full pipeline (có schedule hàng ngày lúc 2:00 AM)
prefect deploy full_pipeline.py:full_pipeline_flow \
  --name "full-pipeline-daily" \
  --pool default-process-pool \
  --cron "0 2 * * *"

# Deploy silver transform riêng (trigger thủ công)
prefect deploy dbt_transforms.py:silver_transform_flow \
  --name "silver-transform" \
  --pool default-process-pool

# Deploy gold transform riêng (trigger thủ công)
prefect deploy dbt_transforms.py:gold_transform_flow \
  --name "gold-transform" \
  --pool default-process-pool
```

### Cách B – Dùng `prefect.yaml` (tốt hơn, version control được)

Tạo file `prefect/prefect.yaml`:

```yaml
name: olist-lakehouse
prefect-version: "3.*"

build: null
push: null

pull:
  - prefect.deployments.steps.set_working_directory:
      directory: /app/flows

deployments:

  - name: bronze-batch-ingestion
    entrypoint: bronze_ingestion.py:bronze_ingestion_flow
    work_pool:
      name: default-process-pool
    parameters:
      data_root: /app/data/raw
    description: "Ingest Olist CSVs into Bronze Iceberg tables."

  - name: silver-transform
    entrypoint: dbt_transforms.py:silver_transform_flow
    work_pool:
      name: default-process-pool
    parameters:
      full_refresh: false

  - name: gold-transform
    entrypoint: dbt_transforms.py:gold_transform_flow
    work_pool:
      name: default-process-pool
    parameters:
      full_refresh: false

  - name: full-pipeline-daily
    entrypoint: full_pipeline.py:full_pipeline_flow
    work_pool:
      name: default-process-pool
    parameters:
      full_refresh: false
      skip_bronze: false
    schedules:
      - cron: "0 2 * * *"
        timezone: "Asia/Ho_Chi_Minh"
        active: true
    description: "Daily full pipeline at 2:00 AM ICT."
```

Chạy deploy:

```bash
docker exec -it olist-prefect-worker bash -c \
  "cd /app/flows && prefect deploy --all --prefect-file /app/flows/../prefect.yaml"
```

### Xác nhận deploy thành công

UI → **Deployments** → phải thấy 4 deployments:

```
bronze-batch-ingestion      (no schedule)
silver-transform            (no schedule)
gold-transform              (no schedule)
full-pipeline-daily         (cron: 0 2 * * *)
```

---

## 8. Chạy flow từ UI

### Trigger thủ công

1. Mở http://localhost:4200
2. Sidebar → **Deployments**
3. Click deployment muốn chạy (ví dụ `bronze-batch-ingestion`)
4. Click nút **Run** (góc phải trên)
5. Hộp thoại **Custom Run** xuất hiện:
   - **Parameters**: có thể override params tại đây (JSON format)
   - Click **Run**

### Theo dõi run đang chạy

1. Sidebar → **Flow Runs**
2. Click vào run name (trạng thái: `Running` / `Completed` / `Failed`)
3. Xem **Timeline** – từng task, thời gian, trạng thái
4. Xem **Logs** – log realtime của flow
5. Xem **Artifacts** – bảng summary rows sau khi xong

---

## 9. Chạy flow từ CLI

### Từ bên ngoài container (máy host)

Cần đặt biến môi trường trỏ vào Prefect Server:

```bash
export PREFECT_API_URL=http://localhost:4200/api

# Trigger deployment có tên
prefect deployment run 'bronze-batch-ingestion/bronze-batch-ingestion'

# Trigger với override params
prefect deployment run 'full-pipeline-daily/full-pipeline-daily' \
  -p skip_bronze=true

# Xem danh sách deployments
prefect deployment ls

# Xem 10 run gần nhất
prefect flow-run ls
```

### Từ bên trong worker container

```bash
docker exec -it olist-prefect-worker bash

# Chạy trực tiếp (không qua deployment, để test nhanh)
cd /app/flows
python bronze_ingestion.py

# Hoặc trigger deployment
prefect deployment run 'full-pipeline-daily/full-pipeline-daily'
```

---

## 10. Đặt lịch tự động (Schedule)

### Xem schedule hiện tại

UI → **Deployments** → click `full-pipeline-daily` → tab **Schedules**

### Sửa / thêm schedule qua UI

1. Vào deployment → tab **Schedules** → **+ Add Schedule**
2. Chọn loại:
   - **Cron**: dùng cron expression (ví dụ `0 2 * * *` = 2 AM mỗi ngày)
   - **Interval**: chạy mỗi N giây/phút/giờ
   - **RRule**: rrule phức tạp (mỗi thứ 2, thứ 4, v.v.)

### Cron expression tham khảo

| Schedule | Cron expression |
|---|---|
| Mỗi ngày 2:00 AM | `0 2 * * *` |
| Mỗi giờ | `0 * * * *` |
| Mỗi 15 phút | `*/15 * * * *` |
| Thứ 2 đầu tuần 6:00 AM | `0 6 * * 1` |
| Mỗi ngày 2:00 AM (múi giờ VN) | `0 2 * * *` + timezone `Asia/Ho_Chi_Minh` |

### Pause / Resume schedule

```bash
# Pause (tắt tạm schedule)
prefect deployment set-schedule 'full-pipeline-daily/full-pipeline-daily' --inactive

# Resume
prefect deployment set-schedule 'full-pipeline-daily/full-pipeline-daily' --active
```

---

## 11. Monitoring & Logs

### Dashboard chính

**http://localhost:4200** → trang chủ hiện:
- **Flow Runs** – số runs theo trạng thái (running / completed / failed)
- **Task Runs** – từng task trong mỗi flow
- **Timelines** – Gantt chart của các runs

### Xem logs realtime

```bash
# Xem log của worker (tất cả flow runs)
docker logs olist-prefect-worker -f

# Hoặc xem trên UI: Flow Runs → chọn run → tab Logs
```

### Alerts khi flow fail

UI → **Notifications** (sidebar) → **Add Notification**:
- **Type**: Email / Slack / PagerDuty / Webhook
- **Trigger**: `Failed`, `Crashed`, `Cancelled`
- **Scope**: chọn deployment cụ thể hoặc tất cả

> Notification cần cấu hình thêm (SMTP cho email, webhook URL cho Slack). Với self-hosted Prefect Server, không có cloud notification mặc định.

### Xem artifacts

UI → **Artifacts** → xem bảng summary từ mỗi flow run (số rows ingested, dbt output log).

### Retry thủ công 1 run bị fail

UI → **Flow Runs** → click run bị fail → **Retry** (góc phải trên).

---

## 12. Xử lý lỗi thường gặp

### Worker không thấy pool / vẫn OFFLINE

**Nguyên nhân:** Pool `default-process-pool` chưa tạo.

```bash
# Kiểm tra pool có tồn tại không:
docker exec olist-prefect-worker prefect work-pool ls
# Nếu không thấy → tạo lại (Bước 2)
```

### Flow run bị PENDING mãi, không chạy

**Nguyên nhân:** Worker đang down hoặc không kết nối được Server.

```bash
# Kiểm tra worker logs:
docker logs olist-prefect-worker --tail 30

# Restart worker:
docker compose restart prefect-worker
```

### `FileNotFoundError` – không tìm thấy CSV

**Nguyên nhân:** `data/` chưa được tải từ Kaggle, hoặc volume mount sai.

```bash
# Kiểm tra file có tồn tại trong container không:
docker exec olist-prefect-worker ls /app/data/raw/ecommerce/
```

Nếu trống → chạy `DATA_INGESTION.md` Bước 4 trước.

### dbt flow fail: `dbt: command not found`

**Nguyên nhân:** Image worker chưa được build lại sau khi thêm dbt vào requirements.

```bash
docker compose build prefect-worker
docker compose up -d prefect-worker
```

### Iceberg connection refused

**Nguyên nhân:** `iceberg-rest` chưa healthy.

```bash
# Kiểm tra:
curl http://localhost:8181/v1/config
docker logs olist-iceberg-rest --tail 20
```

### Flow chạy thành công nhưng không thấy data trong Iceberg

**Nguyên nhân thường gặp:** R2 credentials trong container sai.

```bash
# Kiểm tra env vars đúng không:
docker exec olist-prefect-worker env | grep -E "S3_|ICEBERG"
```

---

## Tóm tắt thứ tự setup

```bash
# 1. Chạy stack
docker compose up -d
docker compose ps   # chờ tất cả healthy

# 2. Tạo work pool
docker exec olist-prefect-worker \
  prefect work-pool create default-process-pool --type process

# 3. Deploy tất cả flows
docker exec -it olist-prefect-worker bash -c "
  cd /app/flows &&
  prefect deploy bronze_ingestion.py:bronze_ingestion_flow \
    --name bronze-batch-ingestion --pool default-process-pool &&
  prefect deploy dbt_transforms.py:silver_transform_flow \
    --name silver-transform --pool default-process-pool &&
  prefect deploy dbt_transforms.py:gold_transform_flow \
    --name gold-transform --pool default-process-pool &&
  prefect deploy full_pipeline.py:full_pipeline_flow \
    --name full-pipeline-daily --pool default-process-pool \
    --cron '0 2 * * *'
"

# 4. Trigger chạy thủ công lần đầu
docker exec olist-prefect-worker \
  prefect deployment run 'full-pipeline-daily/full-pipeline-daily'

# 5. Theo dõi
# Mở http://localhost:4200 → Flow Runs
```
