# Olist Data Lakehouse – Hướng dẫn Setup Chi tiết

> **Mục đích tài liệu này:** Liệt kê toàn bộ credential, API token, biến môi trường và các bước cần thực hiện để chạy stack từ đầu. Đọc hết trước khi chạy lệnh nào.

---

## Mục lục

1. [Yêu cầu hệ thống](#1-yêu-cầu-hệ-thống)
2. [Tổng quan credential cần chuẩn bị](#2-tổng-quan-credential-cần-chuẩn-bị)
3. [Cloudflare R2 – Setup từng bước](#3-cloudflare-r2--setup-từng-bước)
4. [Anthropic API Key](#4-anthropic-api-key)
5. [Tạo file .env](#5-tạo-file-env)
6. [File cấu hình cần kiểm tra / sửa thủ công](#6-file-cấu-hình-cần-kiểm-tra--sửa-thủ-công)
7. [Chuẩn bị Spark JARs (nếu dùng profile spark)](#7-chuẩn-bị-spark-jars-nếu-dùng-profile-spark)
8. [Thứ tự khởi động & lệnh chạy](#8-thứ-tự-khởi-động--lệnh-chạy)
9. [Kiểm tra từng service sau khi khởi động](#9-kiểm-tra-từng-service-sau-khi-khởi-động)
10. [Bảng port reference](#10-bảng-port-reference)
11. [Xử lý lỗi thường gặp](#11-xử-lý-lỗi-thường-gặp)

---

## 1. Yêu cầu hệ thống

| Thứ | Yêu cầu tối thiểu | Ghi chú |
|---|---|---|
| Docker Engine | >= 24.0 | `docker --version` |
| Docker Compose | >= 2.20 (plugin) | `docker compose version` |
| RAM | 8 GB trống | 16 GB nếu bật profile spark |
| Disk | 20 GB trống | Cho volumes và images |
| OS | Linux / macOS / Windows WSL2 | Windows native: dùng WSL2 |

Kiểm tra nhanh:

```bash
docker --version
docker compose version
docker info | grep "Total Memory"
```

---

## 2. Tổng quan credential cần chuẩn bị

Bảng tóm tắt TẤT CẢ thứ cần lấy trước khi chạy:

| # | Credential | Bắt buộc? | Lấy ở đâu | Dùng cho |
|---|---|---|---|---|
| 1 | `S3_ENDPOINT` | **Bắt buộc** | Cloudflare dashboard | Tất cả service truy cập R2 |
| 2 | `S3_ACCESS_KEY` | **Bắt buộc** | Cloudflare R2 API Token | Tất cả service truy cập R2 |
| 3 | `S3_SECRET_KEY` | **Bắt buộc** | Cloudflare R2 API Token | Tất cả service truy cập R2 |
| 4 | `POSTGRES_PASSWORD` | Tuỳ chọn | Tự đặt | PostgreSQL |
| 5 | `ANTHROPIC_API_KEY` | Chỉ khi dùng `--profile bi` | console.anthropic.com | Streamlit BI (Agentic AI) |
| 6 | `SPARK_WORKER_CORES` | Tuỳ chọn | Tự đặt | Spark worker (mặc định: 2) |
| 7 | `SPARK_WORKER_MEMORY` | Tuỳ chọn | Tự đặt | Spark worker (mặc định: 2G) |

---

## 3. Cloudflare R2 – Setup từng bước

### Bước 3.1 – Lấy Account ID

1. Đăng nhập **Cloudflare dashboard**: https://dash.cloudflare.com
2. Ở thanh sidebar bên phải (hoặc URL trình duyệt), copy **Account ID**

   ```
   Ví dụ Account ID: a1b2c3d4e5f6789012345678901234567
   ```

3. Endpoint R2 của bạn sẽ có dạng:

   ```
   S3_ENDPOINT=https://a1b2c3d4e5f6789012345678901234567.r2.cloudflarestorage.com
   ```

### Bước 3.2 – Tạo bucket `olist-lakehouse`

> **Quan trọng:** Bucket phải tạo thủ công qua dashboard, KHÔNG thể tự tạo qua docker compose.

1. Trong Cloudflare dashboard → chọn **R2 Object Storage** (sidebar trái)
2. Click **Create bucket**
3. Đặt tên bucket: `olist-lakehouse`
4. Location: chọn **Automatic** (hoặc region gần nhất)
5. Click **Create bucket**

> Sau khi tạo xong, stack sẽ tự dùng các prefix (thư mục ảo) sau trong bucket này:
> - `olist-lakehouse/` → Iceberg tables (bronze / silver / gold)
> - `olist-lakehouse/mlflow/` → MLflow artifacts

### Bước 3.3 – Tạo R2 API Token

1. Trong R2 dashboard → Click **Manage R2 API Tokens** (góc phải trên)
2. Click **Create API Token**
3. Điền thông tin:

   | Field | Giá trị |
   |---|---|
   | Token name | `olist-lakehouse-rw` (đặt tên gì cũng được) |
   | Permissions | **Object Read & Write** |
   | Specify bucket | Chọn `olist-lakehouse` (giới hạn quyền vào 1 bucket) |
   | TTL | Không giới hạn (hoặc tuỳ policy bảo mật) |

4. Click **Create API Token**
5. **Lưu lại ngay** – trang này chỉ hiện 1 lần:

   ```
   Access Key ID:  <đây là S3_ACCESS_KEY>
   Secret Access Key: <đây là S3_SECRET_KEY>
   ```

   > Nếu lỡ tắt tab mà không lưu → phải tạo lại token mới.

### Bước 3.4 – Kiểm tra R2 kết nối (tuỳ chọn nhưng khuyến khích)

Cần có AWS CLI cài sẵn:

```bash
aws s3 ls s3://olist-lakehouse \
  --endpoint-url https://<ACCOUNT_ID>.r2.cloudflarestorage.com \
  --region auto \
  --no-verify-ssl
```

Nếu không báo lỗi → credentials đúng và bucket tồn tại.

---

## 4. Anthropic API Key

> **Chỉ cần** nếu chạy profile `bi` (Streamlit Agentic BI). Core stack không cần.

1. Vào https://console.anthropic.com
2. **API Keys** → **Create Key**
3. Đặt tên: `olist-bi` (tuỳ)
4. Copy key (dạng `sk-ant-api03-...`)

```
ANTHROPIC_API_KEY=sk-ant-api03-xxxxxxxxxxxxxxxx
```

---

## 5. Tạo file .env

File `.env` đặt cạnh `docker-compose.yml`. File này **đã gitignore** – không commit lên git.

```bash
cp .env.example .env
```

Mở `.env` và điền đầy đủ:

```dotenv
# ─── PostgreSQL ──────────────────────────────────────────────
# Mật khẩu cho database nội bộ. Đặt gì cũng được, không expose ra ngoài.
POSTGRES_PASSWORD=olistpass

# ─── Cloudflare R2 ───────────────────────────────────────────
# Lấy từ Bước 3 ở trên

# Format: https://<ACCOUNT_ID>.r2.cloudflarestorage.com
S3_ENDPOINT=https://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX.r2.cloudflarestorage.com

# Lấy từ R2 API Token (Bước 3.3)
S3_ACCESS_KEY=<Access Key ID từ R2 API Token>
S3_SECRET_KEY=<Secret Access Key từ R2 API Token>

# Giữ nguyên 2 giá trị này cho R2:
S3_REGION=auto
S3_PATH_STYLE=false

# ─── Spark (chỉ cần nếu dùng --profile spark) ────────────────
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2G

# ─── Anthropic (chỉ cần nếu dùng --profile bi) ───────────────
ANTHROPIC_API_KEY=sk-ant-api03-xxxxxxxxxxxxxxxx
```

### Checklist biến môi trường theo service

| Biến | Dùng trong service | Ghi chú |
|---|---|---|
| `POSTGRES_PASSWORD` | postgres, iceberg-rest, prefect-server, prefect-worker, mlflow, trino | Mặc định `olistpass` nếu không set |
| `S3_ENDPOINT` | iceberg-rest, trino, prefect-worker, mlflow, streamlit | **Bắt buộc** – không có default |
| `S3_ACCESS_KEY` | iceberg-rest, spark-master, spark-worker, trino, prefect-worker, mlflow, streamlit | **Bắt buộc** – không có default |
| `S3_SECRET_KEY` | iceberg-rest, spark-master, spark-worker, trino, prefect-worker, mlflow, streamlit | **Bắt buộc** – không có default |
| `S3_REGION` | iceberg-rest, trino, prefect-worker, mlflow, streamlit | Mặc định `auto` |
| `S3_PATH_STYLE` | iceberg-rest, trino | Mặc định `false` |
| `SPARK_WORKER_CORES` | spark-worker | Mặc định `2` |
| `SPARK_WORKER_MEMORY` | spark-worker | Mặc định `2G` |
| `ANTHROPIC_API_KEY` | streamlit | Bắt buộc nếu dùng `--profile bi` |

---

## 6. File cấu hình cần kiểm tra / sửa thủ công

### 6.1 `spark/conf/spark-defaults.conf` – CÒN HARDCODE MINIO

> **Cảnh báo:** File này vẫn còn hardcode `http://minio:9000`. Nếu dùng profile `spark`, PHẢI sửa trước.

Mở file `spark/conf/spark-defaults.conf` và thay 2 block cuối:

**Tìm đoạn cũ:**

```properties
spark.sql.catalog.olist.s3.endpoint             http://minio:9000
spark.sql.catalog.olist.s3.path-style-access    true

spark.hadoop.fs.s3a.endpoint                    http://minio:9000
spark.hadoop.fs.s3a.path.style.access          true
spark.hadoop.fs.s3a.impl                        org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled      false
```

**Thay bằng:**

```properties
spark.sql.catalog.olist.s3.endpoint             https://<ACCOUNT_ID>.r2.cloudflarestorage.com
spark.sql.catalog.olist.s3.path-style-access    false

spark.hadoop.fs.s3a.endpoint                    https://<ACCOUNT_ID>.r2.cloudflarestorage.com
spark.hadoop.fs.s3a.path.style.access          false
spark.hadoop.fs.s3a.impl                        org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled      true
```

> Thay `<ACCOUNT_ID>` bằng Account ID thật từ Bước 3.1.

### 6.2 `trino/catalog/iceberg.properties` – Đã OK

File này dùng biến môi trường `${ENV:S3_ENDPOINT}` nên không cần sửa. Chỉ cần `.env` đúng là xong.

### 6.3 `trino/catalog/postgres.properties` – Đã OK

Chỉ dùng `${ENV:POSTGRES_PASSWORD}`, không cần sửa.

### 6.4 `infra/postgres/init.sql` – Cần kiểm tra

Đảm bảo file này tạo đủ 3 database:

```sql
-- File phải có ít nhất:
CREATE DATABASE iceberg;   -- cho Iceberg REST catalog
CREATE DATABASE prefect;   -- cho Prefect metadata
CREATE DATABASE mlflow;    -- cho MLflow tracking
```

Nếu thiếu database nào → service tương ứng sẽ crash khi khởi động.

---

## 7. Chuẩn bị Spark JARs (nếu dùng profile spark)

Spark cần 3 file JAR. Chạy lệnh sau trong thư mục gốc project:

```bash
cd spark/jars

# Iceberg runtime cho Spark 3.5
curl -L -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar

# Hadoop AWS (S3A file system)
curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS Java SDK
curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

cd ../..
```

Kiểm tra:

```bash
ls -lh spark/jars/*.jar
# Phải thấy đủ 3 file, tổng khoảng ~300 MB
```

---

## 8. Thứ tự khởi động & lệnh chạy

### 8.1 Core stack (bắt buộc)

Bao gồm: PostgreSQL + Iceberg REST + Redpanda + Prefect + MLflow

```bash
docker compose up -d
```

Thứ tự khởi động tự động:

```
postgres (healthy)
    └── iceberg-rest
    └── prefect-server (healthy)
            └── prefect-worker
    └── mlflow
redpanda (healthy)
    └── redpanda-console
    └── redpanda-init  [one-shot: tạo topics]
```

### 8.2 Chờ stack sẵn sàng

```bash
# Theo dõi log realtime
docker compose logs -f

# Hoặc xem trạng thái health check
docker compose ps
```

Đợi tất cả service có `(healthy)` trước khi dùng.

### 8.3 Các profile tuỳ chọn

```bash
# Spark cluster (distributed processing)
docker compose --profile spark up -d

# Trino (query federation)
docker compose --profile query up -d

# Streamlit BI app
docker compose --profile bi up -d

# Full stack (tất cả)
docker compose --profile spark --profile query --profile bi up -d
```

### 8.4 Dừng stack

```bash
# Dừng nhưng giữ data (volumes)
docker compose down

# Dừng và XOÁ data (reset hoàn toàn)
docker compose down -v
```

---

## 9. Kiểm tra từng service sau khi khởi động

### PostgreSQL

```bash
docker exec olist-postgres pg_isready -U olist
# Output mong đợi: /var/run/postgresql:5432 - accepting connections

# Kiểm tra 3 database tồn tại:
docker exec olist-postgres psql -U olist -c "\l"
# Phải thấy: iceberg, prefect, mlflow, postgres
```

### Iceberg REST Catalog

```bash
curl http://localhost:8181/v1/config
# Output mong đợi: JSON với "defaults" và "overrides"
```

### Redpanda

```bash
# Health check
curl http://localhost:18082/v3/clusters
# Output mong đợi: JSON với cluster info

# Kiểm tra topics đã được tạo:
docker exec olist-redpanda rpk topic list
# Phải thấy: olist.orders, olist.reviews, olist.payments, olist.leads, olist.deals
```

**Redpanda Console UI:** http://localhost:8080

### Prefect

```bash
curl http://localhost:4200/api/health
# Output mong đợi: {"status":"healthy"}
```

**Prefect UI:** http://localhost:4200

Sau khi UI mở, kiểm tra:
- **Work Pools** → phải có pool `default-process-pool`
- **Workers** → phải có 1 worker online

### MLflow

```bash
curl http://localhost:5000/health
# Output mong đợi: OK
```

**MLflow UI:** http://localhost:5000

Thử upload artifact để xác nhận R2 kết nối được:

```bash
docker exec olist-mlflow python -c "
import mlflow, os
os.environ['MLFLOW_TRACKING_URI'] = 'http://localhost:5000'
with mlflow.start_run():
    mlflow.log_param('test', 'r2-connectivity')
    mlflow.log_metric('value', 1.0)
print('OK – artifact written to R2')
"
```

### Spark (nếu bật profile spark)

**Spark Master UI:** http://localhost:8090
**Spark Worker UI:** http://localhost:8091

Kiểm tra worker đã register với master:
- Mở http://localhost:8090
- Tab **Workers** → phải thấy 1 worker với trạng thái `ALIVE`

### Trino (nếu bật profile query)

```bash
curl http://localhost:8082/v1/info
# Output: JSON với "coordinator":true và "nodeVersion"
```

**Trino UI:** http://localhost:8082
- User: `admin` (nhập bất kỳ, Trino không yêu cầu mật khẩu mặc định)

Chạy test query:

```sql
-- Trong Trino UI hoặc dùng trino CLI:
SHOW CATALOGS;
-- Phải thấy: iceberg, postgresql, system, tpch, tpcds
```

### Streamlit (nếu bật profile bi)

**Streamlit App:** http://localhost:8501

---

## 10. Bảng port reference

| Port | Service | UI/API | URL |
|---|---|---|---|
| 5432 | PostgreSQL | Database | `postgresql://olist:<password>@localhost:5432` |
| 8181 | Iceberg REST | REST API | http://localhost:8181/v1/config |
| 8080 | Redpanda Console | Web UI | http://localhost:8080 |
| 18081 | Redpanda | Schema Registry | http://localhost:18081 |
| 18082 | Redpanda | HTTP Proxy | http://localhost:18082 |
| 19092 | Redpanda | Kafka API | `localhost:19092` (dùng cho Kafka clients) |
| 4200 | Prefect Server | Web UI | http://localhost:4200 |
| 5000 | MLflow | Web UI + REST | http://localhost:5000 |
| 7077 | Spark Master | RPC | `spark://localhost:7077` |
| 8090 | Spark Master | Web UI | http://localhost:8090 |
| 8091 | Spark Worker | Web UI | http://localhost:8091 |
| 8082 | Trino | Web UI + REST | http://localhost:8082 |
| 8501 | Streamlit | Web App | http://localhost:8501 |

---

## 11. Xử lý lỗi thường gặp

### "S3_ENDPOINT is not set" / service crash ngay khi start

**Nguyên nhân:** File `.env` chưa có hoặc thiếu biến R2.

```bash
# Kiểm tra .env đã load đúng chưa:
docker compose config | grep S3_ENDPOINT
# Phải thấy giá trị thật, không phải trống
```

### iceberg-rest khởi động thất bại

**Nguyên nhân thường gặp:**
1. PostgreSQL chưa tạo database `iceberg` → kiểm tra `infra/postgres/init.sql`
2. R2 credentials sai → kiểm tra `S3_ACCESS_KEY` / `S3_SECRET_KEY`
3. Bucket `olist-lakehouse` chưa tạo → xem lại Bước 3.2

```bash
docker logs olist-iceberg-rest --tail 50
```

### mlflow không ghi được artifact lên R2

**Nguyên nhân:** API token R2 chưa có quyền `Object Write`.

Kiểm tra:
1. Cloudflare dashboard → R2 → Manage R2 API Tokens
2. Tìm token đang dùng → kiểm tra permission là `Object Read & Write`
3. Đảm bảo token áp dụng cho bucket `olist-lakehouse`

### Spark worker không đọc được file từ R2

**Nguyên nhân:** `spark-defaults.conf` vẫn còn hardcode `http://minio:9000`.

→ Xem lại Mục 6.1 để sửa file.

### Redpanda health check fail

**Nguyên nhân:** Container cần 30s để khởi động đầy đủ. Đợi thêm.

```bash
# Xem log:
docker logs olist-redpanda --tail 30

# Force restart nếu stuck:
docker compose restart redpanda
```

### Trino không kết nối được Iceberg catalog

**Nguyên nhân:** `S3_PATH_STYLE` sai. Với R2 phải là `false`.

```bash
# Kiểm tra:
docker exec olist-trino env | grep S3
# S3_PATH_STYLE phải là false
```

### prefect-worker build fail

**Nguyên nhân:** `prefect/Dockerfile` cần build custom image.

```bash
# Build riêng để xem lỗi chi tiết:
docker compose build prefect-worker

# Nếu lỗi pip install → kiểm tra requirements-worker.txt
```

---

## Checklist tổng trước khi chạy

- [ ] Docker Engine >= 24.0 và Docker Compose plugin >= 2.20
- [ ] File `.env` đã tạo từ `.env.example`
- [ ] `S3_ENDPOINT` điền đúng (format: `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`)
- [ ] `S3_ACCESS_KEY` và `S3_SECRET_KEY` lấy từ R2 API Token
- [ ] Bucket `olist-lakehouse` đã tạo trên Cloudflare dashboard
- [ ] `spark/conf/spark-defaults.conf` đã thay địa chỉ MinIO → R2 (nếu dùng Spark)
- [ ] 3 file JAR đã tải vào `spark/jars/` (nếu dùng Spark)
- [ ] `ANTHROPIC_API_KEY` đã điền (nếu dùng profile bi)
- [ ] `infra/postgres/init.sql` có tạo đủ database: `iceberg`, `prefect`, `mlflow`
