# CHƯƠNG 5. ĐÁNH GIÁ HỆ THỐNG

## 5.1 Kết quả đạt được

Sau quá trình triển khai, hệ thống đã đáp ứng toàn bộ functional requirements đề ra trong Chương 3. Bảng dưới đây tóm tắt kết quả theo từng thành phần:

| Thành phần | FR | Kết quả |
|---|---|---|
| Bronze Batch Ingestion | FR-01 | 10 bảng CSV → Iceberg, ~100K đơn hàng + ~1M geolocation records |
| Bronze Streaming Ingestion | FR-02 | Replay 100K đơn hàng qua Redpanda, consumer ghi vào Iceberg liên tục |
| Data Quality Checks | FR-03 | Soda Core checks pass trên tất cả Bronze tables (0 null key, 0 duplicate) |
| Silver Transformation | FR-04 | 7 stg models + 2 int models, tất cả dbt tests pass |
| Gold Transformation | FR-04 | 4 Gold tables (fct_orders, dim_sellers, dim_customers, fct_funnel), partition theo tháng |
| Pipeline Orchestration | FR-05 | Prefect full pipeline flow chạy end-to-end, run history hiển thị trên Cloud UI |
| Analytics Dashboard | FR-06 | Streamlit 3-tab dashboard với 8 biểu đồ từ Gold Layer |
| Containerized Deployment | FR-07 | `docker compose up` khởi động toàn bộ 6 services, healthy trong ~60 giây |

### 5.1.1 Iceberg Tables đã tạo

Sau khi chạy full pipeline, R2 bucket có cấu trúc:

```
s3://olist-lakehouse/
├── bronze/
│   ├── ecom/
│   │   ├── orders/          (data/*.parquet + metadata/)
│   │   ├── order_items/
│   │   ├── order_payments/
│   │   ├── order_reviews/
│   │   ├── customers/
│   │   ├── sellers/
│   │   ├── products/
│   │   └── geolocation/
│   └── marketing/
│       ├── leads/
│       └── deals/
├── silver/
│   ├── stg_orders/
│   ├── stg_sellers/
│   └── ...
└── gold/
    ├── fct_orders/
    │   └── data/
    │       ├── order_date_month=2016-09/
    │       ├── order_date_month=2016-10/
    │       └── ... (26 partitions)
    ├── dim_sellers/
    ├── dim_customers/
    └── fct_funnel/
```

### 5.1.2 Prefect Pipeline Run

Kết quả một full pipeline run điển hình:

| Task | Thời gian | Trạng thái |
|---|---|---|
| Upload 10 tables to R2 (parallel) | ~45 giây | Completed |
| Ingest 10 tables to Bronze (parallel) | ~90 giây | Completed |
| Soda Core quality checks (10 tables) | ~30 giây | All Passed |
| dbt run Silver (7 models) | ~120 giây | Completed |
| dbt run Gold (4 models) | ~60 giây | Completed |
| dbt test | ~45 giây | All Passed |
| **Tổng** | **~6–7 phút** | **Completed** |

Thời gian pipeline ~6–7 phút là chấp nhận được cho daily batch job trên dataset 200MB. Bottleneck chính là Iceberg write (tạo metadata + upload Parquet files lên R2 qua network).

---

## 5.2 Đánh giá hiệu năng

### 5.2.1 Query Performance (DuckDB trên Gold Layer)

Benchmark thực hiện trên machine 4 CPU / 16GB RAM, DuckDB đọc Gold Parquet files từ R2 qua HTTPS:

| Query | Mô tả | Thời gian |
|---|---|---|
| Q1 — Simple aggregate | `SELECT SUM(net_revenue) FROM fct_orders WHERE order_status='delivered'` | 0.8 giây |
| Q2 — Monthly revenue | GROUP BY order_date, tổng hợp 26 tháng | 1.2 giây |
| Q3 — Multi-table join | `fct_orders JOIN dim_sellers JOIN dim_customers`, filter by state | 2.4 giây |
| Q4 — Window function | Monthly revenue YoY comparison với LAG() | 1.9 giây |
| Q5 — Top-N sellers | Rank sellers by revenue, filter top 10 | 1.5 giây |

Tất cả queries đáp ứng NFR-04 (< 5 giây). Thời gian chủ yếu là network latency đọc files từ R2 (~1–2 giây overhead) chứ không phải CPU/memory bottleneck — khi chạy local với files cached, queries < 0.2 giây.

**Hiệu quả Partition Pruning**: Query có filter `WHERE order_date BETWEEN '2018-01-01' AND '2018-06-30'` chỉ đọc 6/26 partitions (23% data), so với không có partition filter phải đọc toàn bộ. DuckDB tự động áp dụng partition pruning nhờ Iceberg metadata.

### 5.2.2 Streaming Throughput

Benchmark streaming pipeline trên local Docker Compose:

| Metric | Kết quả |
|---|---|
| Producer throughput | ~2,000 messages/giây (khi không có sleep delay) |
| Consumer batch latency (100 msgs) | ~1.5–2 giây (bao gồm Iceberg append + R2 write) |
| End-to-end latency (produce → Bronze visible) | ~3–5 giây |
| Messages processed trong 1 phút (replay mode) | ~120 events (tương đương 120 ngày lịch sử khi SPEED_FACTOR=86400) |

End-to-end latency ~3–5 giây đáp ứng yêu cầu "near-realtime" cho use case e-commerce monitoring.

### 5.2.3 Pipeline Reliability

Kiểm tra retry mechanism bằng cách inject lỗi nhân tạo:

**Test 1 — Network timeout khi upload R2**: Task `upload_raw` fail lần 1, Prefect tự động retry sau 30 giây, thành công lần 2. Flow tiếp tục bình thường.

**Test 2 — Redpanda restart giữa chừng**: Consumer mất kết nối, reconnect sau ~5 giây (confluent-kafka auto reconnect), đọc lại từ last committed offset. Không mất message.

**Test 3 — dbt model syntax error**: Task `run_dbt_silver` fail sau 2 retries, Prefect flow fail và gửi notification. Gold layer không bị ảnh hưởng (chưa chạy đến).

**Test 4 — Soda Core quality fail**: Inject 5 null `order_id` vào Bronze batch. Soda check phát hiện, task fail, flow dừng — dbt transform không chạy, Silver/Gold không bị nhiễm dữ liệu xấu.

---

## 5.3 So sánh với kiến trúc truyền thống

So sánh Unified Lakehouse (đề tài) với kiến trúc Data Warehouse truyền thống (PostgreSQL + Python ETL):

| Tiêu chí | Traditional (PostgreSQL + ETL) | Lakehouse (Iceberg + dbt) |
|---|---|---|
| **Schema thay đổi** | Viết `ALTER TABLE`, migrate dữ liệu cũ | Schema Evolution tự động, không downtime |
| **Streaming support** | Phức tạp (trigger/CDC/Debezium) | Native: Iceberg ACID cho concurrent batch+stream write |
| **Time travel** | Không có | `FOR VERSION AS OF` built-in |
| **Storage cost** | Database storage ($0.10/GB/month PostgreSQL RDS) | Object storage ($0.015/GB/month R2) |
| **Query engine flexibility** | Chỉ PostgreSQL | DuckDB, Spark, Trino — cùng data |
| **Data pipeline code** | Python scripts, dễ drift | dbt models: versioned SQL, auto-tested |
| **Reproducibility** | Khó (data thay đổi in-place) | Cao (Bronze immutable, snapshots) |
| **Scale path** | Vertical scaling (bigger server) | Horizontal (thêm Spark workers, tách storage) |
| **Operational complexity** | Thấp (một DB instance) | Cao hơn (nhiều services) |
| **Local dev cost** | Thấp | Trung bình (Docker Compose ~3GB RAM) |

**Nhận xét**: Lakehouse có lợi thế rõ ràng về flexibility, streaming support và scale path. Trade-off là operational complexity cao hơn — nhiều services cần quản lý. Tuy nhiên, Docker Compose và Prefect giảm đáng kể gánh nặng vận hành so với tự quản lý từng service.

---

## 5.4 Hạn chế hệ thống

**H-01 — Single-node deployment**: Toàn bộ hệ thống chạy trên một machine (Docker Compose). Nếu dataset vượt RAM của machine (~16GB), DuckDB sẽ spill to disk và performance giảm đáng kể. Giải pháp: thêm Apache Spark cluster (Iceberg tables đã tương thích).

**H-02 — Redpanda data persistence**: Nếu Redpanda container bị xóa mà không mount volume cho `/var/lib/redpanda/data`, toàn bộ message history bị mất. Trong production, cần `volumes: - redpanda-data:/var/lib/redpanda/data` và backup policy.

**H-03 — DuckDB single-writer**: DuckDB không hỗ trợ concurrent writes — chỉ một process có thể write tại một thời điểm. Dashboard Streamlit (read-only) và dbt (write) không thể chạy đồng thời trên cùng DuckDB file. Giải pháp: dùng in-memory DuckDB (`:memory:`) cho từng session, đọc trực tiếp từ Parquet/Iceberg trên R2.

**H-04 — Iceberg metadata bottleneck**: Với Iceberg REST Catalog lưu metadata trong memory (tabulario/iceberg-rest), restart container xóa sạch catalog state. Cần persist catalog backend (JDBC catalog với PostgreSQL) cho production.

**H-05 — Không có data governance**: Chưa có column-level masking (PII protection cho customer data), row-level security, hay audit log. Cần Apache Atlas hoặc OpenMetadata cho production compliance.

**H-06 — Chưa có CI/CD**: Chưa có GitHub Actions pipeline tự động chạy `dbt test` khi có PR thay đổi Silver/Gold models. Rủi ro: deploy model lỗi vào production.

---

*Tổng kết đóng góp và định hướng phát triển được trình bày trong Chương 6.*
