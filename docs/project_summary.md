# Tóm Tắt Đồ Án: Olist Data Lakehouse Platform

**Sinh viên:** [Tên]  
**Ngày cập nhật:** 27/05/2026  
**Repository:** `C:\DEProject\Olist-data-engineering-project`

---

## 1. Tổng Quan

Xây dựng hệ thống **Data Lakehouse** hoàn chỉnh trên nền tảng Apache Iceberg và Cloudflare R2, áp dụng **Medallion Architecture** (Bronze → Silver → Gold) kết hợp **Lambda Architecture** (batch + streaming). Dữ liệu nguồn là bộ dataset thương mại điện tử Brazil Olist (~100K đơn hàng) và Marketing Funnel (~8K leads).

---

## 2. Kiến Trúc Tổng Thể

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                            │
│   CSV files (Olist e-commerce + Marketing Funnel)               │
└────────────────────┬──────────────────────┬─────────────────────┘
                     │ Batch                 │ Streaming (simulated)
                     ▼                       ▼
           ┌─────────────────┐     ┌──────────────────┐
           │  Batch Ingest   │     │   Redpanda       │
           │  (PyIceberg)    │     │  (Kafka-compat)  │
           └────────┬────────┘     └────────┬─────────┘
                    │                        │ Consumer
                    └───────────┬────────────┘
                                ▼
              ┌─────────────────────────────────┐
              │        BRONZE LAYER             │
              │  Append-only · Raw · Iceberg    │
              │  9 tables · ~197K orders        │
              └─────────────────┬───────────────┘
                                │ PySpark
                                ▼
              ┌─────────────────────────────────┐
              │        SILVER LAYER             │
              │  Cleaned · Typed · Joined       │
              │  8 staging + 1 intermediate     │
              └─────────────────┬───────────────┘
                                │ PySpark
                                ▼
              ┌─────────────────────────────────┐
              │         GOLD LAYER              │
              │  Dimensional model · Partitioned│
              │  fct_orders · fct_funnel        │
              │  dim_sellers · dim_customers    │
              └────────────┬────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
   ┌──────────────────┐      ┌────────────────────┐
   │   Agentic BI     │      │  Realtime Dashboard │
   │  NL → SQL →Chart │      │  Streamlit + Kafka  │
   └──────────────────┘      └────────────────────┘
```

---

## 3. Stack Công Nghệ

| Layer | Công nghệ | Vai trò |
|-------|-----------|---------|
| **Storage** | Cloudflare R2 (S3-compatible) | Object storage, free egress |
| **Table Format** | Apache Iceberg | ACID, time travel, schema evolution |
| **Ingestion** | PyIceberg + boto3 | Batch ingest CSV → Bronze |
| **Streaming** | Redpanda + confluent-kafka | Kafka-compatible broker, no JVM |
| **Processing** | Apache Spark (PySpark) | Silver + Gold transforms |
| **Catalog** | Iceberg REST + PostgreSQL | Table metadata, schema registry |
| **Query** | Trino | Ad-hoc SQL trên cả 3 layers |
| **Orchestration** | Prefect | Pipeline scheduling + monitoring |
| **BI** | Streamlit | Realtime dashboard + Agentic BI |
| **AI** | LLaMA 3.3 70B (OpenRouter) | Text-to-SQL |
| **ML** | MLflow | Experiment tracking + model serving |
| **DevOps** | Docker Compose | Full stack containerization |

---

## 4. Chi Tiết Các Layer

### 4.1 Bronze Layer — Thu nạp Dữ liệu Thô

- **Nguyên tắc:** Append-only, immutable, giữ nguyên schema nguồn
- **Metadata columns:** `_ingested_at`, `_source_file`
- **Script:** `scripts/batch_ingest_bronze.py`
- **Công nghệ:** PyIceberg (Python client), đọc CSV từ R2 qua boto3

| Bảng | Rows |
|------|-----:|
| ecommerce_orders | 197,003 |
| ecommerce_order_items | 112,650 |
| ecommerce_customers | 99,441 |
| ecommerce_order_payments | 103,886 |
| ecommerce_order_reviews | 99,441 |
| ecommerce_products | 32,951 |
| ecommerce_sellers | 3,095 |
| marketing_leads | 8,000 |
| marketing_deals | 842 |

> Bronze có 197K orders vì append-only: nhận cả từ batch lẫn streaming path.

### 4.2 Silver Layer — Làm sạch và Chuẩn hóa

- **Nguyên tắc:** 1 staging table per source, source-conforming
- **Script:** `spark/jobs/silver_transform.py`
- **Công nghệ:** PySpark local[2], `createOrReplace()` cho idempotency

| Transform | Logic chính |
|-----------|-------------|
| `stg_orders` | Cast timestamp, tính delivery days, dedup |
| `stg_order_payments` | Aggregate 1 row/order, dominant payment type |
| `stg_order_reviews` | Dedup lấy review mới nhất (Window function) |
| `stg_customers` | Normalize text (lower/trim) |
| `stg_sellers` | Normalize city/state |
| `int_orders_enriched` | Pre-join: orders ⋈ customers ⋈ payments ⋈ reviews ⋈ items |

### 4.3 Gold Layer — Mô hình Chiều (Kimball)

- **Script:** `spark/jobs/gold_transform.py`
- **Công nghệ:** PySpark + Iceberg hidden partitioning

| Bảng | Rows | Partition | Mô tả |
|------|-----:|-----------|-------|
| `fct_orders` | 99,441 | `months(purchased_at)` | Fact chính: order + delivery + payment + review |
| `fct_funnel` | 8,000 | `years(first_contact_date)` | Marketing: MQL → deal conversion |
| `dim_sellers` | 3,095 | — | Profile + aggregated performance |
| `dim_customers` | 96,096 | — | CLV + churn features |

**Đặc điểm dim_customers:**
- Dedup `customer_unique_id` (1 người có thể có nhiều `customer_id` trong Olist)
- Features cho ML: `is_churned` (>90 ngày không mua), `is_repeat_customer`, `days_since_last_order`

---

## 5. Streaming Pipeline

- **Broker:** Redpanda (Kafka-compatible, ~500MB RAM, không JVM/Zookeeper)
- **Producer:** `streaming/producer.py` — 2 modes:
  - `rate`: N events/giây, vô hạn, shuffle dataset
  - `replay`: nén thời gian theo `SPEED_FACTOR`
- **Consumer:** `streaming/consumer.py`
  - Dual-trigger flush: `BATCH_SIZE=200` OR `FLUSH_INTERVAL=15s`
  - At-least-once delivery: manual commit sau mỗi flush
  - Schema alignment với `schema_to_pyarrow()` để tránh type mismatch
- **Topics:** `olist.orders`, `olist.reviews`, `olist.payments`

**Data enrichment tại producer:** join customers + payments vào order event trước khi gửi Kafka → consumer và dashboard không cần lookup.

---

## 6. Orchestration — Prefect

- **Server:** Prefect 3 + PostgreSQL backend
- **UI:** http://localhost:4200 (hoặc Cloudflare Tunnel)
- **Flows:**
  - `bronze_ingestion_flow` → `silver_transform_flow` → `gold_transform_flow`
  - `full_pipeline_flow`: chuỗi 3 flows với dependency
- **Retry:** `@task(retries=3)` cho idempotency

---

## 7. Query Federation — Trino

- **UI:** http://localhost:8082
- **Catalog:** `iceberg` → query trực tiếp Bronze/Silver/Gold bằng SQL
- **Dùng được để:** kiểm tra data quality, ad-hoc analytics, so sánh layers

```sql
-- Ví dụ: so sánh row count qua các layers
SELECT 'bronze' AS layer, COUNT(*) FROM iceberg.bronze.ecommerce_orders
UNION ALL SELECT 'silver', COUNT(*) FROM iceberg.silver.stg_orders
UNION ALL SELECT 'gold',   COUNT(*) FROM iceberg.gold.fct_orders;
```

---

## 8. Agentic BI

- **File:** `bi/agentic_bi.py`
- **URL:** http://localhost:8501
- **Model:** LLaMA 3.3 70B Instruct qua OpenRouter (free tier)
- **Flow:**
  1. Load Gold tables từ Iceberg → pandas → DuckDB in-memory views
  2. Build schema context với column descriptions
  3. Claude API: câu hỏi tự nhiên → JSON `{sql, explanation, chart_type}`
  4. DuckDB thực thi SQL, self-correction loop (tối đa 3 lần nếu lỗi)
  5. Hiển thị: explanation + SQL expander + DataFrame + Plotly chart

- **Câu hỏi demo:**
  - "Doanh thu theo tháng năm 2018?"
  - "Top 10 bang có nhiều đơn hàng nhất?"
  - "Phân tích churn: bao nhiêu % khách hàng churned?"
  - "Seller nào có review tốt nhất với hơn 100 đơn?"

---

## 9. Realtime Dashboard

- **File:** `bi/pages/1_Realtime_Monitor.py`
- **URL:** http://localhost:8501 → sidebar "Realtime Monitor"
- **Charts:** Revenue by Payment Type · Order Status · Top 10 States · Cumulative Revenue
- **State persistence:** `st.cache_resource` → buffer tồn tại qua page reload
- **Auto-refresh:** mỗi 3 giây

---

## 10. Kế Hoạch Tiếp Theo — ML Models

| Model | Features | Target | Serving |
|-------|----------|--------|---------|
| Delivery delay prediction | seller_state, customer_state, weight, freight | delivery_delay_days | Real-time (streaming consumer) |
| Customer churn | days_since_last_order, total_orders, avg_order_value | is_churned | Batch (Agentic BI) |
| Lead conversion scoring | origin, business_segment, lead_type | is_converted | Batch (Agentic BI) |

**Hướng triển khai:**
- Train trên Gold layer → log experiments với MLflow
- Serve qua `mlflow models serve` REST API
- Streaming consumer gọi inference → gắn `predicted_delay` vào order event
- Dashboard hiện prediction real-time bên cạnh đơn hàng mới

---

## 11. Hạ Tầng — Docker Compose Services

| Service | Container | Port | Trạng thái |
|---------|-----------|------|-----------|
| PostgreSQL | olist-postgres | 5432 | ✅ Running |
| Iceberg REST | olist-iceberg-rest | 8181 | ✅ Running |
| Redpanda | olist-redpanda | 9092/19092 | ✅ Running |
| Redpanda Console | olist-redpanda-console | 8080 | ✅ Running |
| Prefect Server | olist-prefect-server | 4200 | ✅ Running |
| Prefect Worker | olist-prefect-worker | — | ✅ Running |
| MLflow | olist-mlflow | 5000 | ✅ Running |
| Trino | olist-trino | 8082 | ✅ Running |
| Streamlit | olist-streamlit | 8501 | ✅ Running |

---

## 12. Giới Hạn Hiện Tại

| Hạn chế | Lý do | Hướng mở rộng |
|---------|-------|---------------|
| Spark chạy `local[2]`, không phân tán thật | Dataset nhỏ (~200MB), single node đủ | Spark cluster khi scale TB+ |
| Streaming replay data lịch sử | Không có live transaction system | Sinh synthetic data theo phân phối thật |
| Không có schema validation ở Bronze | Chấp nhận raw data as-is | Thêm Great Expectations hoặc Soda Core |
| LLM qua external API | Phụ thuộc internet + rate limit | Self-hosted Ollama hoặc fine-tuned model |

---

## 13. Cấu Trúc Thư Mục

```
olist-data-engineering-project/
├── scripts/               # Batch ingest Bronze
├── spark/jobs/            # Silver + Gold PySpark transforms
├── streaming/             # Redpanda producer + consumer
├── prefect/flows/         # Orchestration flows
├── bi/                    # Streamlit apps (Agentic BI + Dashboard)
├── trino/                 # Trino catalog configs
├── infra/postgres/        # DB init SQL
├── data/raw/              # CSV source files (gitignored)
├── docs/                  # Báo cáo + diagrams
├── docker-compose.yml
└── .env
```
