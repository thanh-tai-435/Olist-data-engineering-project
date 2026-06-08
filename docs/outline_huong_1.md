# **OUTLINE 1: HƯỚNG DATA ENGINEERING / LAKEHOUSE**

**XÂY DỰNG NỀN TẢNG UNIFIED LAKEHOUSE CHO PHÂN TÍCH DỮ LIỆU THƯƠNG MẠI ĐIỆN TỬ**

# **CHƯƠNG 1. GIỚI THIỆU ĐỀ TÀI**

## **1.1 Bối cảnh và lý do chọn đề tài**

Trình bày sự bùng nổ dữ liệu thương mại điện tử và những thách thức của hạ tầng dữ liệu truyền thống: thiếu hỗ trợ streaming, khó scale, không có ACID, schema cứng nhắc. Nêu xu hướng chuyển dịch sang kiến trúc Lakehouse hiện đại.

## **1.2 Bài toán nghiên cứu**

Doanh nghiệp thương mại điện tử cần một hệ thống dữ liệu đáp ứng đồng thời:

- ingest dữ liệu từ nhiều nguồn (batch CSV và sự kiện realtime),
- đảm bảo tính nhất quán và chất lượng dữ liệu (ACID, schema validation),
- phục vụ analytical query nhanh mà không cần Data Warehouse riêng,
- tự động hoá pipeline end-to-end, có khả năng monitor và retry.

Đề tài xây dựng một nền tảng giải quyết bài toán này trên dataset Olist (~100K đơn hàng, ~200MB) theo kiến trúc production-grade.

## **1.3 Mục tiêu đề tài**

### **Mục tiêu chức năng**

- xây dựng Unified Lakehouse với Medallion Architecture (Bronze → Silver → Gold),
- triển khai pipeline batch và streaming song song (Lambda Architecture),
- tự động hoá toàn bộ pipeline bằng workflow orchestration,
- đảm bảo chất lượng dữ liệu tại từng tầng,
- xây dựng analytics dashboard từ Gold Layer.

### **Mục tiêu kỹ thuật**

- sử dụng Apache Iceberg (ACID, Time Travel, Schema Evolution) trên Cloudflare R2,
- triển khai ELT pipeline với dbt (Bronze → Silver → Gold),
- xử lý streaming với Redpanda và PyIceberg consumer,
- sử dụng DuckDB làm analytical query engine (embedded, không container),
- orchestrate toàn bộ pipeline với Prefect,
- kiểm tra chất lượng dữ liệu với Soda Core.

## **1.4 Dataset và phạm vi nghiên cứu**

### **Dataset**

- **Olist E-Commerce Dataset**: ~100K đơn hàng, 8 bảng (orders, order_items, products, sellers, customers, reviews, payments, geolocation).
- **Marketing Funnel Dataset**: leads và closed deals, 2 bảng.

### **Phạm vi công nghệ**

| Layer | Công nghệ |
|---|---|
| Storage | Cloudflare R2 (S3-compatible), Apache Iceberg |
| Ingestion | Python/pandas (batch), Redpanda + confluent-kafka (streaming), PyIceberg |
| Transformation | dbt (ELT), DuckDB (query engine) |
| Quality | Soda Core |
| Orchestration | Prefect |
| Serving | Streamlit |
| DevOps | Docker Compose |

### **Ngoài phạm vi**

- Distributed processing (Apache Spark) — kiến trúc sẵn sàng mở rộng nhưng không triển khai.
- Query federation (Trino) — để lại hướng phát triển.
- Machine Learning (MLflow) — hướng phát triển riêng (Hướng 2).

## **1.5 Cấu trúc báo cáo**

Giới thiệu nội dung 6 chương và luồng triển khai hệ thống từ hạ tầng đến serving layer.

---

# **CHƯƠNG 2. CƠ SỞ LÝ THUYẾT**

## **2.1 Kiến trúc dữ liệu hiện đại**

### **Data Warehouse, Data Lake và Unified Lakehouse**

So sánh 3 mô hình: Data Warehouse (schema cứng, OLAP nhanh), Data Lake (schema-on-read, lưu trữ rẻ), Lakehouse (kết hợp ACID + open format + schema enforcement).

### **Lambda Architecture**

Giới thiệu mô hình Lambda: Batch Layer (xử lý historical), Speed Layer (xử lý realtime), Serving Layer (merge kết quả). Giải thích tại sao Lambda phù hợp với bài toán e-commerce có cả lịch sử đơn hàng và đơn hàng mới liên tục.

## **2.2 Medallion Architecture**

Giới thiệu mô hình Bronze – Silver – Gold:

- **Bronze**: raw data, append-only, immutable — nguồn sự thật gốc.
- **Silver**: cleaned, typed, deduplicated — dữ liệu đã kiểm định.
- **Gold**: aggregated, business-ready — phục vụ BI và analytics.

Giải thích lợi ích: dễ debug (quay về Bronze), incremental processing (chỉ transform delta), tách biệt trách nhiệm giữa các team.

## **2.3 Apache Iceberg và PyIceberg**

Trình bày các tính năng chính của Apache Iceberg:

- **ACID transactions**: concurrent batch + streaming write không conflict.
- **Schema Evolution**: thêm/đổi tên column không phá vỡ downstream.
- **Time Travel**: truy vấn dữ liệu tại thời điểm bất kỳ bằng snapshot ID.
- **Hidden Partitioning**: partition logic trong metadata, không lộ ra query.

Giới thiệu PyIceberg: Python client để tạo bảng, append data, quản lý schema mà không cần Spark/Java.

## **2.4 Streaming Data Pipeline**

Giới thiệu khái niệm event streaming và message queue. Trình bày Redpanda:

- Kafka-compatible API, single container (không cần JVM/Zookeeper).
- Kiến trúc Producer → Topic → Consumer Group.
- Tại sao chọn Redpanda thay Kafka: lightweight (~500MB RAM vs ~2GB), phù hợp local/Codespaces.

Mô tả luồng: Producer đọc CSV, replay theo timestamp gốc → Redpanda topics → Consumer append vào Bronze Iceberg.

## **2.5 ELT Pipeline với dbt**

Giới thiệu mô hình ELT (Extract – Load – Transform) so với ETL truyền thống. Trình bày dbt:

- SQL-based transformation engine, chạy trên DuckDB (hoặc Spark).
- Incremental models: `append` cho Bronze, `merge` cho Silver, `table` cho Gold.
- Built-in testing: `unique`, `not_null`, `relationships`, `accepted_values`.
- Lineage graph tự động.

## **2.6 Analytical Query Engine: DuckDB**

Trình bày DuckDB:

- Embedded OLAP engine (không cần server, không cần container).
- Đọc trực tiếp Parquet/Iceberg files từ R2 qua S3 API.
- Tốc độ analytical query: columnar storage, vectorized execution.
- Phù hợp workload <1TB — lý tưởng cho project này.

## **2.7 Workflow Orchestration với Prefect**

Giới thiệu khái niệm workflow orchestration: dependency management, retry, monitoring, scheduling. Trình bày Prefect:

- `@flow` và `@task` decorators — Python-native, không cần YAML DAG.
- Prefect Cloud free tier: UI monitoring, run history, alerting.
- `task.submit()` cho parallel execution.
- So sánh với Airflow: Prefect ít boilerplate hơn, dễ test locally.

## **2.8 Data Quality với Soda Core**

Trình bày tầm quan trọng của data quality trong pipeline. Giới thiệu Soda Core:

- YAML-based checks: `missing_count`, `duplicate_count`, `min`, `max`, `row_count`.
- Chạy sau mỗi ingestion để validate trước khi promote lên Silver.
- Tích hợp với Prefect: task fail nếu quality check không pass.

---

# **CHƯƠNG 3. PHÂN TÍCH VÀ THIẾT KẾ HỆ THỐNG**

## **3.1 Phân tích yêu cầu hệ thống**

### **Functional Requirements**

- FR1: Ingest toàn bộ 10 bảng Olist từ CSV vào Bronze Layer (batch).
- FR2: Replay lịch sử đơn hàng qua Redpanda, consumer ghi vào Bronze (streaming).
- FR3: Transform Bronze → Silver → Gold tự động bằng dbt.
- FR4: Validate chất lượng dữ liệu tại Bronze sau mỗi lần ingest.
- FR5: Orchestrate toàn bộ pipeline, hiển thị trạng thái trên UI.
- FR6: Dashboard analytics từ Gold Layer (doanh thu, seller, customer).

### **Non-functional Requirements**

- NFR1: Bronze Layer phải append-only, không bao giờ update/delete.
- NFR2: Pipeline phải idempotent — chạy lại không tạo duplicate.
- NFR3: Mỗi task có retry tối thiểu 3 lần với exponential backoff.
- NFR4: Toàn bộ hệ thống chạy trong Docker Compose với `docker compose up`.
- NFR5: Analytical query trên Gold Layer response < 5 giây cho dataset ~200MB.

## **3.2 Thiết kế kiến trúc tổng thể**

Mô tả luồng dữ liệu end-to-end:

```
[CSV Files] ──(batch)──────────────────────────────────────────┐
                                                                ▼
[CSV Replay] ──(stream)──► [Redpanda] ──► [Consumer] ──► [Bronze / Iceberg on R2]
                                                                │
                                                    [Soda Core Quality Check]
                                                                │
                                                         [dbt Silver]
                                                                │
                                                         [dbt Gold]
                                                                │
                                            [DuckDB Query] ──► [Streamlit Dashboard]

[Prefect Orchestrates toàn bộ luồng trên]
```

Giải thích vai trò từng component và tại sao chọn công nghệ tương ứng.

## **3.3 Thiết kế dữ liệu**

### **ERD — Nguồn (Olist Raw)**

Thiết kế quan hệ giữa 10 bảng nguồn: `orders` là bảng trung tâm, quan hệ với `order_items`, `payments`, `reviews`, `customers`; `order_items` quan hệ với `products` và `sellers`.

### **Star Schema — Gold Layer**

- **Fact Tables**: `fct_orders` (grain: order), `fct_funnel` (grain: lead).
- **Dimension Tables**: `dim_sellers`, `dim_customers`, `dim_products`.
- Partition key: `order_date` (monthly) cho `fct_orders`.

## **3.4 Thiết kế Medallion Pipeline**

### **Batch Pipeline**

```
CSV (Kaggle) → upload_raw_to_r2.py → R2/raw/ → batch_ingest_bronze.py → Bronze Iceberg
```

Mỗi bảng là một Iceberg table riêng. Ingest dùng PyIceberg `table.append(df)`.

### **Streaming Pipeline**

```
streaming_producer.py (replay CSV theo timestamp) → Redpanda topic olist.orders
→ stream_consumer.py (consumer group: iceberg-bronze-writer) → Bronze Iceberg
```

### **Transformation Pipeline**

```
Bronze → dbt stg_* (Silver: clean, type cast, dedup) → dbt int_* (enrich, join) → dbt fct_*/dim_* (Gold: aggregate, partition)
```

Incremental strategy: Silver dùng `merge` trên `unique_key`, Gold dùng `table` với partition.

## **3.5 Thiết kế Orchestration (Prefect)**

Thiết kế 3 flows:

- `bronze_ingestion_flow`: ingest tất cả bảng song song (`task.submit()`), sau đó chạy Soda Core checks.
- `dbt_transform_flow`: chạy dbt Silver, sau đó Gold tuần tự.
- `full_pipeline_flow`: gọi hai flow trên với dependency rõ ràng.

Deployment: schedule chạy daily, alerting qua Prefect Cloud khi flow fail.

## **3.6 Thiết kế Dashboard Analytics**

Mô tả 3 nhóm metrics hiển thị trên Streamlit từ Gold Layer:

- **Revenue Analytics**: doanh thu theo tháng, theo category, theo state.
- **Seller Performance**: top sellers, delivery SLA compliance.
- **Customer Analytics**: retention, repeat order rate, geographic distribution.

---

# **CHƯƠNG 4. XÂY DỰNG VÀ TRIỂN KHAI HỆ THỐNG**

## **4.1 Môi trường triển khai**

Triển khai bằng Docker Compose gồm các services:

| Service | Role | Port |
|---|---|---|
| iceberg-rest | Iceberg REST Catalog | 8181 |
| redpanda | Kafka-compatible broker | 9092 |
| redpanda-console | Redpanda UI | 8080 |
| prefect-server | Orchestration server | 4200 |
| prefect-worker | Flow executor | — |
| postgres | Backend cho Prefect | 5432 |

> **Lưu ý**: DuckDB là embedded library, không cần container. Chạy in-process trong dbt và analytics scripts.

Storage backend: Cloudflare R2 (S3-compatible), cấu hình qua `.env` với `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_ENDPOINT`.

## **4.2 Khởi tạo Bronze Tables (iceberg_setup.py)**

Dùng PyIceberg để tạo Iceberg tables trong REST Catalog với schema cố định cho từng bảng. Cấu hình:

- namespace: `bronze.ecom` và `bronze.marketing`
- location: `s3://olist-lakehouse/bronze/{table}/`
- properties: append-only, format-version 2

## **4.3 Xây dựng Ingestion Layer**

### **Batch Ingestion (batch_ingest_bronze.py)**

Đọc CSV từ R2 bằng pandas, validate schema, ghi vào Bronze bằng `table.append(df)`. Mỗi bảng là một task Prefect độc lập, chạy song song.

### **Streaming Ingestion**

- **Producer** (`streaming_producer.py`): đọc `olist_orders_dataset.csv`, sort theo `order_purchase_timestamp`, replay với speed factor (1 ngày = 1 giây), publish lên Redpanda topic `olist.orders`.
- **Consumer** (`stream_consumer.py`): consumer group `iceberg-bronze-writer`, batch 100 messages, append vào Bronze Iceberg mỗi batch.

### **Data Quality (Soda Core)**

`soda_checks.yml` định nghĩa checks cho từng bảng Bronze:

- `missing_count(order_id) = 0`
- `duplicate_count(order_id) = 0`
- `row_count > 0`
- range checks cho numeric columns

Chạy sau mỗi batch ingest, fail Prefect task nếu có vi phạm.

## **4.4 Xây dựng Medallion Transformation (dbt)**

### **Silver Layer**

- `stg_orders.sql`: cast types, handle null, rename columns chuẩn.
- `stg_sellers.sql`: normalize địa chỉ, deduplicate.
- `stg_funnel.sql`: parse dates, validate lead stages.
- `int_orders_enriched.sql`: join orders + items + payments + customers.
- Strategy: `incremental`, `merge`, `unique_key = order_id`.

### **Gold Layer**

- `fct_orders.sql`: aggregate revenue, item count, delivery days per order. Partition by `order_date`.
- `dim_sellers.sql`: seller profile + performance summary.
- `dim_customers.sql`: customer lifetime metrics.
- `fct_funnel.sql`: lead-to-deal conversion metrics.

### **dbt Tests**

Mỗi model có `schema.yml` với tests: `unique`, `not_null`, `relationships` cho foreign keys, `accepted_values` cho status columns.

## **4.5 Xây dựng Orchestration Layer (Prefect)**

Triển khai 3 flows (`prefect/flows/`):

- `bronze_ingestion.py`: task per table, submit parallel, sau đó run Soda checks.
- `dbt_transforms.py`: subprocess `dbt run --select silver.*` → `dbt run --select gold.*`.
- `full_pipeline.py`: orchestrate cả 2 flows, hiển thị trên Prefect Cloud UI.

Cấu hình deployment: schedule cron daily 02:00 UTC, retries=3, timeout=1800s.

## **4.6 Xây dựng Analytics Dashboard (Streamlit)**

Kết nối DuckDB với Gold Layer trên R2 qua `duckdb.read_parquet('s3://...')`. Dashboard gồm 3 tabs:

- **Revenue**: line chart doanh thu theo tháng, bar chart theo category/state.
- **Sellers**: bảng top 10 sellers, scatter plot review score vs. delivery time.
- **Customers**: map phân bố địa lý, pie chart retention.

---

# **CHƯƠNG 5. ĐÁNH GIÁ HỆ THỐNG**

## **5.1 Kết quả đạt được**

Liệt kê theo từng thành phần:

- Bronze Layer nhận dữ liệu từ cả batch (10 bảng × N rows) và streaming (replay 100K events).
- Silver/Gold: N dbt models pass tests, lineage graph đầy đủ.
- Prefect: full pipeline chạy end-to-end, run history hiển thị trên Cloud UI.
- Soda Core: quality checks pass trên tất cả Bronze tables.
- Dashboard: 3 nhóm metrics render từ Gold Layer qua DuckDB.

## **5.2 Đánh giá hiệu năng**

### **Query Performance (DuckDB trên Gold Layer)**

Benchmark 3 loại query (simple filter, multi-table join, window function) trên dataset ~200MB. Ghi nhận response time và so sánh với DuckDB đọc raw Parquet vs. Iceberg.

### **Streaming Throughput**

Đo messages/giây từ Producer → Redpanda → Consumer → Bronze. Ghi nhận end-to-end latency và batch write performance của PyIceberg.

### **Pipeline Reliability**

Đánh giá retry mechanism: inject lỗi nhân tạo (network timeout, schema mismatch), quan sát Prefect retry và alerting.

## **5.3 So sánh với kiến trúc truyền thống**

So sánh Lakehouse (Iceberg + dbt) với Data Warehouse truyền thống (PostgreSQL + ETL):

| Tiêu chí | Traditional DWH | Lakehouse (đề tài) |
|---|---|---|
| Schema thay đổi | Cần migration script | Schema Evolution tự động |
| Streaming support | Khó tích hợp | Native (Iceberg ACID) |
| Time travel | Không | Snapshot-based |
| Storage cost | Database storage | Object storage (R2) |
| Scale | Vertical | Horizontal (tách storage/compute) |

## **5.4 Hạn chế hệ thống**

- Triển khai single-node local, chưa test distributed (Spark cluster).
- Redpanda topic retention mặc định — mất data khi container restart nếu không mount volume.
- DuckDB không hỗ trợ concurrent write — chỉ phù hợp single-writer analytics.
- Chưa có data governance (column masking, row-level security, audit log).
- Chưa có CI/CD tự động chạy dbt test trên mỗi PR.

---

# **CHƯƠNG 6. KẾT LUẬN VÀ HƯỚNG PHÁT TRIỂN**

## **6.1 Kết luận**

Tóm tắt kết quả: đã xây dựng thành công nền tảng Unified Lakehouse cho dữ liệu thương mại điện tử Olist với đầy đủ batch + streaming ingestion, Medallion transformation bằng dbt, orchestration tự động bằng Prefect, và analytics dashboard bằng Streamlit. Hệ thống chạy hoàn toàn trong Docker Compose, phù hợp triển khai local và Codespaces.

## **6.2 Hướng phát triển**

- **Distributed processing**: tích hợp Apache Spark cho dataset >1TB (Iceberg tables đã tương thích).
- **Query federation**: triển khai Trino để join Iceberg + Postgres + CSV trong một query.
- **Machine Learning**: tích hợp MLflow cho delivery delay prediction và churn prediction (Hướng 2).
- **Agentic BI**: Streamlit + Claude API cho natural language → SQL query.
- **Cloud-native deployment**: migrate từ Docker Compose sang Kubernetes (Helm charts).
- **Data Governance**: Apache Atlas hoặc OpenMetadata cho data lineage, column-level security.
- **CI/CD**: GitHub Actions pipeline chạy `dbt test` + Soda Core checks trên mỗi PR.
