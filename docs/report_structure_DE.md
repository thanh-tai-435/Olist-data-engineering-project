# Cấu Trúc Báo Cáo — Phiên Bản Data Engineering
**Chủ đề:** Xây dựng Hệ thống Data Lakehouse với Kiến trúc Medallion và Xử lý Luồng Thời gian Thực  
**Người nộp:** [Tên bạn]  
**Trọng tâm:** Kiến trúc dữ liệu, mô hình dữ liệu đa tầng, pipeline streaming

---

## CHƯƠNG 1 — GIỚI THIỆU *(~4 trang)*

### 1.1 Bối cảnh và Động lực Nghiên cứu
- Sự bùng nổ dữ liệu trong thương mại điện tử và nhu cầu xử lý thời gian thực
- Hạn chế của các kiến trúc truyền thống (Data Warehouse, Data Lake đơn thuần)
- Xu hướng Data Lakehouse trong ngành công nghiệp

### 1.2 Mục tiêu Đề tài
- Thiết kế và triển khai hệ thống Lakehouse hoàn chỉnh trên cloud storage
- Xây dựng pipeline batch và streaming tích hợp
- Cung cấp dữ liệu chất lượng cao cho các hệ thống downstream (BI, ML)

### 1.3 Phạm vi và Giới hạn
- Dataset sử dụng: Olist Brazilian E-Commerce (~100K đơn hàng, 10 bảng)
- Môi trường: Docker Compose, Cloudflare R2, Apache Iceberg
- Không bao gồm: production deployment, security hardening

### 1.4 Cấu trúc Báo cáo

---

## CHƯƠNG 2 — CƠ SỞ LÝ THUYẾT *(~8 trang)*

### 2.1 Kiến trúc Data Lakehouse
- Từ Data Warehouse → Data Lake → Data Lakehouse
- Mô hình tham chiếu: Armbrust et al. (2021)
- So sánh với các kiến trúc tiền nhiệm

### 2.2 Kiến trúc Medallion (Multi-hop Architecture)
- Nguồn gốc và nguyên lý thiết kế
- Ba tầng: Bronze / Silver / Gold — vai trò và cam kết chất lượng
- Chiến lược ghi: append-only, incremental merge, full refresh

### 2.3 Apache Iceberg — Định dạng Bảng Mở
- Giải quyết hạn chế của Hive table format
- ACID transactions trên object storage
- Hidden partitioning, schema evolution, time travel
- Kiến trúc metadata: snapshot → manifest list → manifest → data files

### 2.4 Mô hình Dữ liệu Chiều (Dimensional Modeling)
- Star schema vs Snowflake schema
- Fact tables và Dimension tables — nguyên lý Kimball
- Slowly Changing Dimensions (SCD)
- Ứng dụng trong tầng Gold

### 2.5 Kiến trúc Lambda — Batch và Streaming
- Batch layer, Speed layer, Serving layer
- Ưu nhược điểm so với Kappa Architecture
- At-least-once vs Exactly-once delivery semantics

### 2.6 Apache Spark cho Xử lý Dữ liệu Phân tán
- RDD, DataFrame, Dataset API
- Catalyst optimizer và Tungsten execution engine
- Spark + Iceberg integration

---

## CHƯƠNG 3 — PHÂN TÍCH YÊU CẦU VÀ THIẾT KẾ HỆ THỐNG *(~6 trang)*

### 3.1 Giới thiệu Dataset Olist
- Nguồn: Kaggle — Brazilian E-Commerce Public Dataset
- Mô tả 10 bảng: orders, items, payments, reviews, products, sellers, customers, marketing
- Thống kê: ~1.56M records, phân bố thời gian 2016–2018
- **Hình:** Entity Relationship Diagram (ERD) của dataset gốc

### 3.2 Yêu cầu Chức năng
- Ingest batch từ CSV vào Bronze layer (idempotent)
- Giả lập streaming events qua Kafka-compatible broker
- Transform Bronze → Silver → Gold với PySpark
- Cung cấp dữ liệu cho dashboard real-time và BI

### 3.3 Yêu cầu Phi chức năng
- Idempotency: chạy lại pipeline không tạo duplicate
- Schema evolution: thêm cột không phá vỡ downstream
- Observability: toàn bộ pipeline quan sát được qua Prefect UI

### 3.4 Kiến trúc Tổng thể Hệ thống
- **Hình:** Sơ đồ kiến trúc tổng thể (từ file drawio)
- Phân lớp: Storage / Processing / Orchestration / Serving
- Lý do lựa chọn từng công nghệ

### 3.5 Thiết kế Hạ tầng Docker Compose
- Danh sách services và vai trò từng service
- Network isolation và healthcheck strategy
- Volume management và data persistence

---

## CHƯƠNG 4 — TRIỂN KHAI PIPELINE MEDALLION *(~14 trang — TRỌNG TÂM)*

### 4.1 Tầng Bronze — Thu nạp Dữ liệu Thô

#### 4.1.1 Thiết kế Schema Bronze
- Nguyên tắc: giữ nguyên cấu trúc nguồn, không transform
- Metadata columns: `_ingested_at`, `_source_file`, `_source_path`
- Tạo namespace và bảng Iceberg với PyIceberg

#### 4.1.2 Batch Ingestion Pipeline
- Đọc CSV từ Cloudflare R2 raw/ bằng boto3
- Cast timestamp sang `datetime64[us]` (PyArrow compatibility)
- Append vào Iceberg: `table.append(arrow_table)`
- Xử lý idempotency: kiểm tra tồn tại bảng trước khi tạo

#### 4.1.3 Kết quả Bronze Layer
- **Bảng:** 9 bảng × số lượng rows
- Snapshot metadata trên R2
- **Hình:** Cấu trúc thư mục R2 (data/ + metadata/)

### 4.2 Tầng Silver — Làm sạch và Chuẩn hóa

#### 4.2.1 Kiến trúc Staging Tables
- Nguyên tắc source-conforming: 1 staging table per source
- PySpark SparkSession configuration (Iceberg REST catalog, S3FileIO, JAR dependencies)

#### 4.2.2 Các Phép Biến đổi theo Bảng
- `stg_orders`: cast timestamp, tính delivery days, dedup
- `stg_order_payments`: aggregate 1 row/order, dominant payment type
- `stg_order_reviews`: dedup lấy review mới nhất (Window function)
- `stg_sellers / stg_customers`: normalize text (lower/trim/upper)
- **Bảng tóm tắt:** Transform logic × bảng

#### 4.2.3 Intermediate Table: `int_orders_enriched`
- Mục đích: pre-join để Gold layer tái sử dụng
- Join logic: orders ⋈ customers ⋈ items_agg ⋈ payments ⋈ reviews
- Tính `delivery_delay_days = actual − estimated`
- **Hình:** Lineage graph của int_orders_enriched

#### 4.2.4 Chiến lược Ghi và Idempotency
- `createOrReplace()`: full refresh, đảm bảo deterministic
- Trade-off với incremental merge (dbt)

### 4.3 Tầng Gold — Tổng hợp và Mô hình Chiều *(TRỌNG TÂM)*

#### 4.3.1 Mô hình Dữ liệu Gold Layer
- **Hình:** Star schema — fct_orders ↔ dim_sellers, dim_customers
- Thiết kế theo Kimball dimensional modeling

#### 4.3.2 Fact Table: `fct_orders`
- Columns và logic tính toán
- `delivery_status` label: early / on_time / late / unknown
- Iceberg hidden partitioning: `months(purchased_at)`
- **Hình:** Partition pruning — query plan trước/sau partition

#### 4.3.3 Fact Table: `fct_funnel`
- Marketing funnel từ MQL → deal
- `days_to_close`, `is_converted` features
- Partition: `years(first_contact_date)`

#### 4.3.4 Dimension Table: `dim_sellers`
- Profile + aggregated performance metrics
- `total_revenue`, `avg_review_score`, `delivered_orders`

#### 4.3.5 Dimension Table: `dim_customers`
- Dedup logic: customer_id vs customer_unique_id (đặc thù Olist)
- CLV features: `total_spend`, `avg_order_value`
- Churn prediction features: `is_churned` (>90 ngày), `is_repeat_customer`
- **Hình:** Customer dedup logic (Window function diagram)

#### 4.3.6 Tổng kết Gold Layer
- **Bảng:** 4 bảng × rows × partition strategy

---

## CHƯƠNG 5 — HỆ THỐNG XỬ LÝ STREAMING THỜI GIAN THỰC *(~10 trang)*

### 5.1 Kiến trúc Lambda trong Hệ thống
- Vị trí streaming trong tổng thể kiến trúc
- Lý do chọn Redpanda thay Kafka

### 5.2 Redpanda: Message Broker Kafka-compatible
- Kiến trúc: C++ engine, không JVM/Zookeeper
- So sánh hiệu năng với Apache Kafka
- Cấu hình topics: partitions, retention, replication
- **Hình:** Redpanda Console — topic overview

### 5.3 Streaming Producer — Giả lập Dữ liệu Thời gian Thực
- Hai chế độ: `rate` (N events/s) và `replay` (time-compressed)
- Data enrichment tại nguồn: join customers + payments vào order event
- **Rate mode**: shuffle dataset, vô hạn, phù hợp demo
- **Replay mode**: phương trình nén thời gian `sleep = data_elapsed / SPEED_FACTOR − wall_elapsed`
- **Bảng:** So sánh rate mode vs replay mode

### 5.4 Streaming Consumer — Micro-batch vào Bronze Iceberg
- Dual-trigger flush: BATCH_SIZE (200) và FLUSH_INTERVAL (15s)
- Schema alignment: `schema_to_pyarrow()` để tránh type mismatch
- At-least-once delivery: manual commit sau mỗi flush
- **Hình:** Dual-trigger decision flowchart

### 5.5 Tích hợp với Bronze Iceberg Layer
- Consumer append vào cùng Bronze tables với batch ingestion
- Silver layer xử lý duplicate từ at-least-once với `dropDuplicates()`
- **Hình:** Lambda Architecture — batch path vs speed path

### 5.6 Real-time Monitoring Dashboard
- Streamlit + `st.cache_resource` pattern
- Consumer `auto.offset.reset=earliest`: mở lại UI không mất data
- Charts: Revenue by Payment Type, Order Status, Top States, Cumulative Revenue
- **Hình:** Screenshot dashboard

---

## CHƯƠNG 6 — ĐIỀU PHỐI PIPELINE VỚI PREFECT *(~5 trang)*

### 6.1 Giới thiệu Prefect
- So sánh Prefect với Apache Airflow, Dagster
- `@flow` và `@task` decorators

### 6.2 Cấu trúc Flows
- `bronze_ingestion_flow` → `silver_transform_flow` → `gold_transform_flow`
- `full_pipeline_flow`: chuỗi 3 flows với dependency
- Retry strategy: `@task(retries=3)`

### 6.3 Observability
- **Hình:** Prefect UI — flow run timeline
- Task states: Pending / Running / Completed / Failed
- Logs và artifacts

---

## CHƯƠNG 7 — AGENTIC BI (TỔNG QUAN) *(~3 trang)*

### 7.1 Định nghĩa và Vị trí trong Hệ thống
- Agentic BI là gì và tại sao cần thiết
- Kết nối với Gold layer (input của Agentic BI)

### 7.2 Kiến trúc Tổng quan
- Claude API + DuckDB/Trino + Streamlit
- Flow: câu hỏi tự nhiên → SQL → kết quả → giải thích
- *Chi tiết triển khai: xem báo cáo Agentic BI*

---

## CHƯƠNG 8 — ĐÁNH GIÁ VÀ KẾT QUẢ *(~5 trang)*

### 8.1 Kết quả Triển khai
- **Bảng tổng hợp:** Tất cả bảng × rows × layer × tool
- Thời gian chạy từng bước pipeline
- Dung lượng lưu trữ trên R2 (raw CSV vs Parquet Iceberg)

### 8.2 Kiểm tra Tính đúng đắn của Dữ liệu
- So sánh row count Bronze → Silver → Gold
- Verify dedup: `stg_customers` không có `customer_id` trùng
- Verify partition: query plan có sử dụng partition pruning

### 8.3 Đánh giá Hiệu năng Streaming
- Throughput: events/giây producer vs consumer
- Consumer lag trên Redpanda Console
- Latency: thời gian từ produce đến xuất hiện trong dashboard

### 8.4 Hạn chế và Hướng Phát triển
- Single-node Spark (không phân tán thực sự)
- Không có schema validation ở tầng Bronze
- Hướng mở rộng: Apache Flink cho stateful streaming

---

## CHƯƠNG 9 — KẾT LUẬN *(~2 trang)*

### 9.1 Tóm tắt Đóng góp
### 9.2 Bài học Kinh nghiệm
### 9.3 Hướng Nghiên cứu Tiếp theo

---

## TÀI LIỆU THAM KHẢO

*(Tối thiểu 20 nguồn — ưu tiên: VLDB, SIGMOD, CIDR, ACM, IEEE + sách O'Reilly/Manning)*

Bao gồm: Armbrust et al. (2021), Armbrust et al. (2020) Delta Lake, Zaharia et al. (2016) Spark,
Kimball & Ross (2013), Kleppmann (2017), Reis & Housley (2022), Marz & Warren (2015),
Narkhede et al. (2017), Inmon (2005), Serafini et al. (2023) Redpanda, ...

---

## PHỤ LỤC

- **Phụ lục A:** Docker Compose services và cấu hình
- **Phụ lục B:** Danh sách JAR dependencies và lý do chọn
- **Phụ lục C:** Hướng dẫn triển khai (setup guide)
- **Phụ lục D:** Sơ đồ kiến trúc Medallion (draw.io export)

---

> **Ước tính tổng:** ~57 trang nội dung + phụ lục  
> **Hình/Bảng dự kiến:** ~22 hình, ~12 bảng
