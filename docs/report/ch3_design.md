# CHƯƠNG 3. PHÂN TÍCH VÀ THIẾT KẾ HỆ THỐNG

## 3.1 Phân tích yêu cầu hệ thống

### 3.1.1 Functional Requirements

**FR-01 — Batch Ingestion**
Hệ thống phải có khả năng đọc toàn bộ 10 bảng dữ liệu từ định dạng CSV (Olist E-Commerce + Marketing Funnel), upload lên Cloudflare R2, và ghi vào Bronze Layer dưới dạng Apache Iceberg tables với ACID guarantees.

**FR-02 — Streaming Ingestion**
Hệ thống phải có khả năng replay dữ liệu lịch sử đơn hàng theo timestamp gốc qua Redpanda, với một consumer tự động đọc messages và append vào Bronze Iceberg table tương ứng.

**FR-03 — Data Quality Validation**
Sau mỗi lần ingest (batch hoặc streaming batch), hệ thống phải tự động chạy các quality checks để phát hiện: null trong key columns, duplicate records, giá trị ngoài phạm vi, và freshness. Nếu check fail, pipeline dừng và không promote dữ liệu lên Silver.

**FR-04 — ELT Transformation**
Hệ thống phải tự động transform dữ liệu qua ba tầng Medallion:
- Bronze → Silver: clean, cast types, deduplicate, enrich bằng joins
- Silver → Gold: aggregate, tạo fact/dim tables theo Star Schema, partition by time

**FR-05 — Pipeline Orchestration**
Toàn bộ pipeline (ingest → quality check → transform silver → transform gold) phải được orchestrate tự động với dependency management, retry tự động khi fail, và monitoring qua UI.

**FR-06 — Analytics Dashboard**
Hệ thống phải cung cấp dashboard hiển thị ít nhất 3 nhóm metrics từ Gold Layer: doanh thu theo thời gian, hiệu suất người bán, và phân tích khách hàng.

**FR-07 — Containerized Deployment**
Toàn bộ hệ thống phải chạy trong Docker Compose với lệnh `docker compose up`, không cần cài đặt thủ công bên ngoài container.

### 3.1.2 Non-functional Requirements

**NFR-01 — Append-only Bronze**
Bronze Layer phải tuyệt đối append-only: không có UPDATE, DELETE hay MERGE operations trên Bronze tables. Tính bất biến của Bronze là điều kiện tiên quyết cho reproducibility.

**NFR-02 — Idempotency**
Toàn bộ pipeline phải idempotent: chạy lại cùng một pipeline (cùng input) không tạo ra duplicate records trong Silver hay Gold. Silver dùng `merge` strategy với `unique_key`, Gold dùng `insert_overwrite` partition.

**NFR-03 — Retry Mechanism**
Mỗi Prefect task phải có `retries=3` với `retry_delay_seconds=30`. Task fail sau 3 lần retry → flow fail → alerting.

**NFR-04 — Query Performance**
Analytical queries trên Gold Layer (~200MB dataset) phải trả kết quả trong vòng 5 giây trên một machine thông thường (4 CPU, 8GB RAM).

**NFR-05 — Schema Flexibility**
Hệ thống phải có khả năng thêm column mới vào Bronze schema mà không cần rewrite data files hay rebuild downstream models không liên quan (Iceberg Schema Evolution).

**NFR-06 — Observability**
Pipeline execution history, task-level logs và failure reasons phải hiển thị rõ trên Prefect Cloud UI, không cần SSH vào server để đọc logs.

---

## 3.2 Thiết kế kiến trúc tổng thể

### 3.2.1 Architecture Overview

Hệ thống được thiết kế theo Lambda Architecture với Medallion layering trên Iceberg. Luồng dữ liệu tổng quát:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│  [Olist CSV Files]           [CSV Replay (simulated events)]        │
└──────────┬──────────────────────────────┬───────────────────────────┘
           │ Batch                        │ Stream
           ▼                              ▼
┌──────────────────┐           ┌─────────────────────┐
│  upload_raw_r2   │           │ streaming_producer  │
│  (pandas → R2)   │           │ (confluent-kafka)   │
└────────┬─────────┘           └──────────┬──────────┘
         │                                │
         │                      ┌─────────▼──────────┐
         │                      │      REDPANDA       │
         │                      │  Topics:            │
         │                      │  - olist.orders     │
         │                      │  - olist.reviews    │
         │                      └──────────┬──────────┘
         │                                 │
         │                      ┌──────────▼──────────┐
         │                      │   stream_consumer   │
         │                      │   (batch 100 msgs)  │
         └─────────────┬─────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Iceberg on R2)                  │
│  s3://olist-lakehouse/bronze/ecom/{orders,items,reviews,...}     │
│  s3://olist-lakehouse/bronze/marketing/{leads,deals}            │
│  [Append-only | ACID | Schema Evolution | Time Travel]           │
└──────────────────────────┬───────────────────────────────────────┘
                           │
              [Soda Core Quality Checks]
              (fail → pipeline stops)
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (dbt + DuckDB)                    │
│  stg_orders, stg_sellers, stg_funnel                            │
│  int_orders_enriched, int_seller_performance                    │
│  [Incremental Merge | Cleaned | Typed | Deduplicated]           │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (dbt + DuckDB)                     │
│  fct_orders (partition: order_date)                             │
│  dim_sellers, dim_customers, fct_funnel                         │
│  [Aggregated | Partitioned | Business-Ready]                    │
└──────────────────────────┬───────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                    SERVING LAYER                                 │
│  DuckDB (embedded query) → Streamlit Dashboard                  │
└──────────────────────────────────────────────────────────────────┘

[PREFECT CLOUD: Orchestrates toàn bộ luồng trên]
```

### 3.2.2 Lý do chọn công nghệ

**Cloudflare R2 thay vì AWS S3**: Egress miễn phí (AWS S3 tính phí ~$0.09/GB egress), tương thích 100% S3 API nên toàn bộ code boto3/PyIceberg chạy không thay đổi.

**Redpanda thay vì Kafka**: Single container, không cần Zookeeper, RAM footprint ~500MB thay vì ~2GB — phù hợp Codespaces 4 CPU / 15GB RAM.

**DuckDB thay vì Spark**: Dataset ~200MB không cần distributed processing. DuckDB embedded, không cần container, query tốc độ cao trực tiếp trên Parquet files.

**Prefect thay vì Airflow**: Python-native decorators dễ viết và test; Prefect Cloud free tier không cần self-host scheduler; better local development experience.

---

## 3.3 Thiết kế dữ liệu

### 3.3.1 ERD — Nguồn (Olist Raw)

Quan hệ giữa các bảng nguồn:

```
olist_customers ──────────────────────────────┐
    customer_id (PK)                          │
                                              │
olist_orders ─────────────────────────────────┤
    order_id (PK)                             │
    customer_id (FK → customers)              │
    order_status                              │
    order_purchase_timestamp                  │
    order_delivered_customer_date             │
    order_estimated_delivery_date             │
         │
         ├──── olist_order_items
         │         order_id (FK)
         │         order_item_id
         │         product_id (FK → products)
         │         seller_id (FK → sellers)
         │         price
         │         freight_value
         │
         ├──── olist_order_payments
         │         order_id (FK)
         │         payment_type
         │         payment_value
         │
         └──── olist_order_reviews
                   review_id (PK)
                   order_id (FK)
                   review_score (1–5)
                   review_comment_message

olist_sellers
    seller_id (PK)
    seller_zip_code_prefix (FK → geolocation)
    seller_state

olist_products
    product_id (PK)
    product_category_name
    product_weight_g

olist_geolocation
    geolocation_zip_code_prefix (PK)
    geolocation_lat, geolocation_lng
    geolocation_city, geolocation_state

-- Marketing Funnel (dataset riêng, không join trực tiếp với orders)
olist_mql (qualified leads)
    mql_id (PK)
    first_contact_date
    landing_page_id
    origin (organic, paid, social...)

olist_closed_deals
    mql_id (FK → mql)
    seller_id (FK → sellers)
    won_date
    business_segment
    lead_type
```

### 3.3.2 Star Schema — Gold Layer

Gold Layer được thiết kế theo Star Schema tối ưu cho BI queries:

**Fact Table: `fct_orders`** (grain: một đơn hàng)

| Column | Type | Mô tả |
|---|---|---|
| order_id | VARCHAR (PK) | Khóa chính |
| order_date | DATE (partition key) | Ngày đặt hàng (monthly partition) |
| customer_key | VARCHAR (FK) | FK → dim_customers |
| seller_key | VARCHAR (FK) | FK → dim_sellers (seller chính trong đơn) |
| total_items | INTEGER | Số sản phẩm trong đơn |
| gross_revenue | DECIMAL(10,2) | Tổng giá trị sản phẩm |
| freight_value | DECIMAL(10,2) | Phí vận chuyển |
| net_revenue | DECIMAL(10,2) | gross_revenue + freight_value |
| payment_type | VARCHAR | Phương thức thanh toán chính |
| review_score | INTEGER | Điểm đánh giá (1–5), NULL nếu chưa có |
| actual_delivery_days | INTEGER | Số ngày giao hàng thực tế |
| estimated_delivery_days | INTEGER | Số ngày giao hàng dự kiến |
| is_late_delivery | BOOLEAN | Giao trễ so với dự kiến |
| order_status | VARCHAR | Trạng thái cuối của đơn |

**Fact Table: `fct_funnel`** (grain: một lead)

| Column | Type | Mô tả |
|---|---|---|
| mql_id | VARCHAR (PK) | Khóa chính |
| first_contact_date | DATE | Ngày tiếp cận đầu tiên |
| origin | VARCHAR | Kênh tiếp thị (organic, paid, social) |
| business_segment | VARCHAR | Ngành nghề của seller |
| is_converted | BOOLEAN | Lead có trở thành deal không |
| won_date | DATE | Ngày chốt deal (NULL nếu chưa convert) |
| days_to_close | INTEGER | Số ngày từ first_contact đến won_date |
| seller_id | VARCHAR (FK) | FK → dim_sellers (NULL nếu chưa convert) |

**Dimension Table: `dim_sellers`**

| Column | Type | Mô tả |
|---|---|---|
| seller_id | VARCHAR (PK) | Khóa chính |
| seller_city | VARCHAR | Thành phố |
| seller_state | VARCHAR | Bang (2 ký tự) |
| total_orders | INTEGER | Tổng đơn hàng lịch sử |
| avg_review_score | DECIMAL(3,2) | Điểm đánh giá trung bình |
| on_time_delivery_rate | DECIMAL(5,4) | Tỷ lệ giao đúng hạn |
| first_order_date | DATE | Ngày có đơn hàng đầu tiên |

**Dimension Table: `dim_customers`**

| Column | Type | Mô tả |
|---|---|---|
| customer_id | VARCHAR (PK) | Khóa chính |
| customer_city | VARCHAR | Thành phố |
| customer_state | VARCHAR | Bang |
| total_orders | INTEGER | Tổng số đơn hàng |
| total_spend | DECIMAL(10,2) | Tổng chi tiêu |
| avg_order_value | DECIMAL(10,2) | Giá trị đơn hàng trung bình |
| first_order_date | DATE | Ngày đặt hàng đầu tiên |
| last_order_date | DATE | Ngày đặt hàng gần nhất |
| is_repeat_customer | BOOLEAN | Có hơn 1 đơn hàng |

---

## 3.4 Thiết kế Medallion Pipeline

### 3.4.1 Batch Pipeline

```
[Kaggle CSV] 
    │
    ▼ upload_raw_to_r2.py
[R2: s3://olist-lakehouse/raw/{table}.csv]
    │
    ▼ batch_ingest_bronze.py
    ├── Đọc CSV từ R2 bằng pandas (S3 URI)
    ├── Thêm metadata columns: _ingested_at, _source_file, _batch_id
    ├── Convert sang PyArrow Table (schema mapping)
    └── PyIceberg table.append(arrow_table)
[Bronze Iceberg: s3://olist-lakehouse/bronze/ecom/{table}/]
```

Mỗi bảng là một Prefect task độc lập, chạy song song (`task.submit()`). 10 tasks chạy đồng thời, thời gian tổng bằng thời gian của bảng lớn nhất.

### 3.4.2 Streaming Pipeline

```
[streaming_producer.py]
    ├── Đọc olist_orders_dataset.csv
    ├── Sort theo order_purchase_timestamp (ASC)
    ├── Tính time gap giữa các events
    ├── Sleep proportional to gap (SPEED_FACTOR = 86400: 1 ngày = 1 giây)
    └── confluent_kafka Producer.produce(
            topic="olist.orders",
            key=order_id,             # partition key
            value=json.dumps(payload)
        )

[Redpanda: topic olist.orders, 4 partitions]

[stream_consumer.py]
    ├── Consumer group: iceberg-bronze-writer
    ├── Poll messages theo vòng lặp
    ├── Buffer 100 messages
    ├── Convert buffer → pandas DataFrame
    ├── PyIceberg table.append(df)
    └── consumer.commit() sau khi append thành công
[Bronze Iceberg: bronze.ecom.orders]
```

**Quan trọng**: Consumer chỉ commit offset SAU KHI append Iceberg thành công. Nếu append fail, consumer restart từ offset cũ — đảm bảo at-least-once delivery. Iceberg append là idempotent khi cùng data, nên duplicate không gây lỗi.

### 3.4.3 Transformation Pipeline

```
[Bronze Iceberg] (read by DuckDB via Iceberg extension)
    │
    ▼ dbt run --select silver.*
    ├── stg_orders.sql      (incremental, merge, unique_key=order_id)
    ├── stg_sellers.sql     (incremental, merge, unique_key=seller_id)
    ├── stg_customers.sql   (incremental, merge, unique_key=customer_id)
    ├── stg_products.sql    (incremental, merge, unique_key=product_id)
    ├── stg_order_items.sql (incremental, append, unique_key=order_id+order_item_id)
    ├── stg_payments.sql    (incremental, append)
    ├── stg_reviews.sql     (incremental, merge, unique_key=review_id)
    └── int_orders_enriched.sql (ephemeral: join orders+items+payments+reviews)
    
    ▼ dbt run --select gold.*
    ├── fct_orders.sql     (table, partition_by=order_date, insert_overwrite)
    ├── dim_sellers.sql    (table, full refresh)
    ├── dim_customers.sql  (table, full refresh)
    └── fct_funnel.sql     (table, full refresh)
```

---

## 3.5 Thiết kế Orchestration (Prefect)

Ba flows được thiết kế với dependency rõ ràng:

### Flow 1: `bronze_ingestion_flow`

```python
@flow(name="Bronze Ingestion")
def bronze_ingestion_flow(tables: list[str]):
    # Step 1: Upload raw CSV → R2 (parallel)
    upload_futures = [upload_raw.submit(table) for table in tables]
    
    # Step 2: Ingest R2 → Bronze Iceberg (parallel, sau khi upload xong)
    ingest_futures = [
        ingest_to_bronze.submit(table, wait_for=[upload_futures[i]])
        for i, table in enumerate(tables)
    ]
    
    # Step 3: Quality checks (serial, sau khi tất cả ingest xong)
    for table in tables:
        run_soda_checks.submit(table, wait_for=ingest_futures)
```

### Flow 2: `dbt_transform_flow`

```python
@flow(name="dbt Transform")
def dbt_transform_flow():
    silver = run_dbt_silver()           # dbt run --select silver.*
    gold = run_dbt_gold(wait_for=[silver])  # dbt run --select gold.*
    test = run_dbt_test(wait_for=[gold])    # dbt test
```

### Flow 3: `full_pipeline_flow`

```python
@flow(name="Full Medallion Pipeline", 
      description="Daily: Bronze → Quality → Silver → Gold")
def full_pipeline_flow():
    ingest = bronze_ingestion_flow(tables=ALL_TABLES)
    transform = dbt_transform_flow(wait_for=[ingest])
    return transform
```

**Deployment config**:
- Schedule: `0 2 * * *` (02:00 UTC hàng ngày)
- Work pool: `docker-work-pool` (chạy trong container)
- Retries flow-level: 1 (restart toàn bộ flow nếu fail)

---

## 3.6 Thiết kế Dashboard Analytics

Dashboard Streamlit kết nối trực tiếp DuckDB đọc Gold Layer từ R2, gồm 3 tabs:

### Tab 1 — Revenue Analytics

| Biểu đồ | Nguồn dữ liệu | Loại chart |
|---|---|---|
| Monthly Revenue Trend | `fct_orders` GROUP BY order_date | Line chart |
| Revenue by Product Category | `fct_orders` JOIN `dim_products` | Horizontal bar |
| Revenue by State (Brazil map) | `fct_orders` JOIN `dim_customers` | Choropleth map |
| Average Order Value trend | `fct_orders` | Line chart |

### Tab 2 — Seller Performance

| Biểu đồ | Nguồn dữ liệu | Loại chart |
|---|---|---|
| Top 10 Sellers by Revenue | `dim_sellers` | Bar chart |
| Review Score vs. Delivery Time | `fct_orders` JOIN `dim_sellers` | Scatter plot |
| On-time Delivery Rate by State | `dim_sellers` | Choropleth map |
| Seller Activity Timeline | `fct_orders` GROUP BY seller, month | Heatmap |

### Tab 3 — Customer Analytics

| Biểu đồ | Nguồn dữ liệu | Loại chart |
|---|---|---|
| Customer Geographic Distribution | `dim_customers` | Scatter map |
| Repeat vs. One-time Customers | `dim_customers` | Pie chart |
| Customer Lifetime Value Distribution | `dim_customers` | Histogram |
| New vs. Returning Orders by Month | `fct_orders` JOIN `dim_customers` | Stacked bar |

---

*Thiết kế trong chương này được hiện thực hóa trong Chương 4 — Xây dựng và Triển khai Hệ thống.*
