# CHƯƠNG 4. XÂY DỰNG VÀ TRIỂN KHAI HỆ THỐNG

## 4.1 Môi trường triển khai

### 4.1.1 Docker Compose Stack

Toàn bộ hệ thống được containerized và quản lý bằng Docker Compose. Cấu hình bao gồm 6 services chính:

| Service | Image | Role | Port |
|---|---|---|---|
| `iceberg-rest` | `tabulario/iceberg-rest:latest` | Iceberg REST Catalog | 8181 |
| `redpanda` | `redpandadata/redpanda:latest` | Kafka-compatible broker | 9092, 19092 |
| `redpanda-console` | `redpandadata/console:latest` | Redpanda Web UI | 8080 |
| `prefect-server` | `prefecthq/prefect:3-latest` | Prefect API + UI | 4200 |
| `prefect-worker` | Custom (Python 3.11) | Flow executor | — |
| `postgres` | `postgres:15` | Backend DB (Prefect metadata) | 5432 |

**Lưu ý quan trọng**: DuckDB là embedded library, **không có container**. DuckDB chạy in-process bên trong `prefect-worker` container khi dbt transform chạy.

Cấu hình `.env` lưu các secrets cần thiết:

```ini
# Cloudflare R2
AWS_ACCESS_KEY_ID=<r2-access-key>
AWS_SECRET_ACCESS_KEY=<r2-secret-key>
S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
S3_BUCKET=olist-lakehouse

# Iceberg REST Catalog
CATALOG_WAREHOUSE=s3://olist-lakehouse/
CATALOG_IO_IMPL=org.apache.iceberg.aws.s3.S3FileIO

# Prefect
PREFECT_API_URL=http://prefect-server:4200/api
```

### 4.1.2 Healthchecks và Dependencies

Mỗi service có healthcheck để đảm bảo startup order đúng:

```yaml
iceberg-rest:
  healthcheck:
    test: ["CMD", "bash", "-c", "echo > /dev/tcp/localhost/8181"]
    interval: 10s
    retries: 5

prefect-server:
  healthcheck:
    test: ["CMD", "python3", "-c",
      "import urllib.request; urllib.request.urlopen('http://localhost:4200/api/health')"]
    interval: 15s
    retries: 8
  depends_on:
    postgres:
      condition: service_healthy
```

`prefect-worker` chỉ start sau khi `prefect-server` healthy → tránh tình trạng worker đăng ký vào server chưa sẵn sàng.

---

## 4.2 Khởi tạo Bronze Tables (iceberg_setup.py)

Trước khi ingest dữ liệu, cần khởi tạo Iceberg namespaces và tables trong REST Catalog. File `ingestion/iceberg_setup.py` thực hiện việc này:

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, TimestampType, 
    DoubleType, IntegerType, BooleanType
)

def get_catalog():
    return load_catalog(
        "rest",
        **{
            "uri": "http://iceberg-rest:8181",
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        }
    )

ORDERS_SCHEMA = Schema(
    NestedField(1, "order_id", StringType(), required=True),
    NestedField(2, "customer_id", StringType(), required=True),
    NestedField(3, "order_status", StringType()),
    NestedField(4, "order_purchase_timestamp", TimestampType()),
    NestedField(5, "order_approved_at", TimestampType()),
    NestedField(6, "order_delivered_carrier_date", TimestampType()),
    NestedField(7, "order_delivered_customer_date", TimestampType()),
    NestedField(8, "order_estimated_delivery_date", TimestampType()),
    # Metadata columns
    NestedField(100, "_ingested_at", TimestampType(), required=True),
    NestedField(101, "_source_file", StringType()),
    NestedField(102, "_batch_id", StringType()),
)

def setup_bronze_tables():
    catalog = get_catalog()
    
    # Create namespaces
    for ns in [("bronze",), ("bronze", "ecom"), ("bronze", "marketing")]:
        try:
            catalog.create_namespace(ns)
        except Exception:
            pass  # Đã tồn tại
    
    # Create tables
    tables = {
        "bronze.ecom.orders": ORDERS_SCHEMA,
        "bronze.ecom.order_items": ORDER_ITEMS_SCHEMA,
        # ... các bảng khác
    }
    
    for table_name, schema in tables.items():
        try:
            catalog.create_table(
                identifier=table_name,
                schema=schema,
                properties={
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "snappy",
                    "format-version": "2",
                }
            )
            print(f"Created: {table_name}")
        except Exception as e:
            print(f"Skipped {table_name}: {e}")
```

Mỗi bảng Bronze có thêm 3 metadata columns (`_ingested_at`, `_source_file`, `_batch_id`) để traceability — biết chính xác khi nào và từ đâu mỗi batch của data được ingest.

---

## 4.3 Xây dựng Ingestion Layer

### 4.3.1 Batch Ingestion (batch_ingest_bronze.py)

```python
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from datetime import datetime, timezone
import uuid

COLUMN_TYPE_MAP = {
    "order_purchase_timestamp": "datetime64[us]",
    "order_approved_at": "datetime64[us]",
    "order_delivered_carrier_date": "datetime64[us]",
    "order_delivered_customer_date": "datetime64[us]",
    "order_estimated_delivery_date": "datetime64[us]",
}

def ingest_table(table_name: str, s3_path: str):
    """Đọc CSV từ R2, thêm metadata, append vào Bronze Iceberg."""
    catalog = get_catalog()
    iceberg_table = catalog.load_table(f"bronze.ecom.{table_name}")
    
    # Đọc CSV
    df = pd.read_csv(
        s3_path,
        storage_options={
            "key": os.environ["AWS_ACCESS_KEY_ID"],
            "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
            "client_kwargs": {"endpoint_url": os.environ["S3_ENDPOINT"]}
        }
    )
    
    # Cast types
    for col, dtype in COLUMN_TYPE_MAP.items():
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').astype(dtype)
    
    # Thêm metadata
    batch_id = str(uuid.uuid4())
    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_source_file"] = s3_path
    df["_batch_id"] = batch_id
    
    # Append vào Iceberg
    arrow_table = pa.Table.from_pandas(df)
    iceberg_table.append(arrow_table)
    
    print(f"Ingested {len(df)} rows → bronze.ecom.{table_name}")
    return len(df)
```

### 4.3.2 Streaming Producer (streaming_producer.py)

Producer replay lịch sử đơn hàng theo thứ tự thời gian thực với speed factor:

```python
import pandas as pd
import json
import time
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "redpanda:9092"
SPEED_FACTOR = 86400  # 1 ngày = 1 giây (compress 2 năm lịch sử thành ~730 giây)

def create_order_payload(row: pd.Series) -> dict:
    return {
        "event_type": "order_created",
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "order_status": row["order_status"],
        "order_purchase_timestamp": str(row["order_purchase_timestamp"]),
        "order_estimated_delivery_date": str(row["order_estimated_delivery_date"]),
    }

def replay_orders():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "linger.ms": 5,           # Batch nhỏ để tăng throughput
        "batch.size": 16384,
    })
    
    df = pd.read_csv("data/olist_orders_dataset.csv",
                     parse_dates=["order_purchase_timestamp"])
    df = df.sort_values("order_purchase_timestamp").reset_index(drop=True)
    
    prev_ts = None
    for _, row in df.iterrows():
        if prev_ts is not None:
            gap_seconds = (row["order_purchase_timestamp"] - prev_ts).total_seconds()
            simulated_delay = gap_seconds / SPEED_FACTOR
            if simulated_delay > 0:
                time.sleep(simulated_delay)
        
        producer.produce(
            topic="olist.orders",
            key=row["order_id"].encode("utf-8"),
            value=json.dumps(create_order_payload(row)).encode("utf-8"),
            callback=delivery_callback
        )
        producer.poll(0)  # Non-blocking, trigger callbacks
        prev_ts = row["order_purchase_timestamp"]
    
    producer.flush()
    print("Replay hoàn tất.")
```

### 4.3.3 Streaming Consumer (stream_consumer.py)

Consumer đọc messages từ Redpanda và append vào Bronze Iceberg theo batch:

```python
from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
from datetime import datetime, timezone

BATCH_SIZE = 100  # Append sau mỗi 100 messages để giảm Iceberg write overhead

def consume_to_bronze():
    consumer = Consumer({
        "bootstrap.servers": "redpanda:9092",
        "group.id": "iceberg-bronze-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Manual commit sau khi append thành công
    })
    consumer.subscribe(["olist.orders"])
    
    catalog = get_catalog()
    table = catalog.load_table("bronze.ecom.orders")
    
    buffer = []
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                if buffer:
                    flush_buffer(table, buffer, consumer)
                    buffer = []
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise Exception(f"Kafka error: {msg.error()}")
            
            payload = json.loads(msg.value().decode("utf-8"))
            payload["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            payload["_kafka_offset"] = msg.offset()
            payload["_kafka_partition"] = msg.partition()
            buffer.append(payload)
            
            if len(buffer) >= BATCH_SIZE:
                flush_buffer(table, buffer, consumer)
                buffer = []
    
    finally:
        consumer.close()

def flush_buffer(table, buffer: list, consumer):
    """Append buffer vào Iceberg, commit offset sau khi thành công."""
    df = pd.DataFrame(buffer)
    arrow_table = pa.Table.from_pandas(df)
    table.append(arrow_table)          # Iceberg ACID write
    consumer.commit(asynchronous=False)  # Commit offset chỉ sau khi write thành công
    print(f"Flushed {len(buffer)} messages → Bronze Iceberg")
```

**Lưu ý về at-least-once**: Consumer commit offset SAU KHI Iceberg append thành công. Nếu crash giữa chừng, consumer restart sẽ đọc lại messages chưa commit → có thể có duplicate. Đây được xử lý ở Silver layer bằng `merge` strategy với `unique_key=order_id`.

### 4.3.4 Data Quality với Soda Core (soda_checks.yml)

```yaml
# quality/soda_checks.yml
checks for bronze_orders:
  - row_count > 0:
      name: "Bảng orders không được rỗng"
  - missing_count(order_id) = 0:
      name: "order_id không được NULL"
  - duplicate_count(order_id) = 0:
      name: "order_id phải unique trong batch"
  - missing_count(customer_id) = 0:
      name: "customer_id không được NULL"
  - values in (order_status) must exist in ['delivered', 'shipped', 'canceled',
      'unavailable', 'invoiced', 'processing', 'approved', 'created']:
      name: "order_status chỉ nhận giá trị hợp lệ"

checks for bronze_order_items:
  - row_count > 0
  - missing_count(order_id) = 0
  - missing_count(product_id) = 0
  - min(price) >= 0:
      name: "Giá sản phẩm không được âm"
  - min(freight_value) >= 0:
      name: "Phí vận chuyển không được âm"

checks for bronze_order_payments:
  - row_count > 0
  - missing_count(order_id) = 0
  - min(payment_value) >= 0:
      name: "Giá trị thanh toán không được âm"
  - max(payment_installments) <= 24:
      name: "Số kỳ trả góp tối đa 24"
```

---

## 4.4 Xây dựng Medallion Transformation (dbt)

### 4.4.1 Cấu hình dbt Project

`dbt_project.yml` định nghĩa materialization strategy cho từng layer:

```yaml
name: 'olist'
version: '1.0.0'
config-version: 2

profile: 'olist_duckdb'

models:
  olist:
    silver:
      +materialized: incremental
      +file_format: iceberg
      +incremental_strategy: merge
      +on_schema_change: sync_all_columns
      intermediate:
        +materialized: ephemeral
    gold:
      +materialized: table
      +file_format: iceberg
```

`profiles.yml` kết nối DuckDB với Iceberg REST Catalog:

```yaml
olist_duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ":memory:"
      extensions:
        - httpfs
        - iceberg
      settings:
        s3_endpoint: "{{ env_var('S3_ENDPOINT') }}"
        s3_access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
        s3_secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
```

### 4.4.2 Silver Models

**stg_orders.sql** — staging model cho bảng orders:

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze_ecom', 'orders') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        CAST(order_purchase_timestamp AS TIMESTAMP)    AS purchased_at,
        CAST(order_approved_at AS TIMESTAMP)           AS approved_at,
        CAST(order_delivered_carrier_date AS TIMESTAMP) AS shipped_at,
        CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_at,
        CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_at,
        _ingested_at,
        CURRENT_TIMESTAMP AS _updated_at
    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM cleaned
```

**int_orders_enriched.sql** — intermediate model (ephemeral, không lưu):

```sql
{{ config(materialized='ephemeral') }}

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.purchased_at,
    o.delivered_at,
    o.estimated_delivery_at,
    -- Tính số ngày giao hàng
    DATE_DIFF('day', o.purchased_at, o.delivered_at)          AS actual_delivery_days,
    DATE_DIFF('day', o.purchased_at, o.estimated_delivery_at) AS estimated_delivery_days,
    o.delivered_at > o.estimated_delivery_at                   AS is_late_delivery,
    -- Aggregates từ order_items
    SUM(i.price)         AS gross_revenue,
    SUM(i.freight_value) AS freight_value,
    COUNT(i.order_item_id) AS total_items,
    -- Aggregates từ payments
    MAX(p.payment_type)  AS primary_payment_type,
    SUM(p.payment_value) AS total_payment_value,
    -- Review
    r.review_score
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_order_items') }} i ON o.order_id = i.order_id
LEFT JOIN {{ ref('stg_payments') }}    p ON o.order_id = p.order_id
LEFT JOIN {{ ref('stg_reviews') }}     r ON o.order_id = r.review_id
GROUP BY o.order_id, o.customer_id, o.order_status, o.purchased_at,
         o.delivered_at, o.estimated_delivery_at, r.review_score
```

### 4.4.3 Gold Models

**fct_orders.sql** — fact table chính:

```sql
{{ config(
    materialized='table',
    file_format='iceberg',
    partition_by=[{"field": "order_date", "data_type": "date", "granularity": "month"}]
) }}

SELECT
    order_id,
    customer_id,
    DATE_TRUNC('month', purchased_at)::DATE AS order_date,
    order_status,
    actual_delivery_days,
    estimated_delivery_days,
    is_late_delivery,
    gross_revenue,
    freight_value,
    gross_revenue + freight_value           AS net_revenue,
    total_items,
    primary_payment_type                    AS payment_type,
    review_score,
    purchased_at,
    delivered_at
FROM {{ ref('int_orders_enriched') }}
WHERE order_id IS NOT NULL
```

**dim_sellers.sql** — dimension table người bán:

```sql
{{ config(materialized='table', file_format='iceberg') }}

SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT o.order_id)                    AS total_orders,
    SUM(o.net_revenue)                            AS total_revenue,
    AVG(o.review_score)                           AS avg_review_score,
    AVG(CASE WHEN NOT o.is_late_delivery THEN 1.0 ELSE 0.0 END) AS on_time_delivery_rate,
    MIN(o.purchased_at)::DATE                     AS first_order_date,
    MAX(o.purchased_at)::DATE                     AS last_order_date
FROM {{ ref('stg_sellers') }} s
LEFT JOIN {{ ref('stg_order_items') }} i ON s.seller_id = i.seller_id
LEFT JOIN {{ ref('fct_orders') }} o      ON i.order_id = o.order_id
GROUP BY s.seller_id, s.seller_city, s.seller_state
```

### 4.4.4 dbt Tests (schema.yml)

```yaml
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: order_status
        tests:
          - accepted_values:
              values: ['delivered','shipped','canceled','unavailable',
                       'invoiced','processing','approved','created']
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id

  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: net_revenue
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
```

---

## 4.5 Xây dựng Orchestration Layer (Prefect)

### 4.5.1 Bronze Ingestion Flow

```python
# prefect/flows/bronze_ingestion.py
from prefect import flow, task
from prefect.logging import get_run_logger

TABLES = [
    "orders", "order_items", "order_payments",
    "order_reviews", "customers", "sellers",
    "products", "geolocation"
]

@task(retries=3, retry_delay_seconds=30, name="Upload CSV to R2")
def upload_raw(table: str) -> str:
    logger = get_run_logger()
    s3_path = upload_csv_to_r2(table)
    logger.info(f"Uploaded {table} → {s3_path}")
    return s3_path

@task(retries=3, retry_delay_seconds=60, name="Ingest to Bronze Iceberg")
def ingest_to_bronze(table: str, s3_path: str) -> int:
    logger = get_run_logger()
    row_count = ingest_table(table, s3_path)
    logger.info(f"Ingested {row_count} rows → bronze.ecom.{table}")
    return row_count

@task(retries=0, name="Soda Core Quality Check")
def run_quality_check(table: str) -> bool:
    from soda.scan import Scan
    scan = Scan()
    scan.set_data_source_name(f"bronze_{table}")
    scan.add_sodacl_yaml_file("quality/soda_checks.yml")
    scan.execute()
    if scan.has_check_fails():
        raise ValueError(f"Quality check FAILED for bronze.{table}. Pipeline halted.")
    return True

@flow(name="Bronze Ingestion", log_prints=True)
def bronze_ingestion_flow(tables: list[str] = TABLES):
    # Upload song song
    upload_futures = {t: upload_raw.submit(t) for t in tables}
    
    # Ingest song song (sau khi upload xong)
    ingest_futures = {
        t: ingest_to_bronze.submit(t, upload_futures[t])
        for t in tables
    }
    
    # Chờ tất cả ingest xong trước khi chạy quality checks
    for t in tables:
        ingest_futures[t].result()
    
    # Quality checks (serial để dễ debug nếu fail)
    for t in tables:
        run_quality_check(t)
```

### 4.5.2 dbt Transform Flow

```python
# prefect/flows/dbt_transforms.py
import subprocess
from prefect import flow, task

@task(retries=2, retry_delay_seconds=60, name="dbt run Silver")
def run_dbt_silver():
    result = subprocess.run(
        ["dbt", "run", "--select", "silver.*", "--profiles-dir", "/app/dbt"],
        capture_output=True, text=True, check=True
    )
    print(result.stdout)
    return "silver_complete"

@task(retries=2, name="dbt run Gold")
def run_dbt_gold():
    result = subprocess.run(
        ["dbt", "run", "--select", "gold.*", "--profiles-dir", "/app/dbt"],
        capture_output=True, text=True, check=True
    )
    print(result.stdout)
    return "gold_complete"

@task(retries=1, name="dbt test")
def run_dbt_test():
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "/app/dbt"],
        capture_output=True, text=True, check=True
    )
    print(result.stdout)

@flow(name="dbt Transform Pipeline")
def dbt_transform_flow():
    silver = run_dbt_silver()
    gold = run_dbt_gold(wait_for=[silver])
    run_dbt_test(wait_for=[gold])
```

### 4.5.3 Full Pipeline Flow

```python
# prefect/flows/full_pipeline.py
from prefect import flow
from prefect.deployments import run_deployment

@flow(name="Full Medallion Pipeline", description="Daily batch: Bronze → Silver → Gold")
def full_pipeline_flow():
    # Step 1: Ingest + Quality
    bronze_ingestion_flow()
    
    # Step 2: Transform
    dbt_transform_flow()
    
    print("Pipeline complete. Gold layer ready for analytics.")

if __name__ == "__main__":
    full_pipeline_flow.serve(
        name="daily-medallion",
        cron="0 2 * * *",      # Chạy 02:00 UTC hàng ngày
        description="Daily Olist Medallion Pipeline"
    )
```

Prefect Cloud UI hiển thị run history, task-level logs, và failure details theo thời gian thực.

---

## 4.6 Xây dựng Analytics Dashboard (Streamlit)

Dashboard kết nối DuckDB đọc trực tiếp Gold Layer từ R2:

```python
# bi/realtime_dashboard.py
import streamlit as st
import duckdb
import plotly.express as px
import os

st.set_page_config(page_title="Olist Analytics Dashboard", layout="wide")

@st.cache_resource
def get_duckdb_conn():
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;
        SET s3_endpoint = '{os.environ["S3_ENDPOINT"]}';
        SET s3_access_key_id = '{os.environ["AWS_ACCESS_KEY_ID"]}';
        SET s3_secret_access_key = '{os.environ["AWS_SECRET_ACCESS_KEY"]}';
        SET s3_url_style = 'path';
    """)
    return conn

@st.cache_data(ttl=300)  # Cache 5 phút
def load_monthly_revenue(_conn):
    return _conn.execute("""
        SELECT
            DATE_TRUNC('month', order_date) AS month,
            SUM(net_revenue) AS revenue,
            COUNT(DISTINCT order_id) AS orders
        FROM read_parquet('s3://olist-lakehouse/gold/fct_orders/data/**/*.parquet')
        WHERE order_status = 'delivered'
        GROUP BY 1 ORDER BY 1
    """).df()

def main():
    conn = get_duckdb_conn()
    
    st.title("Olist E-Commerce Analytics Dashboard")
    
    tab1, tab2, tab3 = st.tabs(["Revenue", "Sellers", "Customers"])
    
    with tab1:
        st.subheader("Monthly Revenue Trend")
        df_revenue = load_monthly_revenue(conn)
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Revenue", f"R$ {df_revenue['revenue'].sum():,.0f}")
        col2.metric("Total Orders", f"{df_revenue['orders'].sum():,}")
        col3.metric("Avg Order Value",
                    f"R$ {df_revenue['revenue'].sum()/df_revenue['orders'].sum():,.2f}")
        
        fig = px.line(df_revenue, x="month", y="revenue",
                      title="Monthly Revenue (R$)",
                      labels={"revenue": "Revenue (R$)", "month": "Month"})
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Seller Performance")
        df_sellers = conn.execute("""
            SELECT seller_id, seller_state, total_orders, total_revenue,
                   ROUND(avg_review_score, 2) AS avg_score,
                   ROUND(on_time_delivery_rate * 100, 1) AS on_time_pct
            FROM read_parquet('s3://olist-lakehouse/gold/dim_sellers/data/*.parquet')
            ORDER BY total_revenue DESC LIMIT 20
        """).df()
        st.dataframe(df_sellers, use_container_width=True)
    
    with tab3:
        st.subheader("Customer Analytics")
        df_cust = conn.execute("""
            SELECT customer_state,
                   COUNT(*) AS customer_count,
                   AVG(total_spend) AS avg_ltv,
                   SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) * 100.0
                       / COUNT(*) AS repeat_rate
            FROM read_parquet('s3://olist-lakehouse/gold/dim_customers/data/*.parquet')
            GROUP BY customer_state ORDER BY customer_count DESC
        """).df()
        
        fig = px.bar(df_cust.head(10), x="customer_state", y="customer_count",
                     title="Top 10 States by Customer Count",
                     color="repeat_rate",
                     color_continuous_scale="RdYlGn")
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
```

Dashboard chạy bằng `streamlit run bi/realtime_dashboard.py`, accessible tại `http://localhost:8501`.

---

*Kết quả vận hành và đánh giá hiệu năng của hệ thống đã triển khai được trình bày trong Chương 5.*
