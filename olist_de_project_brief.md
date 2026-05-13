# Đồ án Data Engineering – Olist Lakehouse Platform

## Datasets
- **Olist Brazilian E-Commerce**: ~100K orders, 8 bảng (orders, order_items, products, sellers, customers, payments, reviews, geolocation)
- **Olist Marketing Funnel**: leads → qualified → deals, 2 bảng (marketing_qualified_leads, closed_deals)
- Join key: `seller_id`
- Source: Kaggle (`olistbr/brazilian-ecommerce`, `olistbr/marketing-funnel-olist`)

---

## Mục tiêu kiến trúc
Xây dựng **production-grade lakehouse platform** quy mô lớn (dataset nhỏ nhưng kiến trúc simulate production):
- Lambda architecture: batch path + streaming path song song
- Lakehouse với Apache Iceberg (ACID, time travel, schema evolution)
- Orchestration đầy đủ với Dagster
- Agentic BI với Claude API

---

## Full Stack (updated with enterprise tools)

| Layer | Tool | Vai trò |
|---|---|---|
| Data lake / storage | Cloudflare R2 | S3-compatible, free egress, lưu Iceberg tables |
| Table format | Apache Iceberg + PyIceberg | ACID, time travel, schema evolution, partition pruning |
| Message broker | Redpanda | Kafka-compatible, single container, no JVM/Zookeeper |
| Stream processing | Python consumer / Bytewax | Đọc từ Redpanda, ghi vào Iceberg |
| Data quality | Soda Core / Great Expectations | Validate schema, null, anomaly trước khi vào Iceberg |
| Warehouse / query (small-medium) | DuckDB | Query Iceberg trực tiếp qua extension, nhẹ, nhanh |
| **Distributed processing (large-scale)** | **Apache Spark** ⭐ NEW | PySpark + Iceberg connector, xử lý Silver/Gold khi scale lên TB+ |
| **Query federation** | **Trino / Presto** ⭐ NEW | Join across Iceberg + Postgres + S3 CSV, ad-hoc analytics |
| Transform | dbt (dbt-duckdb adapter) | staging → intermediate → marts |
| **Orchestration (primary)** | **Prefect** ⭐ NEW | @flow/@task decorators, Prefect Cloud UI, real-time monitoring |
| Orchestration (alternative) | Dagster | Assets, sensors, schedules, lineage UI |
| **ML lifecycle** | **MLflow** ⭐ NEW | Experiment tracking, model registry, model serving API |
| CI/CD | GitHub Actions | dbt test, lint SQL, schema check tự động |
| Containerization | Docker Compose | Đóng gói toàn bộ stack |
| BI dashboard | Evidence / Metabase | Historical analytics |
| Agentic BI | Streamlit + Claude API | NL → SQL → Iceberg query (via Trino/DuckDB) |
| Real-time dashboard | Streamlit / custom | Live orders, SLA alerts, lead velocity + ML predictions |

---

## Architecture Flow

```
┌─────────────────────────────────────────────────────┐
│                    DATA SOURCES                      │
│  Olist E-Commerce CSV    Olist Marketing Funnel CSV  │
└────────────┬────────────────────────┬───────────────┘
             │                        │
     ┌───────▼────────┐    ┌──────────▼──────────┐
     │  BATCH PATH    │    │   STREAMING PATH     │
     │                │    │                      │
     │ Python producer│    │ Streaming producer   │
     │ CSV→Parquet    │    │ Replay by timestamp  │
     │                │    │ SPEED_FACTOR=86400   │
     │ Soda Core / GX │    │                      │
     │ quality check  │    │ Redpanda             │
     │                │    │ Topics:              │
     │                │    │ olist.orders         │
     │                │    │ olist.reviews        │
     │                │    │ olist.payments       │
     │                │    │ olist.leads          │
     │                │    │ olist.deals          │
     └───────┬────────┘    └──────────┬───────────┘
             │                        │
             └──────────┬─────────────┘
                        │
        ┌───────────────▼───────────────────────────┐
        │     APACHE ICEBERG ON CLOUDFLARE R2        │
        │                                            │
        │  raw zone → staging zone → curated zone   │
        │  ACID · time travel · schema evolution     │
        │  PyIceberg catalog                         │
        └───────────────┬───────────────────────────┘
                        │
        ┌───────────────▼───────────────────────────┐
        │           COMPUTE – dbt + DuckDB           │
        │  staging → intermediate → marts            │
        │  fct_orders, fct_funnel, dim_sellers...    │
        └───────────────┬───────────────────────────┘
                        │
        ┌───────────────▼───────────────────────────┐
        │              DAGSTER                       │
        │  Assets · Sensors · Partitioned runs       │
        │  ← Docker Compose    GitHub Actions →      │
        └───────────────┬───────────────────────────┘
                        │
        ┌───────────────▼───────────────────────────┐
        │             SERVING / BI                   │
        │  Evidence/Metabase  │  Agentic BI          │
        │  Historical dash    │  Streamlit+Claude    │
        │                     │  Real-time dashboard │
        └────────────────────────────────────────────┘
```

---

## Medallion Architecture – Iceberg trên R2

**Toàn bộ lakehouse được tổ chức theo Medallion Architecture (Bronze → Silver → Gold)**, mỗi layer đều là Iceberg tables với ACID, time travel, và schema evolution.

```
r2://olist-lakehouse/
├── bronze/                                    ← RAW LAYER (Iceberg tables)
│   ├── ecom/
│   │   ├── orders/
│   │   │   ├── data/
│   │   │   │   ├── 00001-*.parquet
│   │   │   │   └── 00002-*.parquet
│   │   │   └── metadata/
│   │   │       ├── snap-8392847298374.avro   ← Iceberg snapshots
│   │   │       └── v1.metadata.json          ← Iceberg catalog metadata
│   │   ├── order_items/
│   │   ├── reviews/
│   │   ├── payments/
│   │   ├── sellers/
│   │   └── customers/
│   └── marketing/
│       ├── leads/
│       └── deals/
│
├── silver/                                    ← CLEANED LAYER (Iceberg tables)
│   ├── stg_orders/                           ← cleaned, typed, deduped
│   ├── stg_order_items/
│   ├── stg_reviews/
│   ├── stg_sellers/                          ← enriched with geolocation
│   ├── stg_customers/
│   ├── stg_funnel/                           ← lead → deal joined
│   └── int_orders_enriched/                  ← intermediate joins
│
└── gold/                                      ← BUSINESS-READY LAYER (Iceberg tables)
    ├── fct_orders/                           ← partitioned by order_date
    │   ├── data/
    │   │   ├── order_date=2017-01/
    │   │   ├── order_date=2017-02/
    │   │   └── ...
    │   └── metadata/
    ├── fct_funnel/                           ← CAC, conversion rate, velocity
    ├── dim_sellers/                          ← SCD Type 2, marketing source
    └── dim_customers/
```

### Medallion Layer – Vai trò

| Layer | Vai trò | Materialization | Iceberg feature |
|-------|---------|-----------------|-----------------|
| **Bronze** | Raw, append-only, immutable, schema-on-read | Incremental (append) | Time travel, snapshots |
| **Silver** | Cleaned, typed, joined, validated | Incremental (merge) | ACID writes, schema evolution |
| **Gold** | Aggregated, partitioned, BI-ready | Table (full refresh hoặc incremental) | Partition pruning, hidden partitioning |

---

## Redpanda – Docker Compose

```yaml
version: "3.8"
services:
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --smp 1
      - --memory 512M
      - --overprovisioned
      - --kafka-addr PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr PLAINTEXT://redpanda:9092
    ports:
      - "9092:9092"
      - "8080:8080"   # Redpanda Console UI
    volumes:
      - redpanda_data:/var/lib/redpanda/data

  redpanda-console:
    image: redpandadata/console:latest
    ports:
      - "8081:8080"
    environment:
      KAFKA_BROKERS: redpanda:9092
    depends_on: [redpanda]

volumes:
  redpanda_data:
```

```bash
# Tạo topics
rpk topic create olist.orders   --partitions 3 --replicas 1
rpk topic create olist.reviews  --partitions 3 --replicas 1
rpk topic create olist.payments --partitions 3 --replicas 1
rpk topic create olist.leads    --partitions 2 --replicas 1
rpk topic create olist.deals    --partitions 2 --replicas 1
```

---

## Streaming Producer

```python
import pandas as pd
import json
import time
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"   # Redpanda – Kafka-compatible, no code change needed
SPEED_FACTOR = 86400           # 1 ngày thực = 1 giây replay (~12 phút cho 2 năm data)

producer = Producer({"bootstrap.servers": BOOTSTRAP})

def load_events():
    orders  = pd.read_csv("olist_orders_dataset.csv", parse_dates=[
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_timestamp", "order_estimated_delivery_date"
    ])
    reviews = pd.read_csv("olist_order_reviews_dataset.csv",
                          parse_dates=["review_creation_date"])
    leads   = pd.read_csv("olist_marketing_qualified_leads_dataset.csv",
                          parse_dates=["first_contact_date"])
    deals   = pd.read_csv("olist_closed_deals_dataset.csv",
                          parse_dates=["won_date"])

    events = []

    for _, row in orders.iterrows():
        events.append({"ts": row["order_purchase_timestamp"],
                       "topic": "olist.orders", "key": row["order_id"],
                       "payload": {"event_type": "order_created",
                                   "order_id": row["order_id"],
                                   "customer_id": row["customer_id"],
                                   "status": row["order_status"],
                                   "timestamp": str(row["order_purchase_timestamp"])}})
        if pd.notna(row["order_approved_at"]):
            events.append({"ts": row["order_approved_at"],
                           "topic": "olist.orders", "key": row["order_id"],
                           "payload": {"event_type": "order_approved",
                                       "order_id": row["order_id"],
                                       "timestamp": str(row["order_approved_at"])}})
        if pd.notna(row["order_delivered_timestamp"]):
            events.append({"ts": row["order_delivered_timestamp"],
                           "topic": "olist.orders", "key": row["order_id"],
                           "payload": {"event_type": "order_delivered",
                                       "order_id": row["order_id"],
                                       "timestamp": str(row["order_delivered_timestamp"]),
                                       "is_late": row["order_delivered_timestamp"] > row["order_estimated_delivery_date"]}})

    for _, row in reviews.iterrows():
        events.append({"ts": row["review_creation_date"],
                       "topic": "olist.reviews", "key": row["order_id"],
                       "payload": {"event_type": "review_submitted",
                                   "order_id": row["order_id"],
                                   "score": int(row["review_score"]),
                                   "timestamp": str(row["review_creation_date"])}})

    for _, row in leads.iterrows():
        events.append({"ts": row["first_contact_date"],
                       "topic": "olist.leads", "key": row["mql_id"],
                       "payload": {"event_type": "lead_captured",
                                   "mql_id": row["mql_id"],
                                   "origin": row["origin"],
                                   "timestamp": str(row["first_contact_date"])}})

    for _, row in deals.iterrows():
        events.append({"ts": row["won_date"],
                       "topic": "olist.deals", "key": row["mql_id"],
                       "payload": {"event_type": "deal_closed",
                                   "mql_id": row["mql_id"],
                                   "seller_id": row["seller_id"],
                                   "business_segment": row["business_segment"],
                                   "timestamp": str(row["won_date"])}})

    events.sort(key=lambda e: e["ts"])
    return events

def replay(events):
    prev_ts = None
    for ev in events:
        if prev_ts is not None:
            gap = (ev["ts"] - prev_ts).total_seconds() / SPEED_FACTOR
            if gap > 0:
                time.sleep(gap)
        producer.produce(topic=ev["topic"], key=ev["key"],
                         value=json.dumps(ev["payload"]))
        producer.poll(0)
        print(f"[{ev['ts']}] {ev['payload']['event_type']} → {ev['topic']}")
        prev_ts = ev["ts"]
    producer.flush()

if __name__ == "__main__":
    events = load_events()
    print(f"Total events: {len(events):,} | Speed: {SPEED_FACTOR}x")
    replay(events)
```

---

## dbt Models cần build – Medallion mapping

```
models/
├── bronze/                          ← BRONZE LAYER (optional dbt models cho ingestion)
│   ├── bronze_orders.sql           ← hoặc dùng PyIceberg trực tiếp
│   └── bronze_leads.sql
│
├── silver/                          ← SILVER LAYER
│   ├── stg_orders.sql              ← read from bronze.ecom.orders
│   ├── stg_order_items.sql
│   ├── stg_order_payments.sql
│   ├── stg_order_reviews.sql
│   ├── stg_products.sql
│   ├── stg_sellers.sql
│   ├── stg_customers.sql
│   ├── stg_leads.sql               ← read from bronze.marketing.leads
│   ├── stg_deals.sql
│   └── intermediate/
│       ├── int_orders_enriched.sql      ← join silver.stg_orders + items + payments
│       ├── int_seller_performance.sql   ← seller + reviews + delivery
│       └── int_funnel_journey.sql       ← leads + deals + seller join
│
└── gold/                            ← GOLD LAYER (marts)
    ├── fct_orders.sql               ← grain: 1 row per order, partitioned by order_date
    ├── fct_funnel.sql               ← grain: 1 row per lead, CAC, conversion
    ├── dim_sellers.sql              ← SCD Type 2, marketing source, performance
    ├── dim_customers.sql
    └── metrics/
        ├── revenue_by_month.sql
        ├── delivery_sla_compliance.sql
        ├── seller_conversion_rate.sql  ← lead → deal → revenue
        └── review_score_trend.sql
```

### dbt project config (dbt_project.yml)

```yaml
name: olist_lakehouse
version: 1.0.0
config-version: 2

profile: olist

models:
  olist_lakehouse:
    bronze:
      +materialized: incremental
      +file_format: iceberg
      +location_root: s3://olist-lakehouse/bronze
      +incremental_strategy: append
      +on_schema_change: append_new_columns
      
    silver:
      +materialized: incremental
      +file_format: iceberg
      +location_root: s3://olist-lakehouse/silver
      +incremental_strategy: merge
      +unique_key: id
      +on_schema_change: sync_all_columns
      
      intermediate:
        +materialized: ephemeral      # không tạo table, chỉ là CTE
      
    gold:
      +materialized: table             # full refresh cho aggregated marts
      +file_format: iceberg
      +location_root: s3://olist-lakehouse/gold
      +partition_by: ['order_date']    # Iceberg hidden partitioning
      
      metrics:
        +materialized: view
```

### Key metrics cần có
- **CAC** (Customer/Seller Acquisition Cost) = marketing spend proxy / deals closed
- **Seller funnel conversion rate** = leads → qualified → closed
- **Delivery SLA compliance** = on-time / total deliveries
- **Revenue trend** by month, by seller segment, by product category
- **Review score** distribution, trend, correlation với delivery time

---

## Dagster Assets structure – Medallion lineage

```python
# assets/bronze.py
@asset(compute_kind="pyiceberg")
def bronze_ecom_orders(context) -> None:
    """
    Ingest CSV → Bronze Iceberg table (append-only)
    PyIceberg write transaction
    """

@asset(compute_kind="pyiceberg")
def bronze_marketing_leads(context) -> None:
    """Upload marketing CSV → Bronze Iceberg"""

# assets/silver.py
@asset(
    deps=[bronze_ecom_orders],
    compute_kind="dbt"
)
def silver_stg_orders(context) -> None:
    """
    dbt run --select silver.stg_orders
    Read from bronze.ecom.orders, clean, type cast
    """

@asset(
    deps=[bronze_ecom_orders, bronze_marketing_leads],
    compute_kind="dbt"
)
def silver_stg_sellers(context) -> None:
    """
    dbt run --select silver.stg_sellers
    Enrich with geolocation, join marketing source via seller_id
    """

# assets/gold.py
@asset(
    deps=[silver_stg_orders, silver_stg_sellers],
    partitions_def=MonthlyPartitionsDefinition(start_date="2016-01-01"),
    compute_kind="dbt"
)
def gold_fct_orders(context) -> None:
    """
    dbt run --select gold.fct_orders
    Partitioned by order_date (monthly)
    """

@asset(
    deps=[silver_stg_funnel, silver_stg_sellers],
    compute_kind="dbt"
)
def gold_fct_funnel(context) -> None:
    """
    dbt run --select gold.fct_funnel
    CAC, conversion rate, lead velocity
    """

# sensors/streaming_sensor.py
@sensor(job=streaming_consumer_job)
def redpanda_sensor(context):
    """
    Trigger consumer job khi có messages mới trong Redpanda
    Consumer ghi vào Bronze layer
    """

# jobs/medallion_pipeline.py
@job
def medallion_daily_refresh():
    """
    Full pipeline: bronze → silver → gold
    Chạy hàng ngày hoặc trigger bởi sensor
    """
    # Bronze ingestion
    bronze_ecom_orders()
    bronze_marketing_leads()
    
    # Silver transformation
    silver_stg_orders()
    silver_stg_sellers()
    
    # Gold marts
    gold_fct_orders()
    gold_fct_funnel()
```

**Dagster UI sẽ hiển thị lineage graph rõ ràng:**
```
bronze_ecom_orders → silver_stg_orders → gold_fct_orders
                  ↘                    ↗
bronze_marketing_leads → silver_stg_sellers → gold_fct_funnel
```

---

## Agentic BI – Streamlit + Claude API

```python
import anthropic
import duckdb
import streamlit as st

client = anthropic.Anthropic()

SCHEMA_CONTEXT = """
Iceberg tables available via DuckDB:
- fct_orders(order_id, customer_id, seller_id, order_date, revenue, delivery_days, is_late)
- fct_marketing_funnel(mql_id, seller_id, lead_date, won_date, origin, business_segment)
- dim_sellers(seller_id, city, state, marketing_source, total_revenue, avg_review_score)
- dim_customers(customer_id, city, state, total_orders, total_spent)
"""

def nl_to_sql(question: str) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": f"""
{SCHEMA_CONTEXT}

Generate a DuckDB SQL query for: {question}
Return ONLY the SQL, no explanation.
"""}]
    )
    return response.content[0].text.strip()

st.title("Olist Agentic BI")
question = st.text_input("Hỏi bằng tiếng Việt hoặc English:")

if question:
    sql = nl_to_sql(question)
    st.code(sql, language="sql")
    con = duckdb.connect()
    # Load Iceberg extension + connect R2
    con.execute("INSTALL iceberg; LOAD iceberg;")
    result = con.execute(sql).df()
    st.dataframe(result)
    st.bar_chart(result.set_index(result.columns[0])[result.columns[1]])
```

---

## Suggested repo structure

```
olist-lakehouse/
├── docker-compose.yml
├── .env.example
├── README.md
├── data/                          ← gitignore, chỉ chứa sample
├── ingestion/
│   ├── batch_producer.py
│   ├── streaming_producer.py
│   ├── stream_consumer.py
│   └── iceberg_setup.py
├── quality/
│   └── soda_checks.yml
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── dagster/
│   ├── assets/
│   ├── sensors/
│   └── jobs/
├── bi/
│   ├── agentic_bi.py             ← Streamlit + Claude API
│   └── realtime_dashboard.py
└── .github/
    └── workflows/
        └── dbt_test.yml
```

---

## Prompt cho Claude Code

```
Tôi đang xây dựng production-grade data engineering project với:

Datasets: Olist E-Commerce + Olist Marketing Funnel (Kaggle)
Stack: Cloudflare R2 + Apache Iceberg (PyIceberg) + Redpanda + 
       Soda Core + DuckDB + dbt + Dagster + Docker Compose + 
       Streamlit + Claude API (Agentic BI)

Architecture: Lambda – batch path (CSV→R2→Iceberg) và streaming path 
(replay events by timestamp→Redpanda→stream consumer→Iceberg) 
hội tụ tại Iceberg curated zone, dbt transform, Dagster orchestrate.

Iceberg zones: raw → staging → curated trên R2.
dbt models: staging → intermediate → marts (fct_orders, fct_funnel, dim_sellers).
Dagster: assets cho mỗi zone, sensor cho Redpanda, partitioned runs.
Agentic BI: Streamlit app dùng Claude API để NL→SQL→DuckDB query Iceberg.

[paste phần cụ thể cần implement]
```


---

## PyIceberg – Code examples

### Tạo Bronze Iceberg table

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType, TimestampType, DoubleType, 
    NestedField, IntegerType
)
import pandas as pd

# Load catalog (REST hoặc file-based)
catalog = load_catalog("olist", **{
    "uri": "http://localhost:8181",  # REST catalog
    "s3.endpoint": "https://xxx.r2.cloudflarestorage.com",
    "s3.access-key-id": "...",
    "s3.secret-access-key": "..."
})

# Define schema
schema = Schema(
    NestedField(1, "order_id", StringType(), required=True),
    NestedField(2, "customer_id", StringType(), required=True),
    NestedField(3, "order_status", StringType()),
    NestedField(4, "order_purchase_timestamp", TimestampType()),
    NestedField(5, "order_approved_at", TimestampType()),
    NestedField(6, "order_delivered_timestamp", TimestampType()),
    NestedField(7, "order_estimated_delivery_date", TimestampType())
)

# Create table
catalog.create_table(
    identifier="bronze.ecom.orders",
    schema=schema,
    location="s3://olist-lakehouse/bronze/ecom/orders",
    properties={
        "write.format.default": "parquet",
        "write.metadata.compression-codec": "gzip"
    }
)

# Write data (append-only)
df = pd.read_csv("olist_orders_dataset.csv")
table = catalog.load_table("bronze.ecom.orders")
table.append(df)

print(f"Bronze table created: {table.metadata.table_uuid}")
print(f"Snapshots: {len(table.metadata.snapshots)}")
```

### Stream consumer ghi vào Bronze

```python
from confluent_kafka import Consumer
from pyiceberg.catalog import load_catalog
import json

catalog = load_catalog("olist")
table = catalog.load_table("bronze.ecom.orders")

consumer = Consumer({
    "bootstrap.servers": "redpanda:9092",
    "group.id": "iceberg-bronze-writer",
    "auto.offset.reset": "earliest"
})
consumer.subscribe(["olist.orders"])

batch = []
BATCH_SIZE = 1000

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    
    event = json.loads(msg.value())
    batch.append(event)
    
    if len(batch) >= BATCH_SIZE:
        df = pd.DataFrame(batch)
        table.append(df)  # ACID write
        batch = []
        consumer.commit()
        print(f"✓ Batch written to Bronze, snapshot {table.current_snapshot().snapshot_id}")
```

### dbt model đọc từ Bronze Iceberg

```sql
-- models/silver/stg_orders.sql
{{ config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

SELECT
    order_id,
    customer_id,
    UPPER(order_status) AS order_status,  -- normalize
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_timestamp,
    order_estimated_delivery_date,
    CASE 
        WHEN order_delivered_timestamp > order_estimated_delivery_date 
        THEN TRUE 
        ELSE FALSE 
    END AS is_late_delivery,
    DATEDIFF('day', order_purchase_timestamp, order_delivered_timestamp) AS delivery_days,
    current_timestamp() AS _loaded_at
FROM {{ source('bronze', 'ecom_orders') }}
{% if is_incremental() %}
WHERE order_purchase_timestamp > (SELECT MAX(order_purchase_timestamp) FROM {{ this }})
{% endif %}
```

### Time travel query (DuckDB)

```sql
-- Query data tại snapshot cụ thể
SELECT COUNT(*) 
FROM iceberg_scan('s3://olist-lakehouse/bronze/ecom/orders', 
                  snapshot_id => 8392847298374);

-- Query data tại thời điểm cụ thể
SELECT * 
FROM iceberg_scan('s3://olist-lakehouse/bronze/ecom/orders',
                  version_as_of => '2024-01-15 09:00:00');

-- Xem tất cả snapshots
SELECT * FROM iceberg_snapshots('s3://olist-lakehouse/bronze/ecom/orders');
```

---

## Medallion Architecture – Best practices

### Bronze layer rules
- **Append-only, never update/delete** — mọi thay đổi là snapshot mới
- **Schema-on-read** — lưu đúng format gốc, không force schema ngay từ đầu
- **Retention policy** — giữ snapshots ít nhất 30 ngày, sau đó expire cũ
- **Partition minimal** — chỉ partition theo ingestion_date nếu cần, không partition business logic

### Silver layer rules
- **Type casting và validation** — đây là nơi enforce schema chặt chẽ
- **Deduplication** — dựa vào unique key (order_id, mql_id)
- **Referential integrity** — validate foreign key trước khi join
- **Incremental merge** — sử dụng `merge` strategy với `unique_key`

### Gold layer rules
- **Business partitioning** — partition theo date/month/year cho query performance
- **Denormalization** — flat structure, ít join hơn khi query
- **SCD Type 2 cho dimensions** — track lịch sử thay đổi (dim_sellers, dim_customers)
- **Aggregated metrics** — pre-compute để BI query nhanh

---


---

## New Tools Integration Details

### Apache Spark (distributed processing)

**Khi nào dùng Spark thay DuckDB:**
- Dataset > 1TB hoặc cần parallel processing trên nhiều nodes
- Complex aggregations cần distributed compute
- Join lớn giữa nhiều bảng (billions of rows)

**Setup:**
```python
# spark_config.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Olist Lakehouse") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3") \
    .config("spark.sql.catalog.olist", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.olist.type", "rest") \
    .config("spark.sql.catalog.olist.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.olist.s3.endpoint", "https://xxx.r2.cloudflarestorage.com") \
    .getOrCreate()

# Read from Bronze Iceberg
df_bronze = spark.table("olist.bronze.ecom.orders")

# Transform
df_silver = df_bronze \
    .withColumn("order_status", upper(col("order_status"))) \
    .dropDuplicates(["order_id"]) \
    .filter(col("order_status").isNotNull())

# Write to Silver Iceberg
df_silver.writeTo("olist.silver.stg_orders") \
    .using("iceberg") \
    .createOrReplace()
```

**Khi nào dùng DuckDB:**
- Dataset < 1TB, single-node đủ
- Interactive queries, fast iteration
- dbt transformations

---

### Trino / Presto (query federation)

**Use cases:**
- Join Iceberg tables (R2) với external data sources (Postgres metadata, CSV trên S3)
- Ad-hoc analytics không cần ETL vào Iceberg trước
- Exploratory queries khi chưa biết cần data nào

**Setup:**
```yaml
# docker-compose.yml
trino:
  image: trinodb/trino:latest
  ports:
    - "8080:8080"
  volumes:
    - ./trino/catalog:/etc/trino/catalog
```

```properties
# trino/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest.uri=http://iceberg-rest:8181
s3.endpoint=https://xxx.r2.cloudflarestorage.com
s3.access-key-id=...
s3.secret-access-key=...

# trino/catalog/postgres.properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/metadata
connection-user=trino
connection-password=...
```

**Query example:**
```sql
-- Join Iceberg Gold tables với Postgres external metadata
SELECT 
    o.order_id,
    o.revenue,
    m.campaign_name,
    m.cost_per_click
FROM iceberg.gold.fct_orders o
JOIN postgres.public.marketing_campaigns m 
    ON o.seller_id = m.seller_id
WHERE o.order_date >= DATE '2024-01-01'
```

---

### Prefect (modern orchestration)

**Tại sao chọn Prefect thay Dagster:**
- UI đẹp hơn, real-time monitoring tốt hơn
- Prefect Cloud free tier (không cần self-host)
- Python decorators tự nhiên hơn Dagster assets
- Automatic retries, dynamic workflows

**Workflow example:**
```python
# flows/medallion_pipeline.py
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import duckdb
from pyiceberg.catalog import load_catalog

@task(retries=3, retry_delay_seconds=60)
def ingest_to_bronze(dataset: str):
    """Upload CSV → Bronze Iceberg"""
    catalog = load_catalog("olist")
    table = catalog.load_table(f"bronze.{dataset}")
    df = pd.read_csv(f"data/{dataset}.csv")
    table.append(df)
    return f"bronze.{dataset}"

@task
def transform_to_silver(bronze_table: str):
    """dbt run silver models"""
    import subprocess
    subprocess.run(["dbt", "run", "--select", "silver.*"])
    return "silver"

@task
def aggregate_to_gold(silver_result: str):
    """dbt run gold marts"""
    import subprocess
    subprocess.run(["dbt", "run", "--select", "gold.*"])
    return "gold"

@flow(name="Medallion Daily Refresh", task_runner=ConcurrentTaskRunner())
def medallion_pipeline():
    """Full Bronze → Silver → Gold pipeline"""
    # Parallel ingestion
    bronze_ecom = ingest_to_bronze.submit("ecommerce")
    bronze_marketing = ingest_to_bronze.submit("marketing")
    
    # Wait for both Bronze ingestions
    bronze_results = [bronze_ecom.result(), bronze_marketing.result()]
    
    # Silver transformation
    silver = transform_to_silver(bronze_results)
    
    # Gold aggregation
    gold = aggregate_to_gold(silver)
    
    return gold

if __name__ == "__main__":
    medallion_pipeline()
```

**Deploy to Prefect Cloud:**
```bash
prefect cloud login
prefect deploy flows/medallion_pipeline.py:medallion_pipeline \
    --name "Olist Medallion Daily" \
    --cron "0 2 * * *"  # 2 AM daily
```

---

### MLflow (ML lifecycle)

**Use cases cho Olist:**
1. **Predict delivery delay** — train model trên `fct_orders` (features: distance, seller_state, product_weight)
2. **Churn prediction** — predict xác suất customer không order lại
3. **Lead scoring** — predict conversion rate từ lead → deal (marketing funnel)

**Training & tracking:**
```python
# ml/train_delivery_model.py
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
import duckdb

# Connect DuckDB → query Gold layer
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")

df = con.execute("""
    SELECT 
        seller_state,
        customer_state,
        product_weight_g,
        freight_value,
        DATEDIFF('day', order_purchase_timestamp, order_delivered_timestamp) AS actual_delivery_days,
        DATEDIFF('day', order_purchase_timestamp, order_estimated_delivery_date) AS estimated_delivery_days
    FROM iceberg_scan('s3://olist-lakehouse/gold/fct_orders')
    WHERE order_delivered_timestamp IS NOT NULL
""").df()

# MLflow experiment
mlflow.set_experiment("delivery-delay-prediction")

with mlflow.start_run():
    # Log params
    mlflow.log_param("model", "RandomForest")
    mlflow.log_param("n_estimators", 100)
    
    # Train
    X = df[['seller_state', 'customer_state', 'product_weight_g', 'freight_value', 'estimated_delivery_days']]
    y = df['actual_delivery_days']
    
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)
    
    # Log metrics
    train_score = model.score(X, y)
    mlflow.log_metric("train_r2", train_score)
    
    # Log model
    mlflow.sklearn.log_model(model, "model")
    
    print(f"Model logged with R²: {train_score:.3f}")
```

**Model serving API:**
```bash
# Register model
mlflow models serve -m "models:/delivery-delay-model/Production" -p 5000

# Predict via REST API
curl -X POST http://localhost:5000/invocations \
  -H 'Content-Type: application/json' \
  -d '{
    "dataframe_records": [{
      "seller_state": "SP",
      "customer_state": "RJ", 
      "product_weight_g": 500,
      "freight_value": 15.5,
      "estimated_delivery_days": 7
    }]
  }'
```

**Integration vào real-time dashboard:**
```python
# dashboard/app.py
import streamlit as st
import requests

st.title("Olist Order Tracking + Delivery Prediction")

order_id = st.text_input("Order ID")
if order_id:
    # Fetch order details from Gold layer
    order = get_order_details(order_id)
    
    # Call MLflow model API
    prediction = requests.post("http://mlflow:5000/invocations", json={
        "dataframe_records": [{
            "seller_state": order['seller_state'],
            "customer_state": order['customer_state'],
            "product_weight_g": order['product_weight_g'],
            "freight_value": order['freight_value'],
            "estimated_delivery_days": order['estimated_delivery_days']
        }]
    }).json()
    
    st.metric("Predicted Delivery Days", f"{prediction['predictions'][0]:.1f}")
    st.metric("Estimated Delivery", order['estimated_delivery_date'])
```

---

## Docker Compose (updated)

```yaml
version: "3.8"
services:
  redpanda:
    image: redpandadata/redpanda:latest
    # ... (như trước)

  iceberg-rest:
    image: tabulario/iceberg-rest:latest
    ports:
      - "8181:8181"
    environment:
      CATALOG_WAREHOUSE: s3://olist-lakehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      AWS_ACCESS_KEY_ID: ${R2_ACCESS_KEY}
      AWS_SECRET_ACCESS_KEY: ${R2_SECRET_KEY}
      AWS_REGION: auto
      S3_ENDPOINT: https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com

  duckdb:
    # DuckDB là embedded, không cần container riêng
    # Chạy trong Python scripts hoặc dbt

  spark-master:
    image: bitnami/spark:3.4
    ports:
      - "8082:8080"  # Spark UI
      - "7077:7077"  # Spark master
    environment:
      - SPARK_MODE=master
    volumes:
      - ./spark/jars:/opt/bitnami/spark/jars

  spark-worker:
    image: bitnami/spark:3.4
    depends_on: [spark-master]
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=4G
      - SPARK_WORKER_CORES=2

  trino:
    image: trinodb/trino:latest
    ports:
      - "8080:8080"
    volumes:
      - ./trino/catalog:/etc/trino/catalog

  prefect-server:
    image: prefecthq/prefect:2-latest
    ports:
      - "4200:4200"
    command: prefect server start
    environment:
      PREFECT_API_URL: http://prefect-server:4200/api

  mlflow:
    image: ghcr.io/mlflow/mlflow:latest
    ports:
      - "5000:5000"
    command: mlflow server --host 0.0.0.0 --backend-store-uri sqlite:///mlflow.db --default-artifact-root s3://olist-lakehouse/mlflow
    environment:
      AWS_ACCESS_KEY_ID: ${R2_ACCESS_KEY}
      AWS_SECRET_ACCESS_KEY: ${R2_SECRET_KEY}
      MLFLOW_S3_ENDPOINT_URL: https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com
```

---

