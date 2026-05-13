# Olist Data Lakehouse Platform – Project Context for Claude Code

## Quick Overview
Enterprise-grade data lakehouse platform with Medallion Architecture (Bronze → Silver → Gold), combining batch and streaming pipelines, query federation, and ML capabilities.

**Datasets**: Olist Brazilian E-Commerce (~100K orders, 8 tables) + Marketing Funnel (leads → deals, 2 tables)  
**Architecture**: Lambda (batch + streaming) → Medallion on Iceberg → Query/ML → Serving  
**Scale**: Small dataset (~200MB) but production-grade architecture simulating TB-scale systems

---

## Core Stack

**Storage & Format**:
- Cloudflare R2 (S3-compatible, free egress)
- Apache Iceberg (ACID, time travel, schema evolution)
- PyIceberg (Python client for Iceberg CRUD)

**Ingestion**:
- Batch: Python + pandas → Parquet → R2
- Streaming: Python + confluent-kafka → Redpanda → Iceberg
- Quality: Soda Core / Great Expectations

**Medallion Layers**:
- **Bronze**: Raw, append-only, immutable (PyIceberg writes)
- **Silver**: Cleaned, typed, joined (dbt transforms via DuckDB/Spark)
- **Gold**: Aggregated, partitioned, BI-ready (dbt marts)

**Processing**:
- DuckDB (small/medium workloads, <1TB)
- Apache Spark (large-scale, distributed, >1TB)
- dbt (SQL transformation engine, incremental models)

**Query & ML**:
- Trino/Presto (query federation across Iceberg + Postgres + CSV)
- MLflow (experiment tracking, model registry, serving)

**Orchestration**:
- Prefect (primary: @flow/@task, Cloud UI, real-time monitoring)
- Dagster (alternative: @asset dependencies, lineage)

**Serving**:
- Evidence / Metabase (historical BI)
- Streamlit + Claude API (Agentic BI: NL → SQL)
- Real-time dashboard (live orders + ML predictions)

**DevOps**:
- Docker Compose (containerization)
- GitHub Actions (CI/CD: dbt test, lint SQL)

---

## Medallion Architecture on R2

```
r2://olist-lakehouse/
├── bronze/                    # PyIceberg writes, append-only
│   ├── ecom/
│   │   ├── orders/
│   │   │   ├── data/*.parquet
│   │   │   └── metadata/
│   │   │       ├── snap-*.avro
│   │   │       └── v*.metadata.json
│   │   ├── order_items/
│   │   ├── reviews/
│   │   └── ...
│   └── marketing/
│       ├── leads/
│       └── deals/
├── silver/                    # dbt incremental merge
│   ├── stg_orders/
│   ├── stg_sellers/
│   └── int_orders_enriched/
└── gold/                      # dbt marts, partitioned
    ├── fct_orders/
    │   └── data/
    │       ├── order_date=2017-01/
    │       └── order_date=2017-02/
    ├── fct_funnel/
    ├── dim_sellers/
    └── dim_customers/
```

---

## Key Design Decisions

**Why Iceberg over raw Parquet?**
- ACID writes (batch + streaming concurrent without conflict)
- Time travel (`SELECT * FROM ... VERSION AS OF '2024-01-15'`)
- Schema evolution (add columns without breaking downstream)
- Hidden partitioning (no partition paths in queries)

**Why Redpanda over Kafka?**
- Single container, no JVM/Zookeeper
- Kafka-compatible API (same Python code)
- Lightweight (~500MB RAM vs Kafka's ~2GB)

**Why both DuckDB and Spark?**
- DuckDB: fast, embedded, perfect for <1TB (dbt default)
- Spark: distributed, scale to PB (when needed)
- Same Iceberg tables, different engines

**Why Prefect over Dagster?**
- Cleaner Python decorators (`@flow`, `@task`)
- Prefect Cloud free tier (no self-host needed)
- Better real-time monitoring UI

**Why Trino?**
- Join Iceberg + external sources without ETL
- Ad-hoc analytics across silos
- Catalogs: Iceberg, Postgres, Hive, S3

---

## dbt Project Structure

```
dbt/
├── dbt_project.yml
├── profiles.yml
└── models/
    ├── bronze/              # Optional: hvis dùng dbt cho ingestion
    │   └── bronze_orders.sql
    ├── silver/              # Cleaning & typing
    │   ├── stg_orders.sql
    │   ├── stg_sellers.sql
    │   ├── stg_funnel.sql
    │   └── intermediate/
    │       ├── int_orders_enriched.sql
    │       └── int_seller_performance.sql
    └── gold/                # Business marts
        ├── fct_orders.sql
        ├── fct_funnel.sql
        ├── dim_sellers.sql
        └── metrics/
            ├── revenue_by_month.sql
            └── delivery_sla_compliance.sql
```

**dbt_project.yml config**:
```yaml
models:
  olist:
    bronze:
      +materialized: incremental
      +file_format: iceberg
      +incremental_strategy: append
    silver:
      +materialized: incremental
      +file_format: iceberg
      +incremental_strategy: merge
      +unique_key: id
      intermediate:
        +materialized: ephemeral
    gold:
      +materialized: table
      +file_format: iceberg
      +partition_by: ['order_date']
```

---

## Prefect Workflow Example

```python
from prefect import flow, task
from pyiceberg.catalog import load_catalog
import pandas as pd

@task(retries=3)
def ingest_to_bronze(dataset: str):
    catalog = load_catalog("olist")
    table = catalog.load_table(f"bronze.ecom.{dataset}")
    df = pd.read_csv(f"data/{dataset}.csv")
    table.append(df)
    return f"bronze.ecom.{dataset}"

@task
def transform_silver():
    import subprocess
    subprocess.run(["dbt", "run", "--select", "silver.*"])

@task
def aggregate_gold():
    import subprocess
    subprocess.run(["dbt", "run", "--select", "gold.*"])

@flow(name="Medallion Daily")
def medallion_pipeline():
    bronze = ingest_to_bronze("orders")
    silver = transform_silver(wait_for=[bronze])
    gold = aggregate_gold(wait_for=[silver])
    return gold
```

---

## Streaming Producer (Redpanda)

```python
import pandas as pd
import json
import time
from confluent_kafka import Producer

BOOTSTRAP = "redpanda:9092"
SPEED_FACTOR = 86400  # 1 ngày = 1 giây

producer = Producer({"bootstrap.servers": BOOTSTRAP})

df = pd.read_csv("olist_orders_dataset.csv", parse_dates=['order_purchase_timestamp'])
events = []

for _, row in df.iterrows():
    events.append({
        "ts": row["order_purchase_timestamp"],
        "topic": "olist.orders",
        "key": row["order_id"],
        "payload": {
            "event_type": "order_created",
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "timestamp": str(row["order_purchase_timestamp"])
        }
    })

events.sort(key=lambda e: e["ts"])

prev_ts = None
for ev in events:
    if prev_ts:
        gap = (ev["ts"] - prev_ts).total_seconds() / SPEED_FACTOR
        if gap > 0:
            time.sleep(gap)
    
    producer.produce(
        topic=ev["topic"],
        key=ev["key"],
        value=json.dumps(ev["payload"])
    )
    producer.poll(0)
    prev_ts = ev["ts"]

producer.flush()
```

---

## MLflow Use Cases

1. **Delivery delay prediction**:
   - Features: `seller_state`, `customer_state`, `product_weight_g`, `freight_value`
   - Target: `actual_delivery_days - estimated_delivery_days`
   - Model: RandomForest / XGBoost

2. **Churn prediction**:
   - Features from `dim_customers`: `days_since_last_order`, `total_orders`, `avg_order_value`
   - Target: binary (will order again in next 90 days?)

3. **Lead scoring**:
   - Features from `fct_funnel`: `origin`, `business_segment`, `first_contact_date`
   - Target: probability of conversion (lead → deal)

**Serving API**:
```bash
mlflow models serve -m "models:/delivery-delay/Production" -p 5000

curl -X POST http://localhost:5000/invocations \
  -H 'Content-Type: application/json' \
  -d '{"dataframe_records": [{"seller_state": "SP", ...}]}'
```

---

## Trino Query Federation Example

```sql
-- Join Iceberg Gold với Postgres external metadata
SELECT 
    o.order_id,
    o.revenue,
    m.campaign_name
FROM iceberg.gold.fct_orders o
JOIN postgres.public.marketing_campaigns m 
    ON o.seller_id = m.seller_id
WHERE o.order_date >= DATE '2024-01-01'
```

---

## Docker Compose Services

```yaml
services:
  redpanda:           # Kafka-compatible broker
  iceberg-rest:       # Iceberg REST catalog
  spark-master:       # Spark distributed processing
  spark-worker:       # Spark workers
  trino:              # Query federation
  prefect-server:     # Workflow orchestration
  mlflow:             # ML lifecycle
  # DuckDB embedded, no container needed
```

---

## File Structure for Implementation

```
olist-lakehouse/
├── docker-compose.yml
├── .env.example
├── README.md
├── data/                    # gitignore
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
│       ├── bronze/
│       ├── silver/
│       └── gold/
├── prefect/
│   ├── flows/
│   │   └── medallion_pipeline.py
│   └── deployments/
├── spark/
│   ├── jobs/
│   │   └── silver_transform.py
│   └── jars/
├── trino/
│   └── catalog/
│       ├── iceberg.properties
│       └── postgres.properties
├── ml/
│   ├── train_delivery_model.py
│   ├── train_churn_model.py
│   └── serve_model.py
├── bi/
│   ├── agentic_bi.py
│   └── realtime_dashboard.py
└── .github/
    └── workflows/
        └── dbt_test.yml
```

---

## Common Prompts for Claude Code

**Setup & Infrastructure**:
- "Create `iceberg_setup.py` to initialize Bronze tables with PyIceberg"
- "Write `docker-compose.yml` with all services configured"
- "Generate Trino catalog configs for Iceberg and Postgres"

**Ingestion**:
- "Implement `streaming_producer.py` to replay Olist CSV events into Redpanda"
- "Write `stream_consumer.py` to consume from Redpanda and append to Bronze Iceberg"
- "Create Soda Core YAML checks for Bronze data quality"

**Transformation**:
- "Generate dbt `stg_orders.sql` Silver model with cleaning logic"
- "Write dbt `fct_orders.sql` Gold mart with joins and aggregations"
- "Create PySpark job to transform Bronze → Silver for large datasets"

**Orchestration**:
- "Implement Prefect flow for Bronze → Silver → Gold pipeline"
- "Create Dagster assets with lineage for comparison"
- "Add Prefect sensors to trigger on Redpanda messages"

**ML**:
- "Write MLflow training script for delivery delay prediction"
- "Implement model serving endpoint with FastAPI wrapper"
- "Create batch inference job to score all orders in Gold layer"

**BI**:
- "Build Streamlit Agentic BI app with Claude API for SQL generation"
- "Implement real-time dashboard showing live orders + ML predictions"
- "Create Evidence.dev markdown reports for executive dashboard"

---

## Important Constraints

**Iceberg**:
- Bronze tables: append-only, never update/delete
- Silver tables: incremental merge with unique_key
- Gold tables: full refresh or incremental with partitioning
- Always use PyIceberg for Python CRUD, never raw Parquet writes

**dbt**:
- Use `{{ source('bronze', 'ecom_orders') }}` to reference Bronze
- Use `{{ ref('stg_orders') }}` for model dependencies
- Test every model: `unique`, `not_null`, `relationships`
- Document all columns in `schema.yml`

**Prefect**:
- Always use `@task(retries=3)` for idempotency
- Use `task.submit()` for parallel execution
- Deploy flows to Prefect Cloud for production

**Redpanda**:
- Topics: `olist.orders`, `olist.reviews`, `olist.leads`, `olist.deals`
- Use `order_id` or `mql_id` as Kafka key for partition ordering
- Consumer group: `iceberg-bronze-writer`

---

## Success Criteria

✅ Bronze layer receives both batch and streaming data with ACID guarantees  
✅ dbt transforms Bronze → Silver → Gold with incremental strategies  
✅ Prefect orchestrates entire pipeline with lineage visible in UI  
✅ Trino can join Iceberg + external sources in single query  
✅ MLflow tracks experiments and serves model via REST API  
✅ Agentic BI generates correct SQL from natural language  
✅ All code runs in Docker Compose with `docker compose up`  
✅ GitHub Actions CI/CD runs dbt test on every PR  

---

## Next Steps

1. Start with infrastructure: `iceberg_setup.py` + `docker-compose.yml`
2. Implement ingestion: batch + streaming producers
3. Build dbt models: Silver cleaning → Gold marts
4. Add Prefect orchestration
5. Integrate Trino for federation
6. Train MLflow models
7. Create BI dashboards

**All code should be production-ready**: error handling, logging, retries, tests, documentation.
