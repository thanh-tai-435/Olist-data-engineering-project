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
- Quality: Soda Core

**Medallion Layers**:
- **Bronze**: Raw, append-only, immutable (PyIceberg writes)
- **Silver**: Cleaned, typed, joined (PySpark `spark/jobs/silver_transform.py`)
- **Gold**: Aggregated, partitioned, BI-ready (PySpark `spark/jobs/gold_transform.py`)

**Processing**:
- Apache Spark (Silver/Gold transforms — `spark/jobs/`)
- DuckDB (Soda Core quality checks, ad-hoc queries)

**Query & ML**:
- Trino/Presto (query federation across Iceberg + Postgres + CSV)
- MLflow (experiment tracking, model registry, serving)

**Orchestration**:
- Prefect (primary: @flow/@task, Cloud UI, real-time monitoring)

**Serving**:
- Streamlit + Claude API (Agentic BI: NL → SQL)
- Real-time dashboard (live orders + ML predictions)

**Lineage**:
- OpenLineage (Spark listener auto-emits Bronze→Silver→Gold events)
- Marquez (lineage store + Web UI, `--profile lineage`)

**DevOps**:
- Docker Compose (containerization, profiles: core/spark/query/bi/lineage)
- GitHub Actions (CI/CD: lint, syntax, compose validation, unit tests)

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
├── silver/                    # PySpark incremental merge
│   ├── stg_orders/
│   ├── stg_sellers/
│   └── int_orders_enriched/
└── gold/                      # PySpark aggregation, partitioned
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

**Why PySpark over dbt?**
- dbt removed — Silver/Gold transforms done entirely in PySpark
- PySpark reads/writes Iceberg natively via `spark-iceberg` extension
- Same engine handles both small and large-scale workloads
- No extra toolchain (dbt CLI, profiles.yml, adapters) to maintain

**Why Prefect over Dagster?**
- Cleaner Python decorators (`@flow`, `@task`)
- Prefect Cloud free tier (no self-host needed)
- Better real-time monitoring UI

**Why Trino?**
- Join Iceberg + external sources without ETL
- Ad-hoc analytics across silos
- Catalogs: Iceberg, Postgres, Hive, S3

---

## Spark Jobs Structure

```
spark/
├── jobs/
│   ├── silver_transform.py    # Bronze → Silver (staging + int_orders_enriched)
│   └── gold_transform.py      # Silver → Gold (fct_orders, fct_funnel, dim_*)
└── jars/
    └── openlineage-spark_2.12-1.17.0.jar
```

**silver_transform.py** runs with OpenLineage listener — auto-emits lineage to Marquez.  
**gold_transform.py** same. Both submit via `prefect-worker` using `spark-submit` internally.

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

@task(retries=3)
def transform_silver():
    import subprocess
    subprocess.run(["spark-submit", "spark/jobs/silver_transform.py"], check=True)

@task(retries=3)
def aggregate_gold():
    import subprocess
    subprocess.run(["spark-submit", "spark/jobs/gold_transform.py"], check=True)

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
   - Model: XGBoost Regressor (`ml/training/train_delivery_model.py`)

2. **Churn prediction**:
   - Features from `dim_customers`: `days_since_last_order`, `total_orders`, `avg_order_value`
   - Target: binary (will order again in next 90 days?)
   - Model: XGBoost Classifier (`ml/training/train_churn_model.py`)

3. **Lead scoring**:
   - Features from `fct_funnel`: `origin`, `business_segment`, `first_contact_date`
   - Target: probability of conversion (lead → deal)
   - Model: XGBoost Classifier (`ml/training/train_lead_scoring.py`)

**Serving API** (`ml/serving/app.py` — FastAPI):
```bash
curl -X POST http://localhost:8000/predict/delivery-delay \
  -H 'Content-Type: application/json' \
  -d '{"seller_state": "SP", "customer_state": "RJ", ...}'
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
  postgres:         # Iceberg catalog backend + Marquez DB
  iceberg-rest:     # Iceberg REST catalog (tabulario)
  redpanda:         # Kafka-compatible broker
  prefect-server:   # Prefect orchestration API
  prefect-worker:   # Executes flows (has Spark + Python deps)
  mlflow:           # ML experiment tracking
  ml-serving:       # FastAPI model serving
  trino:            # Query federation [--profile query]
  spark-master:     # Spark cluster [--profile spark]
  spark-worker:     # Spark worker [--profile spark]
  marquez:          # Lineage API [--profile lineage]
  marquez-web:      # Lineage UI  [--profile lineage]
```

---

## File Structure

```
olist-lakehouse/
├── docker-compose.yml
├── .env.example
├── pyproject.toml           # ruff + pytest config
├── data/                    # gitignore
├── streaming/
│   ├── producer.py          # Redpanda event replay
│   └── consumer.py          # Redpanda → Bronze Iceberg
├── quality/
│   ├── soda_runner.py
│   └── checks/
│       ├── bronze_checks.yml
│       ├── silver_checks.yml
│       └── gold_checks.yml
├── spark/
│   ├── jobs/
│   │   ├── silver_transform.py
│   │   └── gold_transform.py
│   └── jars/
├── prefect/
│   ├── flows/
│   │   ├── bronze_ingestion.py
│   │   ├── spark_transforms.py
│   │   ├── full_pipeline.py
│   │   ├── ml_training.py
│   │   ├── quality_checks.py
│   │   ├── sentiment_flow.py
│   │   └── notifications.py
│   ├── deployments/
│   └── blocks/
├── trino/
│   └── catalog/
│       ├── iceberg.properties
│       └── postgres.properties
├── ml/
│   ├── training/
│   │   ├── train_delivery_model.py
│   │   ├── train_churn_model.py
│   │   ├── train_lead_scoring.py
│   │   └── utils.py
│   ├── serving/
│   │   └── app.py
│   └── sentiment/
├── bi/
│   ├── agentic_bi.py
│   └── pages/
├── infra/
│   ├── postgres/init.sql
│   └── marquez/marquez.yml
├── tests/
│   ├── test_soda_checks.py
│   ├── test_compose.py
│   └── test_pipeline_logic.py
└── .github/
    └── workflows/
        └── ci.yml
```

---

## Common Prompts for Claude Code

**Ingestion**:
- "Implement `streaming/consumer.py` to consume from Redpanda and append to Bronze Iceberg"
- "Create Soda Core YAML checks for Bronze data quality"

**Transformation**:
- "Extend `spark/jobs/silver_transform.py` to add a new staging table"
- "Add partitioning to `spark/jobs/gold_transform.py` fct_orders"

**Orchestration**:
- "Implement Prefect flow for Bronze → Silver → Gold pipeline"
- "Add Prefect sensors to trigger on Redpanda messages"

**ML**:
- "Write MLflow training script for delivery delay prediction"
- "Implement model serving endpoint with FastAPI wrapper"
- "Create batch inference job to score all orders in Gold layer"

**BI**:
- "Build Streamlit Agentic BI app with Claude API for SQL generation"
- "Implement real-time dashboard showing live orders + ML predictions"

---

## Important Constraints

**Iceberg**:
- Bronze tables: append-only, never update/delete
- Silver tables: incremental merge (Spark `MERGE INTO`)
- Gold tables: full refresh or incremental with partitioning
- Always use PyIceberg for Python CRUD, never raw Parquet writes

**PySpark**:
- SparkSession configured with Iceberg + S3A + OpenLineage listener
- `local[2]` mode by default; `--profile spark` for cluster mode
- Silver/Gold jobs submit via `spark-submit` inside `prefect-worker`

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
✅ PySpark transforms Bronze → Silver → Gold with Iceberg MERGE  
✅ Prefect orchestrates entire pipeline with lineage visible in UI  
✅ Trino can join Iceberg + external sources in single query  
✅ MLflow tracks experiments and serves model via REST API  
✅ Agentic BI generates correct SQL from natural language  
✅ OpenLineage captures Bronze→Silver→Gold data lineage in Marquez  
✅ All code runs in Docker Compose with `docker compose up`  
✅ GitHub Actions CI/CD: lint + syntax + compose + unit tests on every PR  

---

## Next Steps

1. Run `python ml/training/train_delivery_model.py` to train and register ML models
2. Run `python ml/training/train_churn_model.py`
3. Run `python ml/training/train_lead_scoring.py`
4. Commit all untracked files (CI/CD, OpenLineage, quality fixes)
5. Test full pipeline: `docker compose up` → trigger Prefect flow

**All code should be production-ready**: error handling, logging, retries, tests.
