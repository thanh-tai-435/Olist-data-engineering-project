# Olist Data Lakehouse Platform

An end-to-end **enterprise-grade data lakehouse** built on the Olist Brazilian E-Commerce dataset (~100K orders). Implements the full modern data stack: Lambda architecture, Medallion layers on Apache Iceberg, ML feature pipelines, LLM-powered Agentic BI, and data lineage — all running locally via Docker Compose.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                 │
│   CSV files (Olist e-commerce + marketing funnel)  │  Synthetic stream│
└──────────────────┬──────────────────────────────────────────────────┘
                   │ batch                              │ streaming
                   ▼                                    ▼
┌──────────────────────┐                  ┌─────────────────────────┐
│   Bronze Ingestion   │                  │  Redpanda (Kafka-compat) │
│  PyIceberg append    │                  │  olist.orders / reviews  │
│  10 Iceberg tables   │                  │  → Bronze Iceberg        │
└──────────┬───────────┘                  └────────────┬────────────┘
           │                                           │
           └──────────────────┬────────────────────────┘
                              │  Prefect orchestration
                              ▼
           ┌──────────────────────────────────────┐
           │         SILVER LAYER (PySpark)        │
           │  stg_orders, stg_sellers, stg_products│
           │  stg_customers, stg_order_items, ...  │
           │  int_orders_enriched  (10 tables)     │
           │  Iceberg MERGE INTO (incremental)     │
           └──────────────────┬───────────────────┘
                              │
                    ┌─────────┼─────────┐
                    │ ML      │ Quality  │ Gold
                    ▼         ▼          ▼
           ┌──────────────┐  Soda  ┌──────────────────────┐
           │Feature Pipelines│ Core │  GOLD LAYER (PySpark)│
           │Silver → XGBoost│      │  fct_orders (99K)    │
           │delivery, churn,│      │  fct_funnel  (8K)    │
           │lead scoring    │      │  dim_sellers (3K)    │
           │BERTimbau NLP   │      │  dim_customers (96K) │
           └──────┬─────────┘      │  review_sentiment    │
                  │                └──────────┬───────────┘
           ┌──────▼────────┐                  │
           │  MLflow       │          ┌───────▼──────────────────┐
           │  Experiment   │          │   SERVING LAYER           │
           │  Tracking +   │          │  Streamlit Agentic BI     │
           │  Model Reg.   │          │  Claude API → DuckDB SQL  │
           │  FastAPI      │          │  Realtime Monitor         │
           │  /predict/*   │          │  ML Predictions page      │
           └───────────────┘          └──────────────────────────┘
```

**Storage**: Cloudflare R2 (S3-compatible, free egress) · Apache Iceberg (ACID, time travel)  
**Orchestration**: Prefect 3 (flows, tasks, quality gates, notifications)  
**Lineage**: OpenLineage (Spark listener) → Marquez (store + Web UI)

---

## Stack

| Layer | Technology | Purpose |
|---|---|---|
| Storage | Cloudflare R2 + Apache Iceberg | S3-compatible object store; ACID transactions, time travel, schema evolution |
| Streaming | Redpanda (Kafka-compatible) | Single-container broker; `olist.orders`, `olist.reviews`, `olist.leads` topics |
| Ingestion | Python + PyIceberg | Batch CSV → Bronze; Streaming consumer → Bronze append-only |
| Transform | PySpark 3.5 | Bronze → Silver (MERGE INTO), Silver → Gold (aggregations + partitioning) |
| Quality | Soda Core | Automated checks on Bronze / Silver / Gold after each pipeline run |
| Orchestration | Prefect 3 | `@flow` / `@task(retries=3)`, quality gates, failure webhooks, daily schedule |
| Query Federation | Trino 435 | Join Iceberg + Postgres + CSV without ETL (`--profile query`) |
| ML Training | XGBoost + scikit-learn + imblearn | Delivery delay (regression), churn (classifier), lead scoring (classifier) |
| NLP | BERTimbau (PyTorch) | Portuguese review sentiment, 5-aspect scoring |
| ML Serving | FastAPI + MLflow | `/predict/delivery-delay`, `/predict/churn`, `/predict/lead` + batch endpoints |
| Experiment Tracking | MLflow 2.13 | Experiment runs, metric logging, model registry, model versioning |
| BI | Streamlit + Claude API | Natural language → intent → DuckDB SQL → chart + insight |
| Lineage | OpenLineage + Marquez | Auto-emitted from Spark jobs; full Bronze → Silver → Gold DAG |
| DevOps | Docker Compose + GitHub Actions | 20+ container services, profiles; CI: lint + syntax + compose + unit tests |
| Remote Access | Cloudflare Tunnel | Zero-config HTTPS tunnels; no port forwarding needed |

---

## Medallion Architecture on Iceberg

```
r2://olist-lakehouse/
├── bronze/               # append-only, immutable, PyIceberg writes
│   ├── ecom/             # orders, order_items, products, sellers,
│   │   └── ...           # customers, payments, reviews, geolocation
│   └── marketing/        # leads, deals
├── silver/               # PySpark MERGE INTO, incremental
│   ├── stg_orders/       # cleaned types, derived fields
│   ├── stg_sellers/
│   ├── int_orders_enriched/   # 6-table join
│   └── ...               # 10 tables total
└── gold/                 # PySpark aggregations, partitioned by date
    ├── fct_orders/        # 99,441 rows — BI-ready order facts
    ├── fct_funnel/        # 8,000 rows — lead-to-deal conversion
    ├── dim_sellers/       # 3,095 rows — seller metrics
    ├── dim_customers/     # 96,096 rows — customer lifetime metrics
    └── review_sentiment/  # 98,673 rows — BERTimbau scores per review
```

---

## ML Models

| Model | Task | Data Source | Key Features | Metric |
|---|---|---|---|---|
| `delivery-delay-xgb` | Regression (days late) | Silver 6-table join | `seller_customer_same_state`, `total_weight_g`, `order_freight`, `avg_product_volume_cm3` | MAE ~5.8 days |
| `customer-churn-xgb` | Binary classifier | Silver temporal split | Rolling windows 30/90/180d, `avg_days_between_orders`, `total_spend` | AUC ~0.54 (sparse label window) |
| `lead-scoring-xgb` | Binary classifier | Silver marketing tables | `origin`, `first_contact_month`, `first_contact_dayofweek` | AUC ~0.67 |
| `review-sentiment-bertimbau` | Multi-label NLP | Silver `stg_order_reviews` | Portuguese BERTimbau, 5 aspects: product quality, delivery speed, seller service, price/value, overall | — |

All classifiers use SMOTE for imbalance handling (imblearn Pipeline, `scale_pos_weight=1`).  
Features read from Silver (not Gold) to enable rolling windows and cross-table joins.

---

## Quick Start

### Prerequisites

- Docker Desktop (16 GB RAM recommended)
- `.env` file with credentials (see `.env.example`)

```bash
# 1. Clone and configure
git clone <repo>
cp .env.example .env
# Fill in: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT (Cloudflare R2)
#          ANTHROPIC_API_KEY, PREFECT_API_KEY (optional for Cloud)

# 2. Start core services
docker compose up -d

# 3. Ingest raw data into Bronze
docker exec olist-prefect-worker python scripts/batch_ingest_bronze.py

# 4. Run Silver transform
docker exec olist-prefect-worker python spark/jobs/silver_transform.py

# 5. Run Gold transform
docker exec olist-prefect-worker python spark/jobs/gold_transform.py

# 6. Train ML models
docker exec olist-prefect-worker python ml/training/train_delivery_model.py
docker exec olist-prefect-worker python ml/training/train_churn_model.py
docker exec olist-prefect-worker python ml/training/train_lead_scoring.py

# 7. Open Streamlit BI
open http://localhost:8501
```

### Docker Compose Profiles

```bash
docker compose up -d                    # core: postgres, iceberg, redpanda, prefect, mlflow, streamlit
docker compose --profile spark up -d   # + spark-master, spark-worker (cluster mode)
docker compose --profile query up -d   # + trino (query federation)
docker compose --profile lineage up -d # + marquez, marquez-web (data lineage)
```

---

## Service URLs

| Service | URL | Purpose |
|---|---|---|
| Streamlit BI | http://localhost:8501 | Agentic BI + Realtime Monitor + ML Predictions |
| Prefect UI | http://localhost:4200 | Flow runs, schedules, task logs |
| MLflow | http://localhost:5000 | Experiments, model registry |
| ML Serving API | http://localhost:8090/docs | FastAPI prediction endpoints |
| Redpanda Console | http://localhost:8080 | Kafka topics, consumer groups |
| Marquez UI | http://localhost:5003 | Data lineage graph (Bronze→Silver→Gold) |
| Iceberg REST | http://localhost:8181 | Catalog API |

---

## Prefect Flows

```
prefect/flows/
├── bronze_ingestion.py   # CSV → Bronze Iceberg (batch)
├── spark_transforms.py   # Silver transform + Gold transform tasks
├── full_pipeline.py      # Bronze → Silver → Quality gate → Gold → Quality gate
├── ml_training.py        # Train / retrain all 3 XGBoost models
├── quality_checks.py     # Soda Core checks per layer, pause-on-fail
├── sentiment_flow.py     # Score new reviews with BERTimbau → Gold
└── notifications.py      # Webhook notifications on flow failure
```

Run the full pipeline:

```bash
docker exec olist-prefect-worker python prefect/flows/full_pipeline.py
```

With daily schedule:

```bash
docker exec olist-prefect-worker python prefect/flows/full_pipeline.py --serve
```

---

## Agentic BI

The BI layer uses **Claude claude-sonnet-4-6 with Tool Use** (multi-step agentic loop):

1. User types a natural language question in Streamlit
2. Claude classifies intent: `DATA_QUERY` / `FOLLOWUP` / `SMALLTALK`
3. For data queries: Claude calls the `query_database` tool with SQL + chart type
4. Tool executes SQL against DuckDB in-memory views of Gold Iceberg tables
5. Claude receives the result, generates a business insight summary
6. Streamlit renders the dataframe + Plotly chart + insight card

Supports multi-query: Claude can call the tool multiple times for complex comparisons.

```
Example: "Phân tích toàn diện top 5 seller: doanh thu, review score, tỷ lệ giao trễ"
→ Query 1: top 5 sellers by revenue
→ Query 2: review scores for those sellers
→ Query 3: delivery delay rate per seller
→ Summary: consolidated insight with rankings
```

---

## Data Quality

Soda Core checks run automatically after each pipeline layer:

```
quality/checks/
├── bronze_checks.yml   # row count, null rate, pk uniqueness
├── silver_checks.yml   # type validation, referential integrity
└── gold_checks.yml     # revenue aggregation bounds, KPI sanity
```

---

## Data Lineage

OpenLineage emits events from Spark jobs automatically (no code changes needed).  
View the Bronze → Silver → Gold DAG in Marquez: http://localhost:5003

```
olist_silver_transform → stg_orders, stg_sellers, ..., int_orders_enriched
olist_gold_transform   → fct_orders, fct_funnel, dim_sellers, dim_customers
```

---

## CI/CD (GitHub Actions)

`.github/workflows/ci.yml` runs on every push and pull request to `main`:

| Job | What it checks |
|---|---|
| **Lint** | `ruff check` + `ruff format --check` across all Python files |
| **Syntax** | `py_compile` on every `.py` — catches import errors without needing deps |
| **Validate Compose** | `docker compose config` — validates all service definitions |
| **Unit Tests** | `pytest tests/` — pipeline logic, Soda check schemas, compose config |

---

## Project Structure

```
olist-lakehouse/
├── docker-compose.yml
├── .env.example
├── streaming/              # producer.py (replay), consumer.py (→ Bronze)
├── quality/checks/         # Soda Core YAML checks per layer
├── spark/jobs/             # silver_transform.py, gold_transform.py
├── prefect/flows/          # orchestration flows
├── ml/
│   ├── training/           # features.py, train_*.py, utils.py
│   ├── serving/            # FastAPI app.py
│   └── sentiment/          # BERTimbau model, inference, drift detection
├── bi/
│   ├── agentic_bi.py       # Claude Tool Use → DuckDB SQL
│   ├── agent.py            # LLM provider abstraction
│   ├── database.py         # PyIceberg → DuckDB in-memory views
│   └── pages/
│       ├── 1_Realtime_Monitor.py
│       ├── 2_Sentiment_Analysis.py
│       └── 3_ML_Predictions.py
├── tests/                  # pytest unit tests
└── .github/workflows/ci.yml
```

---

## Dataset

**Olist Brazilian E-Commerce** (Kaggle): ~100K orders, 2016–2018  
- `orders`, `order_items`, `order_payments`, `order_reviews`
- `customers`, `sellers`, `products`, `geolocation`

**Olist Marketing Funnel**: 8,000 qualified leads  
- `marketing_qualified_leads`, `closed_deals`

Scale: ~200 MB raw → production-grade architecture designed for TB-scale workloads.
