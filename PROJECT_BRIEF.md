# Olist Data Lakehouse Platform – Project Brief

## Executive Summary

Xây dựng enterprise-grade data lakehouse platform cho 2 datasets Olist từ Kaggle:
- Brazilian E-Commerce (~100K orders, 8 tables)
- Marketing Funnel (leads → deals, 2 tables)

**Kiến trúc**: Lambda (batch + streaming) → Medallion on Apache Iceberg → Query federation + ML → Serving  
**Mục tiêu**: Simulate production-grade system với dataset nhỏ (~200MB) nhưng architecture scale được đến TB-PB  
**Công nghệ**: Modern data stack với Iceberg, Redpanda, dbt, Spark, Trino, Prefect, MLflow

---

## Business Context

**Olist** là marketplace platform kết nối sellers nhỏ với customers lớn ở Brazil. Platform cần:
- **Real-time monitoring**: track orders, delivery SLA, reviews ngay khi xảy ra
- **Historical analytics**: revenue trends, seller performance, customer behavior
- **Predictive capabilities**: dự đoán delivery delay, churn, lead conversion
- **Data quality**: validate data trước khi vào warehouse
- **Scalability**: architecture phải scale khi data tăng từ GB → TB

---

## Datasets

### Brazilian E-Commerce (8 tables, ~100K orders)
- `olist_orders_dataset.csv` — order lifecycle (purchase → approved → delivered)
- `olist_order_items_dataset.csv` — items trong order, seller_id, price
- `olist_order_payments_dataset.csv` — payment methods, installments
- `olist_order_reviews_dataset.csv` — review scores, comments
- `olist_products_dataset.csv` — product catalog, categories, dimensions
- `olist_sellers_dataset.csv` — seller location (city, state)
- `olist_customers_dataset.csv` — customer location
- `olist_geolocation_dataset.csv` — lat/lng cho enrichment

**Join key**: `order_id`, `product_id`, `seller_id`, `customer_id`

### Marketing Funnel (2 tables)
- `olist_marketing_qualified_leads_dataset.csv` — leads captured, origin, first_contact_date
- `olist_closed_deals_dataset.csv` — deals won, seller_id, business_segment, won_date

**Join key**: `mql_id` (marketing qualified lead ID), `seller_id` (join với e-commerce)

---

## Architecture Layers

### 1. Ingestion Layer

**Batch Path** (historical data):
- Read CSV từ Kaggle
- Validate với Soda Core (schema, nulls, anomalies)
- Convert to Parquet
- Upload lên Cloudflare R2
- Write vào Bronze Iceberg tables (PyIceberg)

**Streaming Path** (simulate real-time):
- Python producer đọc CSV
- Sort events by timestamp
- Replay với tốc độ nén (1 ngày = 1 giây, SPEED_FACTOR=86400)
- Publish vào Redpanda topics: `olist.orders`, `olist.reviews`, `olist.leads`, `olist.deals`
- Stream consumer batch-write vào Bronze Iceberg (ACID)

**Tools**: Python + pandas, confluent-kafka, PyIceberg, Redpanda, Soda Core

---

### 2. Storage Layer – Medallion Architecture

**Bronze** (raw, immutable):
- Append-only Iceberg tables
- Lưu đúng format gốc từ ingestion
- Snapshots cho time travel
- Partition minimal (chỉ `ingestion_date` nếu cần)

**Silver** (cleaned, typed):
- dbt incremental models
- Type casting, deduplication, null handling
- Join seller_id giữa e-commerce và marketing
- Validation với dbt tests

**Gold** (business-ready):
- dbt marts: `fct_orders`, `fct_funnel`, `dim_sellers`, `dim_customers`
- Aggregated metrics: revenue, conversion rate, SLA compliance
- Partitioned by business date (hidden partitioning)
- BI-ready tables

**Storage**: Cloudflare R2 (S3-compatible, free egress)  
**Format**: Apache Iceberg (ACID, time travel, schema evolution)  
**Tools**: PyIceberg, dbt, DuckDB, Apache Spark

---

### 3. Processing Layer

**Small/Medium Workloads (<1TB)**:
- DuckDB với Iceberg extension
- dbt-duckdb adapter
- Fast, embedded, single-node
- Query trực tiếp Iceberg tables trên R2

**Large-Scale Workloads (>1TB)**:
- Apache Spark với Iceberg connector
- PySpark jobs cho distributed transforms
- Parallel processing trên nhiều workers
- Same Iceberg tables, different engine

**Query Federation**:
- Trino/Presto query engine
- Join Iceberg + Postgres + CSV trong single query
- Catalog: Iceberg, Hive, PostgreSQL, S3
- Ad-hoc analytics without ETL

**Tools**: dbt, DuckDB, Apache Spark, Trino

---

### 4. Orchestration Layer

**Primary: Prefect**
- Python decorators: `@flow`, `@task`
- Prefect Cloud UI (free tier)
- Real-time monitoring
- Automatic retries, dynamic workflows
- Sensors trigger on Redpanda messages

**Alternative: Dagster**
- `@asset` dependencies
- Lineage graph UI
- Partitioned runs
- Asset sensors

**Workflow**: Bronze ingestion → Silver transform → Gold aggregate → ML inference → BI refresh

**Tools**: Prefect, Dagster

---

### 5. ML Layer

**Use Cases**:
1. **Delivery delay prediction**: train trên `fct_orders`, features: distance, weight, seller state
2. **Churn prediction**: predict customer repeat purchase probability
3. **Lead scoring**: marketing funnel conversion probability

**MLflow Components**:
- **Tracking**: log experiments, params, metrics
- **Registry**: version models, stage promotion (dev → prod)
- **Serving**: deploy model as REST API endpoint

**Integration**:
- Train models từ Gold tables
- Serve predictions qua API
- Display trong real-time dashboard

**Tools**: MLflow, scikit-learn, XGBoost

---

### 6. Serving Layer

**Historical BI**:
- Evidence.dev (BI-as-code, Markdown + SQL)
- Metabase (open-source BI)
- Query Gold layer qua Trino hoặc DuckDB
- Dashboards: revenue trend, delivery SLA, seller funnel

**Agentic BI**:
- Streamlit UI
- Claude API: natural language → SQL
- Execute SQL trên Gold Iceberg tables
- Interactive charts với plotly

**Real-time Dashboard**:
- Streamlit app
- Query Bronze/Silver latest data
- Display live orders, SLA alerts
- ML predictions từ MLflow API

**Tools**: Evidence, Metabase, Streamlit, Claude API

---

## Technical Stack Summary

| Layer | Tools | Rationale |
|-------|-------|-----------|
| **Storage** | Cloudflare R2, Apache Iceberg, PyIceberg | ACID, time travel, schema evolution, S3-compatible với free egress |
| **Ingestion** | Python, pandas, Redpanda, Soda Core | Lightweight Kafka alternative, built-in data quality |
| **Processing** | dbt, DuckDB, Apache Spark | dbt cho SQL transforms, DuckDB cho small data, Spark cho scale |
| **Query** | Trino/Presto | Federation across Iceberg + external sources |
| **Orchestration** | Prefect, Dagster | Modern Python workflows, cloud UI, automatic retries |
| **ML** | MLflow | Full lifecycle: tracking → registry → serving |
| **BI** | Evidence, Metabase, Streamlit | Historical + real-time + agentic |
| **DevOps** | Docker Compose, GitHub Actions | Containerization, CI/CD với dbt test |

---

## Key Design Decisions

### Why Medallion Architecture?
- **Bronze**: immutable source of truth, có thể reprocess downstream bất cứ lúc nào
- **Silver**: separation of concerns — cleaning tách biệt khỏi business logic
- **Gold**: BI-ready, không cần transform thêm khi query

### Why Apache Iceberg?
- **ACID**: batch và streaming ghi đồng thời vào Bronze mà không conflict
- **Time travel**: audit, rollback, debug với `VERSION AS OF`
- **Schema evolution**: thêm cột không cần rebuild table
- **Hidden partitioning**: user không cần specify partition trong query

### Why Redpanda over Kafka?
- Single container (no JVM, no Zookeeper)
- Kafka-compatible API (same Python code)
- 500MB RAM vs Kafka 2GB+
- Perfect cho development và dataset nhỏ

### Why both DuckDB and Spark?
- **DuckDB**: development, iteration, small/medium data (<1TB)
- **Spark**: production, distributed, scale to PB
- Same Iceberg tables — swap engine khi cần scale

### Why Prefect over Dagster?
- Cleaner Python API (`@flow` vs `@asset`)
- Prefect Cloud free tier
- Better real-time monitoring
- Dagster vẫn được implement để so sánh

### Why Trino?
- Join Iceberg + Postgres + S3 không cần ETL
- Ad-hoc analytics across silos
- Query pushdown optimization

---

## Implementation Phases

### Phase 1: Infrastructure Setup
- [ ] Docker Compose với all services
- [ ] Cloudflare R2 bucket setup
- [ ] PyIceberg REST catalog configuration
- [ ] Redpanda topics creation

### Phase 2: Bronze Layer
- [ ] Batch producer: CSV → Parquet → R2 → Iceberg
- [ ] Streaming producer: CSV replay → Redpanda
- [ ] Stream consumer: Redpanda → Bronze Iceberg
- [ ] Soda Core data quality checks

### Phase 3: Silver Layer
- [ ] dbt project setup
- [ ] Silver models: stg_orders, stg_sellers, stg_funnel
- [ ] Intermediate models: joins, enrichments
- [ ] dbt tests: unique, not_null, relationships

### Phase 4: Gold Layer
- [ ] Gold marts: fct_orders, fct_funnel
- [ ] Dimensions: dim_sellers, dim_customers (SCD Type 2)
- [ ] Metrics: revenue_by_month, delivery_sla, conversion_rate

### Phase 5: Orchestration
- [ ] Prefect flows: Bronze → Silver → Gold
- [ ] Sensors: trigger on Redpanda messages
- [ ] Schedules: daily refresh at 2 AM
- [ ] Deploy to Prefect Cloud

### Phase 6: Query Federation
- [ ] Trino catalog configs
- [ ] Test queries joining Iceberg + Postgres
- [ ] Ad-hoc analytics examples

### Phase 7: ML Layer
- [ ] MLflow setup
- [ ] Train delivery delay model
- [ ] Model registry & versioning
- [ ] REST API serving

### Phase 8: BI & Dashboards
- [ ] Evidence/Metabase historical dashboards
- [ ] Streamlit Agentic BI app
- [ ] Real-time dashboard với ML predictions

### Phase 9: CI/CD
- [ ] GitHub Actions: dbt test on PR
- [ ] Pre-commit hooks: SQL linting
- [ ] Automated deployments

---

## Success Metrics

**Technical**:
- ✅ Bronze layer ingests both batch and streaming với <1s latency
- ✅ dbt transformations complete trong <5 minutes
- ✅ Iceberg snapshots allow time travel queries
- ✅ Trino queries join Iceberg + Postgres successfully
- ✅ MLflow model serving responds <100ms
- ✅ All services run in Docker Compose

**Business**:
- ✅ Dashboard shows revenue trends by month/region
- ✅ Delivery SLA compliance tracked real-time
- ✅ Marketing funnel conversion rate calculated
- ✅ Delivery delay predictions có R² > 0.7
- ✅ Agentic BI correctly translates NL to SQL

**Quality**:
- ✅ All dbt models have tests (unique, not_null)
- ✅ Soda checks pass 100% on Bronze data
- ✅ GitHub Actions CI green on every commit
- ✅ Documentation complete (dbt docs, README)

---

## Deliverables

1. **Code Repository**:
   - `docker-compose.yml` với all services configured
   - `ingestion/` với batch + streaming producers
   - `dbt/` với Bronze/Silver/Gold models
   - `prefect/` với orchestration flows
   - `ml/` với MLflow training scripts
   - `bi/` với Streamlit apps

2. **Documentation**:
   - README.md với setup instructions
   - dbt docs hosted (schema, lineage, tests)
   - Architecture diagrams
   - API documentation (MLflow endpoints)

3. **Dashboards**:
   - Evidence/Metabase historical reports
   - Streamlit Agentic BI app
   - Real-time monitoring dashboard

4. **Presentation**:
   - Demo video (5-10 phút)
   - Slides giải thích architecture decisions
   - Performance benchmarks (DuckDB vs Spark)

---

## Timeline Estimate

**Week 1-2**: Infrastructure + Bronze layer  
**Week 3-4**: Silver + Gold transformations  
**Week 5**: Orchestration (Prefect)  
**Week 6**: Query federation (Trino) + ML (MLflow)  
**Week 7**: BI dashboards + Agentic BI  
**Week 8**: Testing, documentation, polish  

**Total**: 8 weeks for full implementation

---

## Risk Mitigation

**Risk**: Iceberg complexity quá cao  
**Mitigation**: Start với DuckDB Parquet, migrate sang Iceberg sau khi hiểu rõ

**Risk**: Spark setup cồng kềnh  
**Mitigation**: DuckDB làm primary, Spark optional để demo scalability

**Risk**: Trino thêm overhead  
**Mitigation**: Implement sau cùng, không phải critical path

**Risk**: MLflow model training lâu  
**Mitigation**: Start với simple models (RandomForest), không cần deep learning

**Risk**: Time constraint  
**Mitigation**: Prioritize: Bronze → Silver → Gold → Prefect → BI. Spark/Trino/MLflow là nice-to-have

---

## Resources

**Datasets**:
- https://kaggle.com/datasets/olistbr/brazilian-ecommerce
- https://www.kaggle.com/olistbr/marketing-funnel-olist

**Documentation**:
- Iceberg: https://iceberg.apache.org/docs/latest/
- dbt: https://docs.getdbt.com/
- Prefect: https://docs.prefect.io/
- Trino: https://trino.io/docs/current/

**Examples**:
- dbt Medallion: https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview
- PyIceberg: https://py.iceberg.apache.org/
- Redpanda Docker: https://docs.redpanda.com/current/get-started/quick-start/

---

## Contact & Support

**Questions about**:
- Iceberg setup → Check PyIceberg docs
- dbt models → dbt Slack community
- Prefect flows → Prefect Slack
- MLflow → MLflow GitHub discussions

**Demo day**: Present architecture, live demo, Q&A

---

**Last updated**: 2025-05-08  
**Version**: 1.0 (with Spark, Trino, Prefect, MLflow)
