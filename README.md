# Olist Data Lakehouse Platform

Nền tảng **data lakehouse cấp doanh nghiệp** xây dựng trên bộ dữ liệu thương mại điện tử Olist Brazil (~100K đơn hàng). Triển khai đầy đủ modern data stack: kiến trúc Lambda, Medallion Layers trên Apache Iceberg, ML feature pipelines, Agentic BI tích hợp LLM, và data lineage — tất cả chạy local qua Docker Compose.

---

## Nhóm thực hiện

| STT | Họ và tên | Vai trò |
|---|---|---|
| 1 | Phạm Trần Thanh Tài | Data Engineering, Data Quality & Lineage |
| 2 | Đỗ Phúc Khang | BI & Serving Layer |
| 3 | Nguyễn Nhật Huy | Machine Learning |
| 4 | Thái Khương Anh Đức | Streaming & Orchestration |

---

## Kiến trúc tổng thể

```
┌─────────────────────────────────────────────────────────────────────┐
│                         NGUỒN DỮ LIỆU                               │
│   File CSV (Olist e-commerce + marketing funnel)  │  Luồng tổng hợp │
└──────────────────┬──────────────────────────────────────────────────┘
                   │ batch                              │ streaming
                   ▼                                    ▼
┌──────────────────────┐                  ┌─────────────────────────┐
│   Nạp Bronze (Batch) │                  │  Redpanda (Kafka-compat) │
│  PyIceberg append    │                  │  olist.orders / reviews  │
│  10 bảng Iceberg     │                  │  → Bronze Iceberg        │
└──────────┬───────────┘                  └────────────┬────────────┘
           │                                           │
           └──────────────────┬────────────────────────┘
                              │  Điều phối bởi Prefect
                              ▼
           ┌──────────────────────────────────────┐
           │        LỚP SILVER (PySpark)           │
           │  stg_orders, stg_sellers, stg_products│
           │  stg_customers, stg_order_items, ...  │
           │  int_orders_enriched  (10 bảng)       │
           │  Iceberg MERGE INTO (tăng dần)        │
           └──────────────────┬───────────────────┘
                              │
                    ┌─────────┼─────────┐
                    │ ML      │ Quality  │ Gold
                    ▼         ▼          ▼
           ┌──────────────┐  Soda  ┌──────────────────────┐
           │Feature Pipelines│ Core │  LỚP GOLD (PySpark)  │
           │Silver → XGBoost│      │  fct_orders (99K)    │
           │delivery, churn,│      │  fct_funnel  (8K)    │
           │lead scoring    │      │  dim_sellers (3K)    │
           │BERTimbau NLP   │      │  dim_customers (96K) │
           └──────┬─────────┘      │  review_sentiment    │
                  │                └──────────┬───────────┘
           ┌──────▼────────┐                  │
           │  MLflow       │          ┌───────▼──────────────────┐
           │  Theo dõi     │          │   LỚP PHỤC VỤ (SERVING)  │
           │  thí nghiệm + │          │  Streamlit Agentic BI     │
           │  Model Reg.   │          │  Claude API → DuckDB SQL  │
           │  FastAPI      │          │  Realtime Monitor         │
           │  /predict/*   │          │  ML Predictions page      │
           └───────────────┘          └──────────────────────────┘
```

**Lưu trữ**: Cloudflare R2 (S3-compatible, miễn phí egress) · Apache Iceberg (ACID, time travel)  
**Điều phối**: Prefect 3 (flows, tasks, quality gates, notifications)  
**Lineage**: OpenLineage (Spark listener) → Marquez (store + Web UI)

---

## Công nghệ sử dụng

| Tầng | Công nghệ | Mục đích |
|---|---|---|
| Lưu trữ | Cloudflare R2 + Apache Iceberg | Object store S3-compatible; ACID, time travel, schema evolution |
| Streaming | Redpanda (Kafka-compatible) | Broker đơn container; topic `olist.orders`, `olist.reviews`, `olist.leads` |
| Nạp dữ liệu | Python + PyIceberg | Batch CSV → Bronze; Streaming consumer → Bronze append-only |
| Biến đổi | PySpark 3.5 | Bronze → Silver (MERGE INTO), Silver → Gold (tổng hợp + phân vùng) |
| Chất lượng | Soda Core | Kiểm tra tự động Bronze / Silver / Gold sau mỗi lần chạy |
| Điều phối | Prefect 3 | `@flow` / `@task(retries=3)`, quality gates, webhook thất bại, lịch hằng ngày |
| Query Federation | Trino 435 | Join Iceberg + Postgres + CSV không cần ETL (`--profile query`) |
| ML Training | XGBoost + scikit-learn + imblearn | Dự đoán giao trễ (regression), churn (classifier), lead scoring (classifier) |
| NLP | BERTimbau (PyTorch) | Phân tích cảm xúc review tiếng Bồ Đào Nha, 5 khía cạnh |
| ML Serving | FastAPI + MLflow | `/predict/delivery-delay`, `/predict/churn`, `/predict/lead` + batch endpoints |
| Theo dõi thí nghiệm | MLflow 2.13 | Runs, metric logging, model registry, model versioning |
| BI | Streamlit + Claude API | Ngôn ngữ tự nhiên → intent → DuckDB SQL → biểu đồ + insight |
| Lineage | OpenLineage + Marquez | Tự phát ra từ Spark jobs; DAG Bronze → Silver → Gold đầy đủ |
| DevOps | Docker Compose + GitHub Actions | 20+ dịch vụ container, profiles; CI: lint + syntax + compose + unit tests |
| Truy cập từ xa | Cloudflare Tunnel | HTTPS tunnel không cần cấu hình port forwarding |

---

## Medallion Architecture trên Iceberg

```
s3://retail-data-lake/
├── bronze/               # append-only, bất biến, PyIceberg ghi
│   ├── ecom/             # orders, order_items, products, sellers,
│   │   └── ...           # customers, payments, reviews, geolocation
│   └── marketing/        # leads, deals
├── silver/               # PySpark MERGE INTO, tăng dần
│   ├── stg_orders/       # kiểu dữ liệu sạch, trường dẫn xuất
│   ├── stg_sellers/
│   ├── int_orders_enriched/   # join 6 bảng
│   └── ...               # tổng 10 bảng
├── gold/                 # PySpark tổng hợp, phân vùng theo ngày
│   ├── fct_orders/        # 99.441 dòng — facts đơn hàng sẵn cho BI
│   ├── fct_funnel/        # 8.000 dòng — tỷ lệ chuyển đổi lead → deal
│   ├── dim_sellers/       # 3.095 dòng — chỉ số seller
│   ├── dim_customers/     # 96.096 dòng — chỉ số lifetime khách hàng
│   └── review_sentiment/  # 98.673 dòng — điểm BERTimbau (chạy sentiment_flow.py)
└── mlflow/               # MLflow artifacts (model files, metrics)
```

---

## Mô hình ML

| Mô hình | Bài toán | Nguồn dữ liệu | Feature chính | Chỉ số |
|---|---|---|---|---|
| `delivery-delay-xgb` | Hồi quy (ngày trễ) | Silver join 6 bảng | `seller_customer_same_state`, `total_weight_g`, `order_freight`, `avg_product_volume_cm3` | MAE ~5.8 ngày |
| `customer-churn-xgb` | Phân loại nhị phân | Silver temporal split | Rolling windows 30/90/180d, `avg_days_between_orders`, `total_spend` | AUC ~0.54 |
| `lead-scoring-xgb` | Phân loại nhị phân | Silver marketing tables | `origin`, `first_contact_month`, `first_contact_dayofweek` | AUC ~0.67 |
| `review-sentiment-bertimbau` | NLP đa nhãn | Silver `stg_order_reviews` | BERTimbau tiếng Bồ, 5 khía cạnh: chất lượng SP, tốc độ giao, dịch vụ seller, giá trị, tổng thể | — |

Tất cả classifier dùng SMOTE để xử lý mất cân bằng nhãn (imblearn Pipeline).

---

## Hướng dẫn chạy

### Yêu cầu hệ thống

- Docker Desktop (khuyến nghị **16 GB RAM**)
- File `.env` với thông tin xác thực (xem `.env.example`)
- Python 3.10+ (nếu chạy ngoài container)

### Bước 1 — Clone và cấu hình

```bash
git clone <repo-url>
cd olist-data-engineering-project

cp .env.example .env
# Điền vào .env:
#   AWS_ACCESS_KEY_ID        — R2 Access Key
#   AWS_SECRET_ACCESS_KEY    — R2 Secret Key
#   S3_ENDPOINT              — https://<account>.r2.cloudflarestorage.com
#   ANTHROPIC_API_KEY        — API key Claude (dùng cho Agentic BI)
#   PREFECT_API_KEY          — tuỳ chọn, nếu kết nối Prefect Cloud
```

### Bước 2 — Khởi động dịch vụ cốt lõi

```bash
# Khởi động: postgres, iceberg-rest, redpanda, prefect, mlflow, streamlit, ml-serving
docker compose up -d
```

> Chờ khoảng 60–90 giây để tất cả dịch vụ sẵn sàng. Kiểm tra bằng:
> ```bash
> docker compose ps
> ```

### Bước 3 — Nạp dữ liệu thô vào Bronze

```bash
docker exec olist-prefect-worker python scripts/batch_ingest_bronze.py
```

### Bước 4 — Biến đổi Silver và Gold

```bash
# Bronze → Silver (làm sạch, join, MERGE INTO)
docker exec olist-prefect-worker python spark/jobs/silver_transform.py

# Silver → Gold (tổng hợp, phân vùng theo ngày)
docker exec olist-prefect-worker python spark/jobs/gold_transform.py
```

### Bước 5 — Huấn luyện mô hình ML

```bash
docker exec olist-prefect-worker python ml/training/train_delivery_model.py
docker exec olist-prefect-worker python ml/training/train_churn_model.py
docker exec olist-prefect-worker python ml/training/train_lead_scoring.py
```

### Bước 6 — Mở Streamlit BI

Truy cập: [http://localhost:8501](http://localhost:8501)

---

## Các Profile cần bật theo nhu cầu

Ngoài core services, bật thêm profile tương ứng khi cần tính năng mở rộng:

| Profile | Lệnh bật | Dịch vụ được thêm | Khi nào cần |
|---|---|---|---|
| *(mặc định / core)* | `docker compose up -d` | postgres, iceberg-rest, redpanda, prefect-server, prefect-worker, mlflow, ml-serving, streamlit | Chạy pipeline cơ bản và BI |
| **spark** | `docker compose --profile spark up -d` | spark-master, spark-worker | Khi cần Spark cluster mode (Silver/Gold transform quy mô lớn) |
| **query** | `docker compose --profile query up -d` | trino | Khi cần query federation: join Iceberg + Postgres + CSV |
| **lineage** | `docker compose --profile lineage up -d` | marquez, marquez-web | Khi muốn xem DAG lineage Bronze→Silver→Gold |

**Bật nhiều profile cùng lúc:**

```bash
docker compose --profile spark --profile lineage up -d
```

---

## Chạy toàn bộ pipeline qua Prefect

```bash
# Chạy một lần: Bronze → Silver → Quality Gate → Gold → Quality Gate
docker exec olist-prefect-worker python prefect/flows/full_pipeline.py

# Chạy theo lịch hằng ngày (giữ tiến trình chạy nền)
docker exec olist-prefect-worker python prefect/flows/full_pipeline.py --serve
```

### Các flow riêng lẻ

```
prefect/flows/
├── bronze_ingestion.py   # CSV → Bronze Iceberg (batch)
├── spark_transforms.py   # Silver + Gold transform tasks
├── full_pipeline.py      # Pipeline đầy đủ Bronze → Silver → Gold
├── ml_training.py        # Train / retrain 3 mô hình XGBoost
├── quality_checks.py     # Soda Core checks từng lớp
├── sentiment_flow.py     # Chấm điểm review mới bằng BERTimbau → Gold
└── notifications.py      # Webhook thông báo khi flow thất bại
```

---

## URL các dịch vụ

| Dịch vụ | URL | Mô tả |
|---|---|---|
| Streamlit BI | http://localhost:8501 | Agentic BI + Realtime Monitor + ML Predictions |
| Prefect UI | http://localhost:4200 | Quản lý flow runs, lịch, logs |
| MLflow | http://localhost:5000 | Thí nghiệm, model registry |
| ML Serving API | http://localhost:8090/docs | FastAPI endpoints dự đoán |
| Redpanda Console | http://localhost:8080 | Kafka topics, consumer groups |
| Spark Master UI | http://localhost:8085 | Spark cluster status *(profile spark)* |
| Marquez UI | http://localhost:5003 | Đồ thị lineage Bronze→Silver→Gold *(profile lineage)* |
| Iceberg REST | http://localhost:8181 | Catalog API |

---

## Agentic BI

Tầng BI dùng **Claude Sonnet với Tool Use** (vòng lặp agentic đa bước):

1. Người dùng gõ câu hỏi ngôn ngữ tự nhiên trong Streamlit
2. Claude phân loại ý định: `DATA_QUERY` / `FOLLOWUP` / `SMALLTALK`
3. Với data query: Claude gọi tool `query_database` kèm SQL + loại biểu đồ
4. Tool thực thi SQL trên DuckDB in-memory views của bảng Gold Iceberg
5. Claude nhận kết quả, tạo tóm tắt insight nghiệp vụ
6. Streamlit hiển thị dataframe + biểu đồ Plotly + insight card

Hỗ trợ multi-query: Claude có thể gọi tool nhiều lần cho phân tích phức tạp.

```
Ví dụ: "Phân tích toàn diện top 5 seller: doanh thu, review score, tỷ lệ giao trễ"
→ Query 1: top 5 sellers theo doanh thu
→ Query 2: review scores của 5 sellers đó
→ Query 3: tỷ lệ giao trễ mỗi seller
→ Tổng kết: insight hợp nhất có xếp hạng
```

---

## Chất lượng dữ liệu

Soda Core tự động kiểm tra sau mỗi lớp pipeline:

```
quality/checks/
├── bronze_checks.yml   # row count, null rate, pk uniqueness
├── silver_checks.yml   # type validation, referential integrity
└── gold_checks.yml     # revenue aggregation bounds, KPI sanity
```

---

## Data Lineage

OpenLineage tự động phát sự kiện từ Spark jobs (không cần thay đổi code).  
Xem DAG Bronze → Silver → Gold tại Marquez: http://localhost:5003

```
olist_silver_transform → stg_orders, stg_sellers, ..., int_orders_enriched
olist_gold_transform   → fct_orders, fct_funnel, dim_sellers, dim_customers
```

---

## CI/CD (GitHub Actions)

`.github/workflows/ci.yml` chạy trên mỗi push và pull request lên `main`:

| Job | Kiểm tra gì |
|---|---|
| **Lint** | `ruff check` + `ruff format --check` toàn bộ Python |
| **Syntax** | `py_compile` mọi file `.py` — phát hiện lỗi import mà không cần cài deps |
| **Validate Compose** | `docker compose config` — xác thực định nghĩa tất cả services |
| **Unit Tests** | `pytest tests/` — pipeline logic, Soda check schemas, compose config |

---

## Cấu trúc dự án

```
olist-lakehouse/
├── docker-compose.yml
├── .env.example
├── streaming/              # producer.py (replay), consumer.py (→ Bronze)
├── quality/checks/         # Soda Core YAML checks từng lớp
├── spark/jobs/             # silver_transform.py, gold_transform.py
├── prefect/flows/          # các orchestration flow
├── ml/
│   ├── training/           # features.py, train_*.py, utils.py
│   ├── serving/            # FastAPI app.py
│   └── sentiment/          # mô hình BERTimbau, inference, drift detection
├── bi/
│   ├── agentic_bi.py       # Claude Tool Use → DuckDB SQL
│   ├── agent.py            # abstraction LLM provider
│   ├── database.py         # PyIceberg → DuckDB in-memory views
│   └── pages/
│       ├── 1_Realtime_Monitor.py
│       ├── 2_Sentiment_Analysis.py
│       └── 3_ML_Predictions.py
├── tests/                  # pytest unit tests
└── .github/workflows/ci.yml
```

---

## Bộ dữ liệu

**Olist Brazilian E-Commerce** (Kaggle): ~100K đơn hàng, 2016–2018
- `orders`, `order_items`, `order_payments`, `order_reviews`
- `customers`, `sellers`, `products`, `geolocation`

**Olist Marketing Funnel**: 8.000 qualified leads
- `marketing_qualified_leads`, `closed_deals`

Quy mô: ~200 MB raw → kiến trúc cấp sản xuất thiết kế cho workload TB-scale.
