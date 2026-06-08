# 1.2 Phạm vi công việc cá nhân

Đồ án được thực hiện theo nhóm, trong đó mỗi thành viên đảm nhận một lớp chức năng riêng biệt của hệ thống. Phần này mô tả cụ thể phạm vi công việc mà tác giả trực tiếp thiết kế, xây dựng và kiểm thử trong dự án.

---

## Phân công trong nhóm

| Thành viên | Phạm vi |
|------------|---------|
| **Tác giả (Phần C)** | Data Engineering — toàn bộ Data Platform layer: ingestion, transformation, orchestration, streaming, storage, môi trường triển khai |
| Thành viên khác | Intelligence Layer — Agentic BI (Natural Language → SQL via LLM), dashboard phân tích |

---

## Công việc tác giả thực hiện

### 1. Hạ tầng và môi trường triển khai

- Thiết kế và viết toàn bộ `docker-compose.yml` với kiến trúc profile-based: core stack (luôn chạy), profile `spark` (Spark cluster), profile `bi` (Streamlit), profile `coder` (cloud IDE), profile `query` (Trino)
- Cấu hình **Cloudflare R2** làm object storage backend cho toàn bộ Iceberg tables, thay thế AWS S3 để giảm chi phí egress
- Triển khai **Apache Iceberg REST Catalog** với PostgreSQL backend đảm bảo metadata bền vững qua các lần restart
- Thiết lập **Coder** — cloud IDE cho phép các thành viên nhóm truy cập môi trường phát triển thống nhất từ bất kỳ máy nào qua trình duyệt
- Xây dựng service `url-reporter` tự động theo dõi Cloudflare tunnel containers, cập nhật URL vào `.env` và Coder workspace mỗi khi tunnel restart

### 2. Bronze Layer — Batch Ingestion

- Viết `prefect/flows/bronze_ingestion.py`: flow Prefect đọc 10 bảng CSV từ `data/raw/`, validate kiểu dữ liệu (timestamp[us] thay vì ns để tương thích PyIceberg), tự động tạo Iceberg table nếu chưa tồn tại, append dữ liệu với metadata `_ingested_at` và `_source_file`
- Xử lý edge case: bảng geolocation (~1M dòng) với batch size kiểm soát bộ nhớ; timestamp columns parse với `errors="coerce"` tránh crash khi dữ liệu lỗi
- Tích hợp Prefect Artifacts để hiển thị kết quả ingestion (số bảng, số dòng) trực tiếp trên Prefect UI

### 3. Streaming Layer

- Viết `streaming/producer.py`: replay sự kiện lịch sử từ 3 CSV (orders, reviews, payments) vào Redpanda theo thứ tự thời gian thực. Hỗ trợ hai chế độ: `rate` (phát đều N events/giây cho demo dashboard) và `replay` (nén thời gian theo SPEED_FACTOR cho kiểm thử temporal ordering)
- Viết `streaming/consumer.py`: consume đồng thời 3 topics (`olist.orders`, `olist.reviews`, `olist.payments`), ghi vào Bronze Iceberg với cơ chế dual-flush (batch size + time-based timeout) đảm bảo dữ liệu không bị giữ trong buffer khi lưu lượng thấp

### 4. Silver và Gold Layer — PySpark Transformation

- Thiết kế và viết `spark/jobs/silver_transform.py`: transform 10 Bronze tables thành 10 staging tables (stg_*) + 1 intermediate join table (`int_orders_enriched`). Xử lý: chuẩn hóa kiểu dữ liệu, loại bỏ duplicates bằng window function `row_number()`, trim whitespace, cast numeric columns, tính `delivery_delay_days` từ timestamp thực tế vs ước tính
- Thiết kế và viết `spark/jobs/gold_transform.py`: xây dựng 4 Gold tables phục vụ analytics — `fct_orders` (partitioned by month), `fct_funnel` (partitioned by year), `dim_sellers` (aggregated metrics), `dim_customers` (CLV metrics)
- Cấu hình SparkSession với Iceberg REST catalog và 4 JAR dependencies (iceberg-spark-runtime, iceberg-aws-bundle, hadoop-aws, aws-java-sdk-bundle), hỗ trợ chuyển đổi linh hoạt giữa `local[2]` và `spark://spark-master:7077` qua biến môi trường `SPARK_MASTER` mà không thay đổi code

### 5. Orchestration — Prefect

- Viết 5 Prefect flows:
  - `bronze_ingestion_flow` — batch ingest với task-level retry và artifacts
  - `silver_transform_flow` — PySpark Bronze→Silver, tạo markdown artifact kết quả
  - `gold_transform_flow` — PySpark Silver→Gold, tạo markdown artifact kết quả
  - `ml_training_flow` — trigger training 3 XGBoost models sau khi Gold sẵn sàng
  - `full_pipeline_flow` — orchestrate toàn bộ chuỗi với dependency chaining, tham số `skip_bronze` và `full_refresh`
- Cấu hình Prefect Server và Worker chạy trong Docker, kết nối Cloudflare tunnel để truy cập UI từ ngoài mạng

### 6. MLflow — Model Lifecycle

- Viết 3 training scripts (`train_delivery_model.py`, `train_churn_model.py`, `train_lead_scoring.py`) sử dụng XGBoost + scikit-learn Pipeline, log metrics và artifacts vào MLflow
- Xử lý các vấn đề data quality trong ML: data leakage ở churn model (reference date tính từ dataset max thay vì current date), data leakage ở lead scoring (loại bỏ post-conversion features), cấu hình nội bộ container dùng `http://mlflow:5000` tránh DNS failure với tunnel URL
- Đăng ký 3 models vào MLflow Model Registry: `delivery-delay-xgb`, `customer-churn-xgb`, `lead-scoring-xgb`

### 7. Dashboard Realtime

- Viết `bi/realtime_dashboard.py`: Streamlit app đọc trực tiếp từ Redpanda topic `olist.orders` bằng confluent-kafka Consumer, buffer sự kiện trong `@st.cache_resource`, tự refresh mỗi 3 giây, hiển thị live metrics (total orders, revenue, top states, order timeline)

---

## Ranh giới với phần của thành viên khác

Lớp Intelligence (Agentic BI) được thành viên khác trong nhóm đảm nhận, bao gồm:
- Module `bi/agent.py` — SQL generation và self-correction via LLM
- Module `bi/validator.py` — 3-tier validation và intent classification
- Module `bi/charts.py` — chart selection và visualization
- Module `bi/database.py` — DuckDB + Iceberg connection cho query
- Module `bi/config.py` — centralized config
- `bi/agentic_bi.py` — Streamlit UI cho Agentic BI

Tác giả cung cấp dữ liệu đầu vào (Gold layer tables) và môi trường triển khai (Docker, tunnel) cho lớp này sử dụng, nhưng không tham gia xây dựng logic Intelligence.
