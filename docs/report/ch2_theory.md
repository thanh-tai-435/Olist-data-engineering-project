# CHƯƠNG 2. CƠ SỞ LÝ THUYẾT

## 2.1 Kiến trúc dữ liệu hiện đại

### 2.1.1 Data Warehouse

Data Warehouse (DWH) là hệ thống lưu trữ dữ liệu được tối ưu hóa cho analytical workload. Dữ liệu từ các hệ thống nguồn (OLTP databases, CRM, ERP) được extract, transform và load (ETL) vào DWH theo schema được định nghĩa trước (schema-on-write). Kiến trúc phổ biến bao gồm Star Schema hoặc Snowflake Schema với fact tables và dimension tables.

**Ưu điểm**: query performance cao nhờ columnar storage và pre-computed aggregation; ACID transactions đảm bảo tính nhất quán; schema enforcement giúp data governance.

**Hạn chế**: schema cứng nhắc — mỗi lần thêm column phải viết migration script và reload data; chi phí lưu trữ cao khi scale lên PB; không hỗ trợ tốt dữ liệu bán cấu trúc (JSON, nested arrays); khó tích hợp streaming data; vendor lock-in với các giải pháp thương mại (Snowflake, Redshift, BigQuery).

### 2.1.2 Data Lake

Data Lake là hệ thống lưu trữ tập trung toàn bộ dữ liệu của tổ chức ở dạng raw — có cấu trúc, bán cấu trúc, hoặc phi cấu trúc — trên object storage chi phí thấp (AWS S3, Azure ADLS, Cloudflare R2). Schema được áp dụng khi đọc (schema-on-read), không phải khi ghi.

**Ưu điểm**: lưu trữ linh hoạt, chi phí thấp; hỗ trợ mọi loại dữ liệu; decoupled storage và compute; phù hợp cho data science và ML workloads.

**Hạn chế**: thiếu ACID transactions dẫn đến partial writes và inconsistent reads khi có concurrent access; không có schema enforcement — dữ liệu dễ trở thành "data swamp"; không hỗ trợ row-level update/delete (vấn đề nghiêm trọng cho GDPR right-to-be-forgotten); query performance thấp hơn DWH do không có indexing.

### 2.1.3 Unified Lakehouse

Lakehouse là kiến trúc thống nhất kết hợp tính linh hoạt và chi phí thấp của Data Lake với tính nhất quán và performance của Data Warehouse. Điểm mấu chốt là lớp **metadata + transaction protocol** được thêm vào phía trên object storage, cung cấp:

- ACID transactions trên các file Parquet trong object storage
- Schema enforcement và evolution
- Time travel (query dữ liệu tại thời điểm lịch sử)
- Data versioning và audit trail
- Query performance tốt nhờ file pruning và statistics

Ba table format chính hiện nay implement Lakehouse paradigm: **Apache Iceberg** (Netflix, Apple), **Delta Lake** (Databricks) và **Apache Hudi** (Uber). Đề tài này sử dụng Apache Iceberg do hỗ trợ đa engine (Spark, Trino, DuckDB, Flink) và cộng đồng mã nguồn mở mạnh.

### 2.1.4 Lambda Architecture

Lambda Architecture (Nathan Marz, 2011) là kiến trúc xử lý dữ liệu lớn đồng thời hỗ trợ cả batch và streaming. Gồm 3 layer:

- **Batch Layer**: xử lý toàn bộ historical data, tạo ra batch views chính xác nhưng có độ trễ (hours/days). Ưu tiên correctness.
- **Speed Layer**: xử lý data realtime với độ trễ thấp (seconds/minutes), tạo realtime views. Ưu tiên low latency, có thể approximate.
- **Serving Layer**: merge kết quả từ Batch Layer và Speed Layer để trả lời queries.

Trong bối cảnh đề tài:
- **Batch Layer**: Python/pandas ingest CSV → PyIceberg append → Bronze Iceberg tables
- **Speed Layer**: Redpanda producer → streaming events → consumer → Bronze Iceberg (Iceberg ACID cho phép concurrent batch + streaming write vào cùng bảng)
- **Serving Layer**: dbt transforms Bronze → Silver → Gold → DuckDB queries → Streamlit dashboard

Iceberg ACID là yếu tố then chốt cho phép Batch Layer và Speed Layer ghi đồng thời vào cùng Bronze table mà không conflict — giải quyết điểm yếu của Lambda Architecture truyền thống khi dùng raw Parquet.

---

## 2.2 Medallion Architecture

Medallion Architecture (phổ biến bởi Databricks) là pattern tổ chức data lake/lakehouse thành các layer chất lượng tăng dần, thường được gọi là Bronze–Silver–Gold.

### Bronze Layer (Raw / Landing Zone)

Bronze là điểm đến đầu tiên của tất cả dữ liệu, lưu trữ data **raw và nguyên vẹn** như khi nhận từ nguồn. Các đặc điểm chính:

- **Append-only**: không bao giờ update hay delete data đã ghi. Mọi sự thay đổi đến từ nguồn đều thêm row mới.
- **Immutable**: Bronze là "nguồn sự thật gốc" — nếu Silver/Gold có lỗi, luôn có thể reprocess từ Bronze.
- **Schema nhẹ**: chỉ thêm metadata columns (`_ingested_at`, `_source_file`) vào schema gốc, không transform.
- **Lưu cả dữ liệu lỗi**: row có null, duplicate, giá trị bất hợp lệ vẫn được lưu — để debug và audit.

### Silver Layer (Cleaned / Validated)

Silver transform dữ liệu từ Bronze thành dạng đã được làm sạch, chuẩn hóa và validated:

- Cast đúng data types (string → timestamp, string → decimal)
- Handle null values theo business rules
- Deduplicate theo business key
- Chuẩn hóa tên column theo convention
- Join các bảng liên quan để tạo enriched views
- Strategy: **incremental merge** — chỉ xử lý delta từ lần chạy trước, không reprocess toàn bộ history

### Gold Layer (Business-Ready / Aggregated)

Gold là layer cuối cùng, tối ưu hóa cho BI và analytics:

- Aggregated metrics theo business logic (doanh thu theo tháng, seller performance score)
- Pre-joined fact và dimension tables theo Star Schema
- Partitioned by time (order_date) để accelerate time-based queries
- Strategy: **full refresh** với partition pruning hoặc **incremental** với partition overwrite

**Lợi ích của Medallion Architecture**:

1. **Reproducibility**: Bronze là immutable source of truth, có thể rebuild Silver/Gold bất kỳ lúc nào.
2. **Debugging**: khi Gold có số liệu sai, truy ngược về Silver rồi Bronze để xác định lỗi ở layer nào.
3. **Incremental processing**: Silver chỉ xử lý delta, giảm compute cost khi dataset lớn.
4. **Separation of concerns**: batch team ghi Bronze, data engineer transform Silver/Gold, analyst dùng Gold — không ảnh hưởng lẫn nhau.

---

## 2.3 Apache Iceberg và PyIceberg

### 2.3.1 Apache Iceberg

Apache Iceberg là open table format cho large analytic datasets, ban đầu được phát triển bởi Netflix (2017) và hiện là Apache top-level project. Iceberg giải quyết các vấn đề cốt lõi của dữ liệu lớn trên object storage.

**Kiến trúc Iceberg** gồm 3 tầng:

```
Catalog (Iceberg REST / Hive Metastore)
    └── Table metadata pointer
            └── metadata/ (v*.metadata.json)
                    ├── Schema versions
                    ├── Partition spec
                    └── Snapshot list
                            └── manifest-list (snap-*.avro)
                                    └── manifest files (*.avro)
                                            └── data files (*.parquet)
```

Mỗi **snapshot** là một immutable view của toàn bộ table tại một thời điểm. Khi ghi dữ liệu mới, Iceberg tạo snapshot mới trỏ đến tập files mới + files cũ cần giữ lại — không sửa files cũ.

**ACID Transactions**: Iceberg dùng **Optimistic Concurrency Control** — nhiều writer có thể chuẩn bị snapshot song song, nhưng chỉ một commit thành công nếu có conflict. Đảm bảo **Serializable Isolation** cho writes.

**Schema Evolution**: Iceberg track schema history bằng column IDs (không phải tên). Có thể:
- Thêm column mới → không ảnh hưởng files cũ
- Đổi tên column → chỉ cập nhật metadata
- Drop column → ẩn khỏi queries, files cũ không bị xóa
- Thay đổi type (widening) → tự động convert khi đọc

**Time Travel**: mỗi Iceberg snapshot có ID và timestamp. Truy vấn dữ liệu quá khứ:
```sql
-- DuckDB / Trino / Spark
SELECT * FROM orders FOR VERSION AS OF 1234567890
SELECT * FROM orders FOR TIMESTAMP AS OF '2024-01-15 00:00:00'
```

**Hidden Partitioning**: partition logic được lưu trong metadata, không lộ ra trong file path hay query. Query optimizer tự động prune partition không cần thiết.

### 2.3.2 PyIceberg

PyIceberg là Python client chính thức cho Apache Iceberg, cho phép thực hiện toàn bộ CRUD operations từ Python mà không cần Spark hay Java runtime:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("rest", uri="http://iceberg-rest:8181")
table = catalog.load_table("bronze.ecom.orders")
table.append(df_arrow)  # Write PyArrow Table hoặc pandas DataFrame
```

PyIceberg hỗ trợ: tạo/drop namespace và table, append data, overwrite partition, scan table (trả về PyArrow RecordBatchReader), schema evolution, snapshot management.

---

## 2.4 Streaming Data Pipeline

### 2.4.1 Khái niệm Event Streaming

Event streaming là mô hình xử lý dữ liệu trong đó mỗi sự kiện nghiệp vụ (đơn hàng tạo, thanh toán xác nhận, review gửi) được publish lên một distributed message queue ngay khi xảy ra, thay vì chờ batch job. Các consumer đọc events theo thứ tự và xử lý theo mục đích riêng.

**Ưu điểm so với batch**:
- Độ trễ thấp: dữ liệu available trong seconds thay vì hours
- Decoupling: producer và consumer độc lập, có thể scale riêng
- Replay: consumer có thể đọc lại từ offset bất kỳ trong quá khứ
- Fan-out: nhiều consumer group xử lý cùng data stream khác nhau

### 2.4.2 Redpanda

Redpanda là Kafka-compatible streaming platform được viết bằng C++, không có JVM và không cần Zookeeper. Điểm phân biệt so với Apache Kafka:

| Tiêu chí | Apache Kafka | Redpanda |
|---|---|---|
| Runtime | JVM (Java) | Native C++ |
| Dependencies | Zookeeper (hoặc KRaft) | Không cần |
| RAM (single node) | ~2GB | ~500MB |
| Latency | ~5–10ms | ~1–2ms |
| API compatibility | — | 100% Kafka API |
| Docker image size | ~800MB | ~200MB |

Trong đề tài, Redpanda đảm nhận vai trò message broker cho streaming pipeline:
- **Topics**: `olist.orders` (đơn hàng mới), `olist.reviews` (đánh giá mới)
- **Partition key**: `order_id` — đảm bảo các events cùng đơn hàng đến consumer theo đúng thứ tự
- **Consumer group**: `iceberg-bronze-writer` — một nhóm consumer ghi vào Bronze Iceberg

### 2.4.3 Confluent Kafka Python Client

`confluent-kafka` là Python client hiệu năng cao cho Kafka/Redpanda, dùng `librdkafka` C library làm backend:

```python
from confluent_kafka import Producer, Consumer

# Producer
producer = Producer({"bootstrap.servers": "redpanda:9092"})
producer.produce(topic="olist.orders", key=order_id, value=json.dumps(payload))
producer.flush()

# Consumer
consumer = Consumer({
    "bootstrap.servers": "redpanda:9092",
    "group.id": "iceberg-bronze-writer",
    "auto.offset.reset": "earliest"
})
consumer.subscribe(["olist.orders"])
```

---

## 2.5 ELT Pipeline với dbt

### 2.5.1 ETL vs ELT

**ETL (Extract–Transform–Load)**: dữ liệu được transform trước khi load vào DWH. Transform logic chạy ngoài database (Python, Spark). Phù hợp khi cần xử lý dữ liệu phức tạp trước khi lưu.

**ELT (Extract–Load–Transform)**: dữ liệu được load raw vào storage trước (Bronze), sau đó transform bằng SQL trực tiếp trong engine (DuckDB, Spark). Phù hợp với Lakehouse paradigm vì:
- Tận dụng processing power của engine
- Transform logic được version control bằng SQL files
- Dễ debug — raw data luôn sẵn sàng trong Bronze
- Schema-on-read: không cần biết schema đầy đủ trước khi load

### 2.5.2 dbt (data build tool)

dbt là SQL-based transformation framework biến câu lệnh SELECT thành các materialized models:

```sql
-- models/silver/stg_orders.sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

SELECT
    order_id,
    customer_id,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS purchased_at,
    order_status,
    CURRENT_TIMESTAMP AS _updated_at
FROM {{ source('bronze', 'ecom_orders') }}
{% if is_incremental() %}
WHERE order_purchase_timestamp > (SELECT MAX(purchased_at) FROM {{ this }})
{% endif %}
```

**Tính năng chính của dbt**:

- **Incremental models**: chỉ xử lý data mới, tiết kiệm compute
- **Ref/Source macros**: `{{ ref('stg_orders') }}` tự động resolve dependencies và build lineage graph
- **Built-in testing**: khai báo tests trong `schema.yml`, chạy `dbt test` để validate
- **Documentation**: `dbt docs generate` tạo interactive data catalog
- **Jinja templating**: logic Python (if/else, for loops) trong SQL

**Incremental strategies trong dbt**:

| Strategy | Hành vi | Dùng khi |
|---|---|---|
| `append` | Insert new rows, không xóa | Bronze (append-only) |
| `merge` | Upsert theo unique_key | Silver (dedup + update) |
| `insert_overwrite` | Overwrite partitions | Gold (partition refresh) |
| `delete+insert` | Xóa rồi insert theo filter | Khi cần rebuild partition |

---

## 2.6 Analytical Query Engine: DuckDB

DuckDB là embedded analytical database engine được thiết kế cho OLAP workloads trên single machine. Khác với PostgreSQL (OLTP, client-server), DuckDB chạy **in-process** — không cần server, không có network overhead, không cần container.

**Kiến trúc kỹ thuật**:
- **Columnar storage**: dữ liệu lưu theo cột, đọc chỉ những columns cần thiết
- **Vectorized execution engine**: xử lý batch của values (vector) thay vì từng row — SIMD instructions, cache-friendly
- **Push-based execution**: data được "push" qua pipeline operators, tránh materialization trung gian
- **Parallel query execution**: tự động parallelize query trên multiple CPU cores

**Đọc trực tiếp từ object storage**:

```python
import duckdb

conn = duckdb.connect()
conn.execute("""
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint = 'https://xxx.r2.cloudflarestorage.com';
    SET s3_access_key_id = '...';
    SET s3_secret_access_key = '...';
""")

# Đọc trực tiếp Parquet từ R2 — không cần download local
result = conn.execute("""
    SELECT order_status, COUNT(*) as cnt, SUM(payment_value) as revenue
    FROM read_parquet('s3://olist-lakehouse/gold/fct_orders/data/*.parquet')
    GROUP BY order_status
    ORDER BY revenue DESC
""").fetchdf()
```

**Tại sao DuckDB phù hợp cho dự án này**:
- Dataset ~200MB fits entirely in memory → maximum performance
- Không cần container → đơn giản hóa Docker Compose
- Native Iceberg reader (DuckDB 0.10+) qua extension `iceberg`
- dbt-duckdb adapter hoạt động tốt cho Silver/Gold transformation
- Phù hợp cho single-user analytics (Streamlit dashboard)

---

## 2.7 Workflow Orchestration với Prefect

### 2.7.1 Tầm quan trọng của Orchestration

Một data pipeline không có orchestration là tập hợp các script riêng lẻ cần chạy thủ công theo đúng thứ tự. Trong môi trường production, điều này không thể chấp nhận:

- Nếu bước 2 fail, bước 3 vẫn chạy → dữ liệu corrupt
- Không có retry → một lỗi tạm thời (network timeout) làm cả pipeline fail
- Không có monitoring → không biết pipeline đã chạy thành công hay chưa
- Không có scheduling → phải nhớ chạy thủ công mỗi ngày

**Workflow Orchestration** giải quyết các vấn đề này bằng cách định nghĩa rõ dependency, retry policy, scheduling và monitoring cho toàn bộ pipeline.

### 2.7.2 Prefect

Prefect là modern workflow orchestration framework cho Python, sử dụng decorator-based API:

```python
from prefect import flow, task

@task(retries=3, retry_delay_seconds=30)
def ingest_orders():
    # logic ingest...
    return row_count

@task(retries=2)
def run_dbt_silver():
    subprocess.run(["dbt", "run", "--select", "silver.*"], check=True)

@flow(name="Daily Medallion Pipeline")
def medallion_pipeline():
    count = ingest_orders()      # Bước 1
    run_dbt_silver()             # Bước 2: chạy sau Bước 1
```

**Tính năng chính của Prefect**:

- **`@task` decorator**: đơn vị cơ bản của pipeline, có thể retry, timeout, cache
- **`@flow` decorator**: nhóm các tasks, định nghĩa dependency, handle failures
- **Parallel execution**: `task.submit()` để submit tasks song song, `futures.result()` để wait
- **Prefect Cloud UI**: real-time monitoring, run history, failure alerting — free tier đủ dùng
- **Deployments**: schedule flow chạy tự động (cron), trigger từ event hoặc API call
- **Artifacts**: log metrics, dataframes, plots từ bên trong task

**So sánh Prefect với Apache Airflow**:

| Tiêu chí | Apache Airflow | Prefect |
|---|---|---|
| DAG definition | Python file với DAG class | Python functions với decorators |
| Boilerplate | Cao (DAG, Operators) | Thấp (chỉ decorators) |
| Local testing | Cần Airflow scheduler | `flow()` chạy như Python function thông thường |
| Dynamic tasks | Hạn chế | Native (task mapping) |
| UI | Self-hosted | Prefect Cloud (free tier) |
| Retry logic | Operator-level | Task-level, configurable |

---

## 2.8 Data Quality với Soda Core

### 2.8.1 Tầm quan trọng của Data Quality

Dữ liệu kém chất lượng là một trong những nguyên nhân phổ biến nhất làm mất tin tưởng vào data platform. Các vấn đề thường gặp:

- **Completeness**: cột quan trọng bị null (`order_id IS NULL`)
- **Uniqueness**: duplicate records do ingestion retry không idempotent
- **Validity**: giá trị ngoài phạm vi hợp lệ (`payment_value < 0`)
- **Freshness**: table không được cập nhật trong khoảng thời gian dự kiến
- **Volume**: số row đột ngột giảm mạnh → upstream pipeline bị lỗi

Nếu không có quality checks, lỗi từ Bronze sẽ lan truyền lên Silver và Gold — đến khi analyst phát hiện số liệu sai thì rất khó trace về nguồn gốc.

### 2.8.2 Soda Core

Soda Core là open-source data quality framework sử dụng YAML-based checks:

```yaml
# quality/soda_checks.yml
checks for bronze_orders:
  - row_count > 0:
      name: "Bronze orders không được rỗng"
  - missing_count(order_id) = 0:
      name: "order_id không được null"
  - duplicate_count(order_id) = 0:
      name: "order_id phải unique"
  - min(payment_value) >= 0:
      name: "payment_value không được âm"
  - freshness(ingested_at) < 2h:
      name: "Data phải được ingest trong 2 giờ qua"
```

**Tích hợp với Prefect**:

```python
from soda.scan import Scan

@task(retries=0)  # Không retry quality checks
def run_quality_checks(table_name: str):
    scan = Scan()
    scan.add_sodacl_yaml_file("quality/soda_checks.yml")
    scan.execute()
    if scan.has_check_fails():
        raise ValueError(f"Data quality failed for {table_name}")
```

Nếu quality check fail, Prefect task raise exception → flow fail → alerting → pipeline dừng lại, không promote dữ liệu xấu lên Silver.

---

*Các khái niệm lý thuyết trong chương này được áp dụng trực tiếp vào thiết kế hệ thống được trình bày trong Chương 3.*
