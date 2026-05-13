# Olist – Hướng dẫn Kéo Dữ liệu từ Kaggle → R2 + Redpanda

## Tổng quan luồng dữ liệu

```
Kaggle
  │
  │  kaggle CLI / API
  ▼
data/raw/                          ← CSV gốc trên máy local
  ├── ecommerce/  (8 files)
  └── marketing/  (2 files)
  │
  ├──► [Batch path]
  │      boto3 upload
  │         ▼
  │    R2: olist-lakehouse/raw/    ← CSV gốc backup trên cloud
  │         ▼
  │    PyIceberg write
  │         ▼
  │    R2: olist-lakehouse/       ← Bronze Iceberg tables (Parquet)
  │
  └──► [Streaming path]
         confluent-kafka Producer
              ▼
         Redpanda topics:
           olist.orders      (3 partitions)
           olist.reviews     (3 partitions)
           olist.payments    (3 partitions)
           olist.leads       (2 partitions)
           olist.deals       (2 partitions)
```

---

## Mục lục

1. [Yêu cầu](#1-yêu-cầu)
2. [Lấy Kaggle API Token](#2-lấy-kaggle-api-token)
3. [Cài thư viện Python](#3-cài-thư-viện-python)
4. [Tải dataset từ Kaggle](#4-tải-dataset-từ-kaggle)
5. [Upload CSV thô lên R2](#5-upload-csv-thô-lên-r2)
6. [Ingest vào Bronze Iceberg (batch)](#6-ingest-vào-bronze-iceberg-batch)
7. [Stream dữ liệu lên Redpanda](#7-stream-dữ-liệu-lên-redpanda)
8. [Kiểm tra kết quả](#8-kiểm-tra-kết-quả)

---

## 1. Yêu cầu

- Python >= 3.10
- Docker stack đã chạy (`docker compose up -d` xong, tất cả service healthy)
- File `.env` đã điền đủ `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`
- Tài khoản Kaggle (miễn phí)

---

## 2. Lấy Kaggle API Token

### Bước 2.1 – Tạo token trên Kaggle

1. Đăng nhập https://www.kaggle.com
2. Click avatar góc phải trên → **Settings**
3. Kéo xuống mục **API** → Click **Create New Token**
4. File `kaggle.json` tự động tải xuống, nội dung:

```json
{"username":"your_kaggle_username","key":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}
```

### Bước 2.2 – Đặt file đúng chỗ

**Linux / macOS:**
```bash
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

**Windows (PowerShell):**
```powershell
New-Item -ItemType Directory -Force -Path "$env:USERPROFILE\.kaggle"
Move-Item "$env:USERPROFILE\Downloads\kaggle.json" "$env:USERPROFILE\.kaggle\kaggle.json"
```

### Bước 2.3 – Kiểm tra Kaggle CLI hoạt động

```bash
kaggle datasets list --search "olist"
# Phải thấy 2 datasets: brazilian-ecommerce và marketing-funnel-olist
```

---

## 3. Cài thư viện Python

```bash
pip install \
  kaggle \
  boto3==1.34.0 \
  pandas==2.2.0 \
  pyarrow==15.0.0 \
  "pyiceberg[s3fs,pandas]==0.6.0" \
  confluent-kafka==2.3.0 \
  python-dotenv
```

---

## 4. Tải dataset từ Kaggle

Chạy từ thư mục gốc project (`Olist-data-engineering-project/`):

```bash
# Tạo thư mục chứa data
mkdir -p data/raw/ecommerce
mkdir -p data/raw/marketing

# Dataset 1: Brazilian E-Commerce (8 files, ~46 MB zip)
kaggle datasets download olistbr/brazilian-ecommerce \
  --path data/raw/ecommerce \
  --unzip

# Dataset 2: Marketing Funnel (2 files, ~1 MB zip)
kaggle datasets download olistbr/marketing-funnel-olist \
  --path data/raw/marketing \
  --unzip
```

**Kết quả mong đợi:**

```
data/raw/
├── ecommerce/
│   ├── olist_customers_dataset.csv          (~100K rows)
│   ├── olist_geolocation_dataset.csv        (~1M rows)
│   ├── olist_order_items_dataset.csv        (~112K rows)
│   ├── olist_order_payments_dataset.csv     (~104K rows)
│   ├── olist_order_reviews_dataset.csv      (~100K rows)
│   ├── olist_orders_dataset.csv             (~100K rows)
│   ├── olist_products_dataset.csv           (~33K rows)
│   └── olist_sellers_dataset.csv            (~3K rows)
└── marketing/
    ├── olist_closed_deals_dataset.csv       (~842 rows)
    └── olist_marketing_qualified_leads_dataset.csv  (~8K rows)
```

Kiểm tra nhanh:

```bash
# Xem số dòng từng file
wc -l data/raw/ecommerce/*.csv
wc -l data/raw/marketing/*.csv
```

---

## 5. Upload CSV thô lên R2

> **Mục đích:** Lưu bản gốc CSV lên R2 làm archive, trước khi transform. Path: `olist-lakehouse/raw/`

Tạo file `scripts/upload_raw_to_r2.py`:

```python
"""
Upload toàn bộ raw CSV lên R2.
Path đích: s3://olist-lakehouse/raw/ecommerce/ và .../raw/marketing/
"""
import os
import boto3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Kết nối R2 ──────────────────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name=os.environ.get("S3_REGION", "auto"),
)

BUCKET = "olist-lakehouse"
LOCAL_DATA = Path("data/raw")

# ── Upload từng file ─────────────────────────────────────────────────────────
def upload_directory(local_dir: Path, s3_prefix: str):
    files = list(local_dir.glob("*.csv"))
    print(f"\nUploading {len(files)} files from {local_dir} → s3://{BUCKET}/{s3_prefix}")

    for csv_file in sorted(files):
        s3_key = f"{s3_prefix}/{csv_file.name}"
        size_mb = csv_file.stat().st_size / 1024 / 1024
        print(f"  [{size_mb:6.2f} MB]  {csv_file.name}  →  {s3_key}")

        s3.upload_file(
            Filename=str(csv_file),
            Bucket=BUCKET,
            Key=s3_key,
        )

    print(f"  Done: {len(files)} files uploaded.")

upload_directory(LOCAL_DATA / "ecommerce", "raw/ecommerce")
upload_directory(LOCAL_DATA / "marketing", "raw/marketing")

# ── Xác nhận ────────────────────────────────────────────────────────────────
print("\n=== Files on R2 ===")
resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/")
for obj in resp.get("Contents", []):
    size_kb = obj["Size"] / 1024
    print(f"  {obj['Key']:70s}  {size_kb:8.1f} KB")
```

Chạy:

```bash
python scripts/upload_raw_to_r2.py
```

---

## 6. Ingest vào Bronze Iceberg (batch)

> **Mục đích:** Đọc CSV → convert sang Parquet → ghi vào Bronze Iceberg tables trên R2.
> Bronze tables là append-only, lưu nguyên schema gốc.

Tạo file `scripts/batch_ingest_bronze.py`:

```python
"""
Batch ingest: CSV gốc → Bronze Iceberg tables trên R2.
Dùng PyIceberg REST catalog (đang chạy tại localhost:8181).

Tables tạo ra:
  bronze.ecommerce_orders
  bronze.ecommerce_order_items
  bronze.ecommerce_order_payments
  bronze.ecommerce_order_reviews
  bronze.ecommerce_products
  bronze.ecommerce_sellers
  bronze.ecommerce_customers
  bronze.ecommerce_geolocation
  bronze.marketing_leads
  bronze.marketing_deals
"""
import os
import pandas as pd
import pyarrow as pa
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, LongType, DoubleType, TimestampType, IntegerType
)
from pyiceberg.partitioning import PartitionSpec

load_dotenv()

# ── Kết nối Iceberg REST catalog ─────────────────────────────────────────────
catalog = load_catalog(
    "rest",
    **{
        "uri": "http://localhost:8181",
        "s3.endpoint": os.environ["S3_ENDPOINT"],
        "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
        "s3.region": os.environ.get("S3_REGION", "auto"),
        "s3.path-style-access": "false",
    }
)

# Tạo namespace nếu chưa có
try:
    catalog.create_namespace("bronze")
    print("Namespace 'bronze' created.")
except Exception:
    print("Namespace 'bronze' already exists.")

LOCAL_DATA = Path("data/raw")
INGESTED_AT = datetime.now(timezone.utc).isoformat()

# ── Hàm tiện ích ─────────────────────────────────────────────────────────────

def ingest_csv(csv_path: Path, table_name: str, timestamp_cols: list[str] = None):
    """Đọc CSV, thêm metadata cột, ghi vào Bronze Iceberg table."""
    full_table = f"bronze.{table_name}"
    print(f"\n{'─'*60}")
    print(f"Ingesting: {csv_path.name}  →  {full_table}")

    # Đọc CSV
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")

    # Parse timestamp columns
    if timestamp_cols:
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

    # Thêm metadata columns cho Bronze
    df["_ingested_at"] = INGESTED_AT
    df["_source_file"] = csv_path.name

    # Convert sang PyArrow table (tự suy ra schema)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    # Tạo hoặc lấy Iceberg table
    if catalog.table_exists(full_table):
        tbl = catalog.load_table(full_table)
        print(f"  Table exists – appending data.")
    else:
        tbl = catalog.create_table(
            identifier=full_table,
            schema=arrow_table.schema,
            location=f"s3://olist-lakehouse/bronze/{table_name}",
            partition_spec=PartitionSpec(),  # unpartitioned ở Bronze
        )
        print(f"  Table created.")

    tbl.append(arrow_table)
    print(f"  Written {len(df):,} rows to {full_table}")


# ── Ingest tất cả datasets ───────────────────────────────────────────────────

print("=" * 60)
print("BRONZE INGESTION START")
print("=" * 60)

# E-Commerce datasets
ingest_csv(
    LOCAL_DATA / "ecommerce/olist_orders_dataset.csv",
    "ecommerce_orders",
    timestamp_cols=["order_purchase_timestamp", "order_approved_at",
                    "order_delivered_carrier_date", "order_delivered_customer_date",
                    "order_estimated_delivery_date"],
)
ingest_csv(
    LOCAL_DATA / "ecommerce/olist_order_items_dataset.csv",
    "ecommerce_order_items",
    timestamp_cols=["shipping_limit_date"],
)
ingest_csv(
    LOCAL_DATA / "ecommerce/olist_order_payments_dataset.csv",
    "ecommerce_order_payments",
)
ingest_csv(
    LOCAL_DATA / "ecommerce/olist_order_reviews_dataset.csv",
    "ecommerce_order_reviews",
    timestamp_cols=["review_creation_date", "review_answer_timestamp"],
)
ingest_csv(
    LOCAL_DATA / "ecommerce/olist_products_dataset.csv",
    "ecommerce_products",
)
ingest_csv(
    LOCAL_DATA / "ecommerce/olist_sellers_dataset.csv",
    "ecommerce_sellers",
)
ingest_csv(
    LOCAL_DATA / "ecommerce/olist_customers_dataset.csv",
    "ecommerce_customers",
)
ingest_csv(
    LOCAL_DATA / "ecommerce/olist_geolocation_dataset.csv",
    "ecommerce_geolocation",
)

# Marketing datasets
ingest_csv(
    LOCAL_DATA / "marketing/olist_marketing_qualified_leads_dataset.csv",
    "marketing_leads",
    timestamp_cols=["first_contact_date"],
)
ingest_csv(
    LOCAL_DATA / "marketing/olist_closed_deals_dataset.csv",
    "marketing_deals",
    timestamp_cols=["won_date"],
)

print("\n" + "=" * 60)
print("BRONZE INGESTION COMPLETE")

# ── Tổng kết ─────────────────────────────────────────────────────────────────
print("\n=== Bronze tables ===")
for ns in catalog.list_namespaces():
    for table_id in catalog.list_tables(ns[0]):
        tbl = catalog.load_table(table_id)
        print(f"  {'.'.join(table_id)}")
```

Chạy:

```bash
python scripts/batch_ingest_bronze.py
```

**Kết quả mong đợi (phần cuối log):**

```
BRONZE INGESTION COMPLETE

=== Bronze tables ===
  bronze.ecommerce_customers
  bronze.ecommerce_geolocation
  bronze.ecommerce_order_items
  bronze.ecommerce_order_payments
  bronze.ecommerce_order_reviews
  bronze.ecommerce_orders
  bronze.ecommerce_products
  bronze.ecommerce_sellers
  bronze.marketing_deals
  bronze.marketing_leads
```

---

## 7. Stream dữ liệu lên Redpanda

> **Mục đích:** Simulate real-time events – đọc CSV, sort theo timestamp, publish từng row dưới dạng JSON message lên Redpanda topic.
>
> **SPEED_FACTOR=86400** nghĩa là 1 ngày dữ liệu = 1 giây thực. Dataset ~2 năm dữ liệu sẽ phát trong khoảng 730 giây (~12 phút).

**Mapping topic → file CSV:**

| Redpanda topic | File CSV | Timestamp column |
|---|---|---|
| `olist.orders` | `olist_orders_dataset.csv` | `order_purchase_timestamp` |
| `olist.reviews` | `olist_order_reviews_dataset.csv` | `review_creation_date` |
| `olist.payments` | `olist_order_payments_dataset.csv` | *(join qua order_id)* |
| `olist.leads` | `olist_marketing_qualified_leads_dataset.csv` | `first_contact_date` |
| `olist.deals` | `olist_closed_deals_dataset.csv` | `won_date` |

Tạo file `scripts/stream_to_redpanda.py`:

```python
"""
Streaming producer: đọc CSV → sort theo timestamp → publish lên Redpanda.
Chạy trong khi docker stack đang up (Redpanda tại localhost:19092).

Cách dùng:
  python scripts/stream_to_redpanda.py              # tốc độ bình thường (SPEED_FACTOR=86400)
  python scripts/stream_to_redpanda.py --fast       # không delay (load hết ngay lập tức)
  python scripts/stream_to_redpanda.py --topic orders   # chỉ 1 topic
"""
import os
import sys
import json
import time
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException

LOCAL_DATA = Path("data/raw")

# ── Cấu hình Kafka producer ─────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "olist-stream-producer",
    "linger.ms": 10,          # batch nhỏ để giảm số request
    "batch.size": 65536,
    "compression.type": "snappy",
    "acks": "1",
})

def delivery_report(err, msg):
    if err:
        print(f"  ERROR delivery: {err}", file=sys.stderr)

# ── Cấu hình datasets ────────────────────────────────────────────────────────
STREAM_CONFIG = {
    "orders": {
        "csv": LOCAL_DATA / "ecommerce/olist_orders_dataset.csv",
        "topic": "olist.orders",
        "timestamp_col": "order_purchase_timestamp",
        "key_col": "order_id",
    },
    "reviews": {
        "csv": LOCAL_DATA / "ecommerce/olist_order_reviews_dataset.csv",
        "topic": "olist.reviews",
        "timestamp_col": "review_creation_date",
        "key_col": "review_id",
    },
    "payments": {
        "csv": LOCAL_DATA / "ecommerce/olist_order_payments_dataset.csv",
        "topic": "olist.payments",
        "timestamp_col": None,      # payments không có timestamp riêng
        "key_col": "order_id",
    },
    "leads": {
        "csv": LOCAL_DATA / "marketing/olist_marketing_qualified_leads_dataset.csv",
        "topic": "olist.leads",
        "timestamp_col": "first_contact_date",
        "key_col": "mql_id",
    },
    "deals": {
        "csv": LOCAL_DATA / "marketing/olist_closed_deals_dataset.csv",
        "topic": "olist.deals",
        "timestamp_col": "won_date",
        "key_col": "mql_id",
    },
}

# ── Hàm stream 1 topic ──────────────────────────────────────────────────────

def stream_topic(name: str, config: dict, speed_factor: int = 86400, fast: bool = False):
    """
    Đọc CSV, sort theo timestamp, publish lên Kafka với delay proportional.
    speed_factor=86400 → 1 ngày data = 1 giây thực.
    fast=True → không delay.
    """
    topic = config["topic"]
    timestamp_col = config["timestamp_col"]
    key_col = config["key_col"]

    print(f"\n{'─' * 60}")
    print(f"Topic: {topic}  |  Source: {config['csv'].name}")

    df = pd.read_csv(config["csv"], low_memory=False)
    print(f"  Loaded {len(df):,} rows")

    # Sort theo timestamp nếu có
    if timestamp_col and timestamp_col in df.columns:
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
        df = df.dropna(subset=[timestamp_col]).sort_values(timestamp_col)
        min_ts = df[timestamp_col].min()
        max_ts = df[timestamp_col].max()
        total_seconds = (max_ts - min_ts).total_seconds()
        print(f"  Time range: {min_ts.date()} → {max_ts.date()} ({total_seconds/86400:.0f} days)")
        if not fast:
            print(f"  Replay duration: ~{total_seconds/speed_factor:.0f} seconds")
    else:
        df = df.reset_index(drop=True)
        min_ts = None

    # Publish từng row
    sent = 0
    prev_ts = None

    for _, row in df.iterrows():
        # Tính delay giữa các event
        if not fast and timestamp_col and min_ts is not None:
            curr_ts = row[timestamp_col]
            if prev_ts is not None and pd.notna(curr_ts) and pd.notna(prev_ts):
                delta_real = (curr_ts - prev_ts).total_seconds()
                delay = delta_real / speed_factor
                if delay > 0:
                    time.sleep(min(delay, 0.1))  # cap 100ms để không block quá lâu
            prev_ts = curr_ts

        # Serialize message
        record = {k: (v if pd.notna(v) else None) for k, v in row.items()}
        # Thêm event metadata
        record["_event_time"] = datetime.now(timezone.utc).isoformat()
        record["_source"] = config["csv"].name

        # Key là ID chính của record
        key = str(record.get(key_col, sent))

        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(record, default=str).encode("utf-8"),
            on_delivery=delivery_report,
        )

        sent += 1
        if sent % 1000 == 0:
            producer.poll(0)  # trigger callbacks không block
            print(f"  {sent:,} / {len(df):,} messages sent...", end="\r")

    producer.flush()
    print(f"\n  Done: {sent:,} messages → {topic}")
    return sent

# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Stream Olist data to Redpanda")
    parser.add_argument("--fast", action="store_true",
                        help="No delay – publish all messages immediately")
    parser.add_argument("--topic", choices=list(STREAM_CONFIG.keys()),
                        help="Stream only this topic (default: all)")
    parser.add_argument("--speed", type=int, default=86400,
                        help="Speed factor (default: 86400 → 1 day = 1 second)")
    args = parser.parse_args()

    targets = {args.topic: STREAM_CONFIG[args.topic]} if args.topic else STREAM_CONFIG

    print("=" * 60)
    print(f"REDPANDA STREAM PRODUCER")
    print(f"Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"Mode: {'FAST (no delay)' if args.fast else f'Replay (speed_factor={args.speed})'}")
    print(f"Topics: {', '.join(t['topic'] for t in targets.values())}")
    print("=" * 60)

    total = 0
    for name, config in targets.items():
        count = stream_topic(name, config, speed_factor=args.speed, fast=args.fast)
        total += count

    print(f"\n{'=' * 60}")
    print(f"STREAM COMPLETE – Total: {total:,} messages published")

if __name__ == "__main__":
    main()
```

### Cách chạy streaming

```bash
# Load nhanh toàn bộ data (không delay) – phù hợp để test
python scripts/stream_to_redpanda.py --fast

# Replay với tốc độ 86400x (1 ngày = 1 giây) – mô phỏng production
python scripts/stream_to_redpanda.py

# Chỉ stream 1 topic
python scripts/stream_to_redpanda.py --fast --topic orders

# Replay chậm hơn: 1 ngày = 10 giây
python scripts/stream_to_redpanda.py --speed 8640
```

---

## 8. Kiểm tra kết quả

### 8.1 Kiểm tra R2 (raw CSV đã lên chưa)

```bash
# Dùng AWS CLI với R2 endpoint
aws s3 ls s3://olist-lakehouse/raw/ \
  --endpoint-url $S3_ENDPOINT \
  --region auto \
  --recursive \
  --human-readable

# Phải thấy 10 file CSV tổng cộng
```

Hoặc dùng Python:

```python
import boto3, os
from dotenv import load_dotenv
load_dotenv()

s3 = boto3.client("s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name="auto",
)
resp = s3.list_objects_v2(Bucket="olist-lakehouse", Prefix="raw/")
for obj in resp["Contents"]:
    print(f"{obj['Key']:70s}  {obj['Size']/1024:8.1f} KB")
```

### 8.2 Kiểm tra Bronze Iceberg tables

```python
import os
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
load_dotenv()

catalog = load_catalog("rest", **{
    "uri": "http://localhost:8181",
    "s3.endpoint": os.environ["S3_ENDPOINT"],
    "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
    "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
    "s3.region": "auto",
    "s3.path-style-access": "false",
})

for table_id in catalog.list_tables("bronze"):
    tbl = catalog.load_table(table_id)
    df = tbl.scan().to_pandas()
    print(f"{'.'join(table_id):45s}  {len(df):>8,} rows")
```

**Kết quả mong đợi:**

```
bronze.ecommerce_customers           99,441 rows
bronze.ecommerce_geolocation      1,000,163 rows
bronze.ecommerce_order_items        112,650 rows
bronze.ecommerce_order_payments     103,886 rows
bronze.ecommerce_order_reviews       99,224 rows
bronze.ecommerce_orders              99,441 rows
bronze.ecommerce_products            32,951 rows
bronze.ecommerce_sellers              3,095 rows
bronze.marketing_deals                  842 rows
bronze.marketing_leads               8,000 rows
```

### 8.3 Kiểm tra Redpanda topics

Mở Redpanda Console tại http://localhost:8080 → **Topics** → chọn topic bất kỳ → **Messages**

Hoặc dùng CLI:

```bash
# Xem số messages mỗi topic
docker exec olist-redpanda rpk topic describe olist.orders
docker exec olist-redpanda rpk topic describe olist.reviews
docker exec olist-redpanda rpk topic describe olist.payments
docker exec olist-redpanda rpk topic describe olist.leads
docker exec olist-redpanda rpk topic describe olist.deals

# Đọc thử 5 message đầu của topic orders
docker exec olist-redpanda rpk topic consume olist.orders \
  --num 5 \
  --brokers localhost:9092
```

**Số messages mong đợi sau `--fast`:**

| Topic | Messages |
|---|---|
| `olist.orders` | ~99,441 |
| `olist.reviews` | ~99,224 |
| `olist.payments` | ~103,886 |
| `olist.leads` | ~8,000 |
| `olist.deals` | ~842 |

### 8.4 Kiểm tra nhanh qua Iceberg REST API

```bash
# Danh sách namespaces
curl -s http://localhost:8181/v1/namespaces | python -m json.tool

# Danh sách tables trong namespace bronze
curl -s http://localhost:8181/v1/namespaces/bronze/tables | python -m json.tool

# Schema của bảng orders
curl -s http://localhost:8181/v1/namespaces/bronze/tables/ecommerce_orders | python -m json.tool
```

---

## Tóm tắt thứ tự chạy

```bash
# 1. Đảm bảo stack đang chạy
docker compose ps

# 2. Tải data từ Kaggle (chỉ cần chạy 1 lần)
kaggle datasets download olistbr/brazilian-ecommerce --path data/raw/ecommerce --unzip
kaggle datasets download olistbr/marketing-funnel-olist --path data/raw/marketing --unzip

# 3. Upload raw CSV lên R2 (backup)
python scripts/upload_raw_to_r2.py

# 4. Ingest vào Bronze Iceberg (batch)
python scripts/batch_ingest_bronze.py

# 5a. Stream toàn bộ data ngay lập tức (để test nhanh)
python scripts/stream_to_redpanda.py --fast

# 5b. HOẶC stream với delay để simulate production (chạy nền)
nohup python scripts/stream_to_redpanda.py > logs/stream.log 2>&1 &
```
