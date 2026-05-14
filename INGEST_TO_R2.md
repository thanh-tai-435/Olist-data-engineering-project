# Hướng dẫn Ingest Data vào Cloudflare R2

Tài liệu này hướng dẫn cách đẩy toàn bộ dữ liệu Olist lên Cloudflare R2, bao gồm:

- **Raw layer**: Upload CSV gốc làm archive bất biến
- **Bronze layer**: Ghi dữ liệu vào Apache Iceberg tables qua PyIceberg REST catalog

> **Yêu cầu trước**: R2 bucket `olist-lakehouse` đã tạo và `.env` đã điền credentials.  
> Xem `SETUP.md` mục 3 nếu chưa làm bước này.

---

## Tổng quan luồng

```
data/raw/                   (CSV gốc trên máy local)
    │
    ├──► boto3 upload
    │         ▼
    │    R2: olist-lakehouse/raw/          ← CSV backup, không bao giờ xoá
    │
    └──► PyIceberg append
              ▼
         R2: olist-lakehouse/bronze/       ← Iceberg Parquet tables, append-only
               ├── ecommerce_orders/
               ├── ecommerce_order_items/
               ├── ecommerce_order_payments/
               ├── ecommerce_order_reviews/
               ├── ecommerce_products/
               ├── ecommerce_sellers/
               ├── ecommerce_customers/
               ├── ecommerce_geolocation/
               ├── marketing_leads/
               └── marketing_deals/
```

---

## Mục lục

1. [Yêu cầu](#1-yêu-cầu)
2. [Chuẩn bị dữ liệu gốc](#2-chuẩn-bị-dữ-liệu-gốc)
3. [Cài đặt thư viện Python](#3-cài-đặt-thư-viện-python)
4. [Bước 1 – Upload raw CSV lên R2](#4-bước-1--upload-raw-csv-lên-r2)
5. [Bước 2 – Ingest vào Bronze Iceberg](#5-bước-2--ingest-vào-bronze-iceberg)
6. [Kiểm tra kết quả](#6-kiểm-tra-kết-quả)
7. [Xử lý lỗi thường gặp](#7-xử-lý-lỗi-thường-gặp)

---

## 1. Yêu cầu

| Thứ | Yêu cầu |
|---|---|
| Python | >= 3.10 |
| Docker stack | `docker compose up -d` đã chạy, `iceberg-rest` healthy |
| File `.env` | `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` đã điền |
| Bucket | `olist-lakehouse` đã tạo trên Cloudflare dashboard |

Kiểm tra stack và catalog đang chạy:

```bash
docker compose ps
# Đảm bảo iceberg-rest, postgres đều (healthy)

curl -s http://localhost:8181/v1/config
# Phải trả về JSON, không lỗi
```

---

## 2. Chuẩn bị dữ liệu gốc

Dữ liệu CSV đặt tại `data/raw/`. Nếu chưa có, tải từ Kaggle:

```bash
mkdir -p data/raw/ecommerce data/raw/marketing

kaggle datasets download olistbr/brazilian-ecommerce \
  --path data/raw/ecommerce --unzip

kaggle datasets download olistbr/marketing-funnel-olist \
  --path data/raw/marketing --unzip
```

> Cài Kaggle CLI: `pip install kaggle`  
> Tạo token tại https://www.kaggle.com → Settings → API → Create New Token  
> Đặt file `kaggle.json` vào `~/.kaggle/kaggle.json`

Kết quả thư mục sau khi tải:

```
data/raw/
├── ecommerce/
│   ├── olist_customers_dataset.csv          (~99K rows)
│   ├── olist_geolocation_dataset.csv        (~1M rows)
│   ├── olist_order_items_dataset.csv        (~113K rows)
│   ├── olist_order_payments_dataset.csv     (~104K rows)
│   ├── olist_order_reviews_dataset.csv      (~99K rows)
│   ├── olist_orders_dataset.csv             (~99K rows)
│   ├── olist_products_dataset.csv           (~33K rows)
│   └── olist_sellers_dataset.csv            (~3K rows)
└── marketing/
    ├── olist_closed_deals_dataset.csv       (~842 rows)
    └── olist_marketing_qualified_leads_dataset.csv  (~8K rows)
```

---

## 3. Cài đặt thư viện Python

```bash
pip install \
  boto3==1.34.0 \
  pandas==2.2.0 \
  pyarrow==15.0.0 \
  "pyiceberg[s3fs,pandas]==0.6.0" \
  python-dotenv
```

---

## 4. Bước 1 – Upload raw CSV lên R2

Tạo file `scripts/upload_raw_to_r2.py`:

```python
"""
Upload toàn bộ raw CSV lên R2 làm archive bất biến.
Đích: s3://olist-lakehouse/raw/ecommerce/ và .../raw/marketing/
"""
import os
import boto3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name=os.environ.get("S3_REGION", "auto"),
)

BUCKET = "olist-lakehouse"
LOCAL_DATA = Path("data/raw")


def upload_directory(local_dir: Path, s3_prefix: str):
    files = sorted(local_dir.glob("*.csv"))
    print(f"\nUploading {len(files)} files: {local_dir} → s3://{BUCKET}/{s3_prefix}")

    for f in files:
        key = f"{s3_prefix}/{f.name}"
        size_mb = f.stat().st_size / 1_048_576
        print(f"  {size_mb:6.2f} MB  {f.name}")
        s3.upload_file(Filename=str(f), Bucket=BUCKET, Key=key)

    print(f"  Done ({len(files)} files).")


upload_directory(LOCAL_DATA / "ecommerce", "raw/ecommerce")
upload_directory(LOCAL_DATA / "marketing", "raw/marketing")

print("\n=== Files on R2 (raw/) ===")
resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/")
for obj in resp.get("Contents", []):
    print(f"  {obj['Key']:70s}  {obj['Size'] / 1024:8.1f} KB")
```

Chạy:

```bash
mkdir -p scripts
python scripts/upload_raw_to_r2.py
```

**Output mong đợi:**

```
Uploading 8 files: data/raw/ecommerce → s3://olist-lakehouse/raw/ecommerce
   0.53 MB  olist_customers_dataset.csv
  ...

=== Files on R2 (raw/) ===
  raw/ecommerce/olist_customers_dataset.csv           530.1 KB
  raw/ecommerce/olist_geolocation_dataset.csv       18342.7 KB
  ...
```

---

## 5. Bước 2 – Ingest vào Bronze Iceberg

Tạo file `scripts/batch_ingest_bronze.py`:

```python
"""
Batch ingest: CSV gốc → Bronze Iceberg tables trên R2.
Catalog: Iceberg REST tại localhost:8181
Tables: bronze.ecommerce_* và bronze.marketing_*

Mỗi lần chạy APPEND thêm data (không ghi đè).
Dùng cờ --overwrite để drop-and-recreate bảng nếu cần reset.
"""
import os
import sys
import argparse
import pandas as pd
import pyarrow as pa
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
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
    },
)

# ── Config ingestion ──────────────────────────────────────────────────────────

LOCAL_DATA = Path("data/raw")
INGESTED_AT = datetime.now(timezone.utc).isoformat()

# Danh sách tất cả datasets cần ingest
DATASETS = [
    # (csv_path, iceberg_table_name, [timestamp_columns])
    (
        LOCAL_DATA / "ecommerce/olist_orders_dataset.csv",
        "ecommerce_orders",
        ["order_purchase_timestamp", "order_approved_at",
         "order_delivered_carrier_date", "order_delivered_customer_date",
         "order_estimated_delivery_date"],
    ),
    (
        LOCAL_DATA / "ecommerce/olist_order_items_dataset.csv",
        "ecommerce_order_items",
        ["shipping_limit_date"],
    ),
    (
        LOCAL_DATA / "ecommerce/olist_order_payments_dataset.csv",
        "ecommerce_order_payments",
        [],
    ),
    (
        LOCAL_DATA / "ecommerce/olist_order_reviews_dataset.csv",
        "ecommerce_order_reviews",
        ["review_creation_date", "review_answer_timestamp"],
    ),
    (
        LOCAL_DATA / "ecommerce/olist_products_dataset.csv",
        "ecommerce_products",
        [],
    ),
    (
        LOCAL_DATA / "ecommerce/olist_sellers_dataset.csv",
        "ecommerce_sellers",
        [],
    ),
    (
        LOCAL_DATA / "ecommerce/olist_customers_dataset.csv",
        "ecommerce_customers",
        [],
    ),
    (
        LOCAL_DATA / "ecommerce/olist_geolocation_dataset.csv",
        "ecommerce_geolocation",
        [],
    ),
    (
        LOCAL_DATA / "marketing/olist_marketing_qualified_leads_dataset.csv",
        "marketing_leads",
        ["first_contact_date"],
    ),
    (
        LOCAL_DATA / "marketing/olist_closed_deals_dataset.csv",
        "marketing_deals",
        ["won_date"],
    ),
]

# ── Hàm ingest 1 table ────────────────────────────────────────────────────────

def ingest_table(
    csv_path: Path,
    table_name: str,
    timestamp_cols: list[str],
    overwrite: bool = False,
) -> int:
    full_name = f"bronze.{table_name}"
    print(f"\n{'─' * 60}")
    print(f"  {csv_path.name}  →  {full_name}")

    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  Loaded: {len(df):,} rows × {len(df.columns)} cols")

    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Metadata columns (tiêu chuẩn Bronze)
    df["_ingested_at"] = INGESTED_AT
    df["_source_file"] = csv_path.name

    arrow_tbl = pa.Table.from_pandas(df, preserve_index=False)

    if overwrite and catalog.table_exists(full_name):
        catalog.drop_table(full_name)
        print(f"  Dropped existing table (--overwrite).")

    if catalog.table_exists(full_name):
        tbl = catalog.load_table(full_name)
        print(f"  Table exists → appending.")
    else:
        tbl = catalog.create_table(
            identifier=full_name,
            schema=arrow_tbl.schema,
            location=f"s3://olist-lakehouse/bronze/{table_name}",
            partition_spec=PartitionSpec(),   # Bronze không partition
        )
        print(f"  Table created.")

    tbl.append(arrow_tbl)
    print(f"  Written: {len(df):,} rows")
    return len(df)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest Olist CSV → Bronze Iceberg on R2")
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Drop và tạo lại bảng thay vì append (dùng khi reset hoặc schema đổi)",
    )
    parser.add_argument(
        "--table", metavar="NAME",
        help="Chỉ ingest 1 bảng cụ thể (vd: ecommerce_orders)",
    )
    args = parser.parse_args()

    # Tạo namespace nếu chưa có
    try:
        catalog.create_namespace("bronze")
        print("Namespace 'bronze' created.")
    except Exception:
        print("Namespace 'bronze' already exists.")

    # Lọc datasets cần ingest
    targets = DATASETS
    if args.table:
        targets = [d for d in DATASETS if d[1] == args.table]
        if not targets:
            print(f"Table '{args.table}' not found. Available:")
            for _, name, _ in DATASETS:
                print(f"  {name}")
            sys.exit(1)

    print("=" * 60)
    print(f"BRONZE INGESTION — {len(targets)} table(s)")
    print(f"Mode: {'OVERWRITE' if args.overwrite else 'APPEND'}")
    print("=" * 60)

    total_rows = 0
    for csv_path, table_name, ts_cols in targets:
        rows = ingest_table(csv_path, table_name, ts_cols, overwrite=args.overwrite)
        total_rows += rows

    # Tổng kết
    print("\n" + "=" * 60)
    print(f"DONE — {total_rows:,} total rows written")
    print("\n=== Bronze tables ===")
    for table_id in catalog.list_tables("bronze"):
        print(f"  bronze.{table_id[1]}")


if __name__ == "__main__":
    main()
```

### Cách chạy

```bash
# Ingest toàn bộ 10 tables (append)
python scripts/batch_ingest_bronze.py

# Reset hoàn toàn rồi ingest lại
python scripts/batch_ingest_bronze.py --overwrite

# Chỉ ingest 1 table cụ thể
python scripts/batch_ingest_bronze.py --table ecommerce_orders

# Kết hợp: reset 1 table
python scripts/batch_ingest_bronze.py --table marketing_deals --overwrite
```

**Output mong đợi (cuối log):**

```
============================================================
DONE — 1,617,743 total rows written

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

## 6. Kiểm tra kết quả

### 6.1 Xác nhận raw CSV trên R2

```bash
# Dùng AWS CLI (cần cài: pip install awscli)
aws s3 ls s3://olist-lakehouse/raw/ \
  --endpoint-url $S3_ENDPOINT \
  --region auto \
  --recursive \
  --human-readable \
  --summarize
```

Phải thấy **10 files** với tổng kích thước ~100 MB.

Hoặc kiểm tra bằng Python:

```python
import os, boto3
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

### 6.2 Xác nhận Bronze Iceberg tables

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

print(f"{'Table':45s}  {'Rows':>10}")
print("─" * 58)
for table_id in catalog.list_tables("bronze"):
    tbl = catalog.load_table(table_id)
    df = tbl.scan().to_pandas()
    print(f"{'bronze.' + table_id[1]:45s}  {len(df):>10,}")
```

**Kết quả mong đợi:**

```
Table                                              Rows
──────────────────────────────────────────────────────────
bronze.ecommerce_customers                        99,441
bronze.ecommerce_geolocation               1,000,163
bronze.ecommerce_order_items                     112,650
bronze.ecommerce_order_payments                  103,886
bronze.ecommerce_order_reviews                    99,224
bronze.ecommerce_orders                           99,441
bronze.ecommerce_products                         32,951
bronze.ecommerce_sellers                           3,095
bronze.marketing_deals                               842
bronze.marketing_leads                             8,000
```

### 6.3 Xác nhận cấu trúc Iceberg trên R2

```bash
# Kiểm tra thư mục bronze/ trên R2
aws s3 ls s3://olist-lakehouse/bronze/ \
  --endpoint-url $S3_ENDPOINT \
  --region auto

# Xem metadata của 1 table cụ thể
aws s3 ls s3://olist-lakehouse/bronze/ecommerce_orders/ \
  --endpoint-url $S3_ENDPOINT \
  --region auto \
  --recursive
```

Mỗi Iceberg table có cấu trúc:

```
bronze/ecommerce_orders/
├── data/
│   └── *.parquet          ← dữ liệu thực
└── metadata/
    ├── v1.metadata.json   ← schema + partition spec
    ├── snap-*.avro        ← snapshot manifest
    └── *.avro             ← manifest files
```

### 6.4 Kiểm tra qua Iceberg REST API

```bash
# Danh sách namespaces
curl -s http://localhost:8181/v1/namespaces | python -m json.tool

# Danh sách tables trong bronze
curl -s http://localhost:8181/v1/namespaces/bronze/tables | python -m json.tool

# Schema của 1 table
curl -s http://localhost:8181/v1/namespaces/bronze/tables/ecommerce_orders | python -m json.tool
```

---

## 7. Xử lý lỗi thường gặp

### `NoCredentialsError` hoặc `403 Forbidden`

```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```

**Nguyên nhân:** Biến môi trường R2 chưa load.

```bash
# Kiểm tra .env đã có đủ chưa:
python -c "from dotenv import load_dotenv; import os; load_dotenv(); print(os.environ.get('S3_ENDPOINT', 'NOT SET'))"
```

**Sửa:** Đảm bảo file `.env` đặt đúng thư mục gốc project và đã điền `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`.

---

### `NoSuchBucket` khi upload

```
botocore.exceptions.ClientError: An error occurred (NoSuchBucket)
```

**Nguyên nhân:** Bucket `olist-lakehouse` chưa tạo trên Cloudflare dashboard.

**Sửa:** Vào Cloudflare → R2 → Create bucket → đặt tên `olist-lakehouse`. Xem `SETUP.md` mục 3.2.

---

### `ConnectionError: http://localhost:8181`

```
pyiceberg.exceptions.NoSuchIcebergTableError: ...
requests.exceptions.ConnectionError: HTTPConnectionPool(host='localhost', port=8181)
```

**Nguyên nhân:** Iceberg REST catalog chưa khởi động.

```bash
# Khởi động lại:
docker compose up -d iceberg-rest

# Xem log nếu crash:
docker logs olist-iceberg-rest --tail 50
```

---

### `ArrowInvalid` hoặc schema mismatch khi append

```
pyarrow.lib.ArrowInvalid: Schema at index 0 was different
```

**Nguyên nhân:** Table đã tạo với schema cũ, CSV source có schema mới (thêm/đổi cột).

**Sửa:** Ingest lại với `--overwrite`:

```bash
python scripts/batch_ingest_bronze.py --table <tên_table> --overwrite
```

---

### Upload chậm / timeout với file lớn (geolocation ~18 MB)

**Sửa:** Dùng multipart upload qua `boto3.s3.transfer`:

```python
from boto3.s3.transfer import TransferConfig

config = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,   # 8 MB
    multipart_chunksize=8 * 1024 * 1024,
    max_concurrency=4,
)
s3.upload_file(
    Filename=str(csv_file),
    Bucket=BUCKET,
    Key=s3_key,
    Config=config,
)
```

---

## Thứ tự chạy tóm tắt

```bash
# 0. Đảm bảo stack đang chạy
docker compose up -d
docker compose ps   # đợi iceberg-rest: (healthy)

# 1. Tải data từ Kaggle (chỉ cần 1 lần)
python -c "import kaggle" 2>/dev/null || pip install kaggle
kaggle datasets download olistbr/brazilian-ecommerce --path data/raw/ecommerce --unzip
kaggle datasets download olistbr/marketing-funnel-olist --path data/raw/marketing --unzip

# 2. Upload raw CSV lên R2 (backup archive)
python scripts/upload_raw_to_r2.py

# 3. Ingest vào Bronze Iceberg
python scripts/batch_ingest_bronze.py

# 4. Kiểm tra kết quả
curl -s http://localhost:8181/v1/namespaces/bronze/tables | python -m json.tool
```

Sau khi hoàn thành, Bronze layer đã sẵn sàng để dbt transform lên Silver → Gold.  
Xem `DATA_INGESTION.md` mục 7 nếu muốn thêm streaming qua Redpanda.
