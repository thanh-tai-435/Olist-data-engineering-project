"""
Batch ingest: CSV từ R2 raw/ → Bronze Iceberg tables trên R2.
Catalog: Iceberg REST tại localhost:8181
Tables: bronze.ecommerce_* và bronze.marketing_*

Mỗi lần chạy APPEND thêm data (không ghi đè).
Dùng cờ --overwrite để drop-and-recreate bảng nếu cần reset.

Yêu cầu: upload_raw_to_r2.py đã chạy trước (raw CSV phải có trên R2).

Cách dùng:
  python scripts/batch_ingest_bronze.py                          # ingest tất cả (append)
  python scripts/batch_ingest_bronze.py --overwrite              # reset rồi ingest lại
  python scripts/batch_ingest_bronze.py --table ecommerce_orders # chỉ 1 table
  python scripts/batch_ingest_bronze.py --table ecommerce_orders --overwrite
"""
import io
import os
import sys
import argparse
import boto3
import pandas as pd
import pyarrow as pa
from datetime import datetime, timezone
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec

load_dotenv()

# ── Kết nối R2 (đọc raw CSV) ─────────────────────────────────────────────────

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name=os.environ.get("S3_REGION", "auto"),
)

BUCKET = "retail-data-lake"
ICEBERG_URI = "http://localhost:8181"

# catalog khởi tạo lazy trong main() để lỗi kết nối được bắt rõ ràng
catalog = None

# ── Config ingestion ──────────────────────────────────────────────────────────

INGESTED_AT = datetime.now(timezone.utc).isoformat()

# (r2_key, iceberg_table_name, timestamp_columns)
DATASETS = [
    (
        "raw/ecommerce/olist_orders_dataset.csv",
        "ecommerce_orders",
        ["order_purchase_timestamp", "order_approved_at",
         "order_delivered_carrier_date", "order_delivered_customer_date",
         "order_estimated_delivery_date"],
    ),
    (
        "raw/ecommerce/olist_order_items_dataset.csv",
        "ecommerce_order_items",
        ["shipping_limit_date"],
    ),
    (
        "raw/ecommerce/olist_order_payments_dataset.csv",
        "ecommerce_order_payments",
        [],
    ),
    (
        "raw/ecommerce/olist_order_reviews_dataset.csv",
        "ecommerce_order_reviews",
        ["review_creation_date", "review_answer_timestamp"],
    ),
    (
        "raw/ecommerce/olist_products_dataset.csv",
        "ecommerce_products",
        [],
    ),
    (
        "raw/ecommerce/olist_sellers_dataset.csv",
        "ecommerce_sellers",
        [],
    ),
    (
        "raw/ecommerce/olist_customers_dataset.csv",
        "ecommerce_customers",
        [],
    ),
    (
        "raw/ecommerce/olist_geolocation_dataset.csv",
        "ecommerce_geolocation",
        [],
    ),
    (
        "raw/marketing/olist_marketing_qualified_leads_dataset.csv",
        "marketing_leads",
        ["first_contact_date"],
    ),
    (
        "raw/marketing/olist_closed_deals_dataset.csv",
        "marketing_deals",
        ["won_date"],
    ),
]

# ── Hàm đọc CSV từ R2 ────────────────────────────────────────────────────────

def read_csv_from_r2(r2_key: str) -> pd.DataFrame:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=r2_key)
    except s3.exceptions.NoSuchKey:
        raise FileNotFoundError(f"s3://{BUCKET}/{r2_key} not found. Run upload_raw_to_r2.py first.")
    return pd.read_csv(io.BytesIO(obj["Body"].read()), low_memory=False)


# ── Hàm ingest 1 table ────────────────────────────────────────────────────────

def ingest_table(
    r2_key: str,
    table_name: str,
    timestamp_cols: list[str],
    overwrite: bool = False,
) -> int:
    full_name = f"bronze.{table_name}"
    filename = r2_key.split("/")[-1]
    print(f"\n{'-' * 60}")
    print(f"  s3://{BUCKET}/{r2_key}  ->  {full_name}")

    try:
        df = read_csv_from_r2(r2_key)
    except FileNotFoundError as e:
        print(f"  [SKIP] {e}")
        return 0

    print(f"  Loaded: {len(df):,} rows × {len(df.columns)} cols")

    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").astype("datetime64[us]")

    # Metadata columns (tiêu chuẩn Bronze layer)
    df["_ingested_at"] = INGESTED_AT
    df["_source_file"] = filename
    df["_source_path"] = f"s3://{BUCKET}/{r2_key}"

    arrow_tbl = pa.Table.from_pandas(df, preserve_index=False)

    if overwrite and catalog.table_exists(full_name):
        catalog.drop_table(full_name)
        print(f"  Dropped existing table (--overwrite).")

    if catalog.table_exists(full_name):
        tbl = catalog.load_table(full_name)
        print(f"  Table exists -> appending.")
    else:
        tbl = catalog.create_table(
            identifier=full_name,
            schema=arrow_tbl.schema,
            location=f"s3://{BUCKET}/bronze/{table_name}",
            partition_spec=PartitionSpec(),
        )
        print(f"  Table created.")

    tbl.append(arrow_tbl)
    print(f"  Written: {len(df):,} rows")
    return len(df)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest CSV từ R2 raw/ → Bronze Iceberg")
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Drop và tạo lại bảng thay vì append",
    )
    parser.add_argument(
        "--table", metavar="NAME",
        help="Chỉ ingest 1 bảng (vd: ecommerce_orders)",
    )
    args = parser.parse_args()

    # ── Kết nối Iceberg REST catalog (lazy) ──────────────────────────────────
    global catalog
    try:
        catalog = load_catalog(
            "rest",
            **{
                "uri": ICEBERG_URI,
                "s3.endpoint": os.environ["S3_ENDPOINT"],
                "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
                "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
                "s3.region": os.environ.get("S3_REGION", "auto"),
                "s3.path-style-access": "false",
            },
        )
    except Exception as e:
        print(f"[ERROR] Cannot connect to Iceberg REST catalog at {ICEBERG_URI}")
        print(f"        Make sure Docker stack is running: docker compose up -d")
        print(f"        Then verify: curl http://localhost:8181/v1/config")
        print(f"        Detail: {e}")
        sys.exit(1)

    # Tạo namespace nếu chưa có
    try:
        catalog.create_namespace("bronze")
        print("Namespace 'bronze' created.")
    except Exception:
        print("Namespace 'bronze' already exists.")

    # Lọc datasets
    targets = DATASETS
    if args.table:
        targets = [d for d in DATASETS if d[1] == args.table]
        if not targets:
            print(f"Table '{args.table}' not found. Available tables:")
            for _, name, _ in DATASETS:
                print(f"  {name}")
            sys.exit(1)

    print("=" * 60)
    print(f"BRONZE INGESTION — {len(targets)} table(s)")
    print(f"Source bucket: s3://{BUCKET}/raw/")
    print(f"Mode: {'OVERWRITE' if args.overwrite else 'APPEND'}")
    print("=" * 60)

    total_rows = 0
    for r2_key, table_name, ts_cols in targets:
        rows = ingest_table(r2_key, table_name, ts_cols, overwrite=args.overwrite)
        total_rows += rows

    print("\n" + "=" * 60)
    print(f"DONE — {total_rows:,} total rows written")
    print("\n=== Bronze tables ===")
    for table_id in catalog.list_tables("bronze"):
        print(f"  bronze.{table_id[1]}")


if __name__ == "__main__":
    main()
