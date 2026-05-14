"""
Flow: Bronze Batch Ingestion
Đọc CSV từ data/raw/ → validate → ghi vào Bronze Iceberg tables trên R2.
"""
import os
import pandas as pd
import pyarrow as pa
from pathlib import Path
from datetime import datetime, timezone
from pyiceberg.catalog import load_catalog
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact


DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/app/data/raw"))
ICEBERG_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
INGESTED_AT = datetime.now(timezone.utc).isoformat()


def get_catalog():
    return load_catalog("rest", **{
        "uri": ICEBERG_URI,
        "s3.endpoint": os.environ["S3_ENDPOINT"],
        "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
        "s3.region": os.environ.get("S3_REGION", "auto"),
        "s3.path-style-access": "false",
    })


@task(name="ensure-bronze-namespace", retries=2, retry_delay_seconds=5)
def ensure_namespace():
    log = get_run_logger()
    catalog = get_catalog()
    try:
        catalog.create_namespace("bronze")
        log.info("Namespace 'bronze' created.")
    except Exception:
        log.info("Namespace 'bronze' already exists.")
    return catalog


@task(name="ingest-csv-to-iceberg", retries=1, retry_delay_seconds=10)
def ingest_csv(table_name: str, csv_path: Path, timestamp_cols: list[str] = None) -> dict:
    log = get_run_logger()
    full_table = f"bronze.{table_name}"

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    log.info(f"Reading {csv_path.name} ...")
    df = pd.read_csv(csv_path, low_memory=False)

    if timestamp_cols:
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

    df["_ingested_at"] = INGESTED_AT
    df["_source_file"] = csv_path.name
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    catalog = get_catalog()
    if catalog.table_exists(full_table):
        tbl = catalog.load_table(full_table)
    else:
        tbl = catalog.create_table(
            identifier=full_table,
            schema=arrow_table.schema,
            location=f"s3://retail-data-lake/bronze/{table_name}",
        )

    tbl.append(arrow_table)
    log.info(f"  {len(df):,} rows → {full_table}")
    return {"table": full_table, "rows": len(df), "source": csv_path.name}


@flow(
    name="bronze-batch-ingestion",
    description="Ingest all Olist CSV files into Bronze Iceberg tables.",
    log_prints=True,
)
def bronze_ingestion_flow(data_root: str = None):
    root = Path(data_root) if data_root else DATA_ROOT
    log = get_run_logger()
    log.info(f"Data root: {root}")

    ensure_namespace()

    datasets = [
        ("ecommerce_orders",        root / "ecommerce/olist_orders_dataset.csv",
         ["order_purchase_timestamp", "order_approved_at",
          "order_delivered_carrier_date", "order_delivered_customer_date",
          "order_estimated_delivery_date"]),
        ("ecommerce_order_items",   root / "ecommerce/olist_order_items_dataset.csv",
         ["shipping_limit_date"]),
        ("ecommerce_order_payments",root / "ecommerce/olist_order_payments_dataset.csv", []),
        ("ecommerce_order_reviews", root / "ecommerce/olist_order_reviews_dataset.csv",
         ["review_creation_date", "review_answer_timestamp"]),
        ("ecommerce_products",      root / "ecommerce/olist_products_dataset.csv", []),
        ("ecommerce_sellers",       root / "ecommerce/olist_sellers_dataset.csv", []),
        ("ecommerce_customers",     root / "ecommerce/olist_customers_dataset.csv", []),
        ("ecommerce_geolocation",   root / "ecommerce/olist_geolocation_dataset.csv", []),
        ("marketing_leads",         root / "marketing/olist_marketing_qualified_leads_dataset.csv",
         ["first_contact_date"]),
        ("marketing_deals",         root / "marketing/olist_closed_deals_dataset.csv",
         ["won_date"]),
    ]

    results = []
    for table_name, csv_path, ts_cols in datasets:
        result = ingest_csv(table_name, csv_path, ts_cols)
        results.append(result)

    # Tạo summary artifact hiện trên Prefect UI
    create_table_artifact(
        key="bronze-ingestion-summary",
        table=results,
        description="Bronze layer ingestion results",
    )

    total_rows = sum(r["rows"] for r in results)
    log.info(f"DONE: {len(results)} tables, {total_rows:,} total rows ingested.")
    return results


if __name__ == "__main__":
    bronze_ingestion_flow()
