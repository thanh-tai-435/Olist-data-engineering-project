"""
Flow: Bronze Batch Ingestion
Đọc CSV từ data/raw/ → validate → ghi vào Bronze Iceberg tables trên R2.
"""
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
from prefect.artifacts import create_table_artifact
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from prefect import flow, get_run_logger, task

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
def ingest_csv(table_name: str, csv_path: Path, timestamp_cols: list[str] = None, overwrite: bool = False) -> dict:
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

    # PyIceberg chỉ hỗ trợ timestamp[us], pandas mặc định ra timestamp[ns]
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].astype("datetime64[us]")

    df["_ingested_at"] = INGESTED_AT
    df["_source_file"] = csv_path.name
    df["_source_path"] = str(csv_path)
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    catalog = get_catalog()

    try:
        tbl = catalog.load_table(full_table)
        if overwrite:
            catalog.drop_table(full_table)
            log.info(f"  Dropped {full_table} (overwrite=True)")
            raise NoSuchTableError(full_table)
    except NoSuchTableError:
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
def bronze_ingestion_flow(data_root: str | None = None, overwrite: bool = False):
    root = Path(data_root) if data_root else DATA_ROOT
    log = get_run_logger()
    log.info(f"Data root: {root}  overwrite={overwrite}")

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

    # Submit all tables in parallel — Prefect runs them concurrently
    futures = [
        ingest_csv.submit(table_name, csv_path, ts_cols, overwrite)
        for table_name, csv_path, ts_cols in datasets
    ]
    results, fail_count = [], 0
    for (table_name, *_), fut in zip(datasets, futures):
        try:
            results.append(fut.result())
        except Exception as e:
            log.error("Ingestion failed for %s: %s", table_name, e)
            fail_count += 1
    if fail_count:
        raise RuntimeError(f"{fail_count}/{len(datasets)} table ingestions failed.")

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
