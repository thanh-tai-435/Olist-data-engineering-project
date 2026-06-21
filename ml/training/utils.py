"""Shared utilities for ML training scripts."""
import logging
import os

import mlflow
import pandas as pd
from pyiceberg.catalog import load_catalog

log = logging.getLogger(__name__)


def get_catalog():
    return load_catalog("rest", **{
        "uri":                   os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
        "s3.endpoint":           os.environ["S3_ENDPOINT"],
        "s3.access-key-id":      os.environ["S3_ACCESS_KEY"],
        "s3.secret-access-key":  os.environ["S3_SECRET_KEY"],
        "s3.region":             os.environ.get("S3_REGION", "auto"),
        "s3.path-style-access":  "false",
    })


def load_gold_table(table_name: str) -> pd.DataFrame:
    # Strip catalog prefix if present (e.g. "olist.gold.fct_orders" → "gold.fct_orders")
    parts = table_name.split(".")
    if len(parts) == 3:
        table_name = ".".join(parts[1:])
    catalog = get_catalog()
    log.info("Loading %s ...", table_name)
    df = catalog.load_table(table_name).scan().to_pandas()
    log.info("  → %d rows, %d cols", len(df), len(df.columns))
    return df


def setup_mlflow() -> None:
    uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(uri)
    log.info("MLflow tracking URI: %s", uri)
