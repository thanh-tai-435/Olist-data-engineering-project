"""Shared utilities for sentiment scripts — runs on HOST (not in Docker worker)."""
import logging
import os

import mlflow
from pyiceberg.catalog import load_catalog

log = logging.getLogger(__name__)


def get_catalog():
    return load_catalog("rest", **{
        "uri":                  os.environ.get("ICEBERG_REST_URI", "http://localhost:8181"),
        "s3.endpoint":          os.environ["S3_ENDPOINT"],
        "s3.access-key-id":     os.environ["S3_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
        "s3.region":            os.environ.get("S3_REGION", "auto"),
        "s3.path-style-access": "false",
    })


def setup_mlflow() -> None:
    uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(uri)
    log.info("MLflow tracking URI: %s", uri)
