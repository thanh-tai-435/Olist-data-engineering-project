"""
Soda Core runner: load Iceberg tables → temp DuckDB file → run checks.

Usage:
    python soda_runner.py --layer bronze
    python soda_runner.py --layer silver
    python soda_runner.py --layer gold
"""
import argparse
import logging
import os
import tempfile
from pathlib import Path

import duckdb
from pyiceberg.catalog import load_catalog
from soda.scan import Scan

log = logging.getLogger(__name__)

CHECKS_DIR = Path(__file__).parent / "checks"

# Iceberg table names per layer → DuckDB table name used in checks YAML
LAYER_TABLES: dict[str, dict[str, str]] = {
    "bronze": {
        "ecommerce_orders":        "bronze.ecommerce_orders",
        "ecommerce_order_items":   "bronze.ecommerce_order_items",
        "ecommerce_order_payments":"bronze.ecommerce_order_payments",
        "ecommerce_order_reviews": "bronze.ecommerce_order_reviews",
        "ecommerce_products":      "bronze.ecommerce_products",
        "ecommerce_sellers":       "bronze.ecommerce_sellers",
        "ecommerce_customers":     "bronze.ecommerce_customers",
    },
    "silver": {
        "stg_orders":           "silver.stg_orders",
        "stg_order_items":      "silver.stg_order_items",
        "stg_order_payments":   "silver.stg_order_payments",
        "stg_order_reviews":    "silver.stg_order_reviews",
        "stg_products":         "silver.stg_products",
        "stg_sellers":          "silver.stg_sellers",
        "stg_customers":        "silver.stg_customers",
        "int_orders_enriched":  "silver.int_orders_enriched",
    },
    "gold": {
        "fct_orders":      "gold.fct_orders",
        "fct_funnel":      "gold.fct_funnel",
        "dim_sellers":     "gold.dim_sellers",
        "dim_customers":   "gold.dim_customers",
        "review_sentiment": "gold.review_sentiment",
    },
}


def _get_catalog():
    return load_catalog("rest", **{
        "uri":                  os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
        "s3.endpoint":          os.environ["S3_ENDPOINT"],
        "s3.access-key-id":     os.environ["S3_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
        "s3.region":            os.environ.get("S3_REGION", "auto"),
        "s3.path-style-access": "false",
    })


def _build_duckdb(tables: dict[str, str], catalog) -> str:
    """Load Iceberg tables into a temp DuckDB file, return its path."""
    tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)
    tmp.close()
    os.unlink(tmp.name)  # DuckDB requires a non-existent path to create a fresh DB
    con = duckdb.connect(tmp.name)
    for tbl_name, iceberg_name in tables.items():
        log.info("  Loading %s ...", iceberg_name)
        df = catalog.load_table(iceberg_name).scan().to_pandas()
        con.register("_df", df)
        con.execute(f'CREATE TABLE "{tbl_name}" AS SELECT * FROM _df')
        log.info("    → %d rows", len(df))
    con.close()
    return tmp.name


def run_checks(layer: str) -> dict:
    """Run Soda checks for a layer. Returns result summary dict."""
    if layer not in LAYER_TABLES:
        raise ValueError(f"Unknown layer '{layer}'. Choose: {list(LAYER_TABLES)}")

    log.info("=== Soda quality check: %s ===", layer.upper())
    catalog = _get_catalog()
    db_path = _build_duckdb(LAYER_TABLES[layer], catalog)

    # Forward slashes required in YAML on all platforms
    db_path_yaml = db_path.replace("\\", "/")
    checks_file  = CHECKS_DIR / f"{layer}_checks.yml"

    scan = Scan()
    scan.set_data_source_name("duckdb")
    scan.add_configuration_yaml_str(f"""
data_source duckdb:
  type: duckdb
  path: "{db_path_yaml}"
""")
    scan.add_sodacl_yaml_file(str(checks_file))
    scan.execute()

    # Clean up temp file
    try:
        os.unlink(db_path)
    except OSError:
        pass

    from soda.execution.check_outcome import CheckOutcome  # noqa: PLC0415
    failed  = sum(1 for c in scan._checks if c.outcome == CheckOutcome.FAIL)
    warned  = sum(1 for c in scan._checks if c.outcome == CheckOutcome.WARN)
    passed  = sum(1 for c in scan._checks if c.outcome == CheckOutcome.PASS)
    total   = len(scan._checks)
    result = {
        "layer":   layer,
        "passed":  passed,
        "warned":  warned,
        "failed":  failed,
        "errored": 0,
        "total":   total,
    }

    log.info(
        "  ✓ %d passed  ⚠ %d warned  ✗ %d failed  (total %d)",
        result["passed"], result["warned"], result["failed"], result["total"],
    )

    return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True)
    args = parser.parse_args()
    run_checks(args.layer)
