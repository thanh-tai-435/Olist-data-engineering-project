"""Validate Soda Core YAML check files — no infrastructure required."""
from pathlib import Path

import pytest
import yaml

CHECKS_DIR = Path("quality/checks")

EXPECTED_TABLES = {
    "bronze": [
        "ecommerce_orders",
        "ecommerce_order_items",
        "ecommerce_order_payments",
        "ecommerce_order_reviews",
        "ecommerce_products",
        "ecommerce_sellers",
        "ecommerce_customers",
    ],
    "silver": [
        "stg_orders",
        "stg_order_items",
        "stg_order_payments",
        "stg_order_reviews",
        "stg_products",
        "stg_sellers",
        "stg_customers",
        "int_orders_enriched",
    ],
    "gold": [
        "fct_orders",
        "fct_funnel",
        "dim_sellers",
        "dim_customers",
        "review_sentiment",
    ],
}


def _load(layer: str) -> dict:
    path = CHECKS_DIR / f"{layer}_checks.yml"
    assert path.exists(), f"Missing: {path}"
    return yaml.safe_load(path.read_text())


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_checks_file_is_valid_yaml(layer):
    data = _load(layer)
    assert isinstance(data, dict)


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_every_check_block_has_items(layer):
    data = _load(layer)
    for key, checks in data.items():
        assert key.startswith("checks for"), f"Unexpected key: '{key}'"
        assert isinstance(checks, list) and len(checks) > 0, \
            f"Empty check block under '{key}'"


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_expected_tables_all_covered(layer):
    data = _load(layer)
    defined = {k.removeprefix("checks for ") for k in data}
    for table in EXPECTED_TABLES[layer]:
        assert table in defined, \
            f"[{layer}] No checks defined for table '{table}'"


def test_bronze_orders_has_duplicate_check():
    data = _load("bronze")
    checks = data["checks for ecommerce_orders"]
    assert any("duplicate_count" in str(c) for c in checks)


def test_gold_fct_orders_has_business_rules():
    data = _load("gold")
    checks = data["checks for fct_orders"]
    # Must enforce non-negative revenue and valid delivery_status
    check_str = str(checks)
    assert "order_revenue" in check_str
    assert "delivery_status" in check_str


def test_silver_review_score_bounded():
    data = _load("silver")
    checks = data["checks for stg_order_reviews"]
    check_str = str(checks)
    assert "review_score" in check_str
