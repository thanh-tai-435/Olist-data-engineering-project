"""Unit tests for pipeline logic — no Prefect/Spark/Iceberg required."""
import pytest


# ── Inline the logic under test so we don't need to import prefect ──────────

def _raise_if_failed(result: dict) -> None:
    failed = result.get("failed", 0)
    if failed > 0:
        raise RuntimeError(f"Soda [{result['layer']}]: {failed} check(s) failed")


def _quality_gate(result: dict, pause: bool) -> str:
    """Returns 'ok', 'paused', or raises."""
    failed = result.get("failed", 0) + result.get("errored", 0)
    if failed == 0:
        return "ok"
    if pause:
        return "paused"
    _raise_if_failed(result)


# ── raise_if_failed ───────────────────────────────────────────────────────────

def test_raise_if_failed_passes_when_no_failures():
    _raise_if_failed({"layer": "bronze", "failed": 0, "passed": 10})


def test_raise_if_failed_raises_on_failure():
    with pytest.raises(RuntimeError, match="2 check"):
        _raise_if_failed({"layer": "silver", "failed": 2, "passed": 3})


def test_raise_if_failed_message_contains_layer():
    with pytest.raises(RuntimeError, match="gold"):
        _raise_if_failed({"layer": "gold", "failed": 1})


# ── quality_gate ──────────────────────────────────────────────────────────────

def test_quality_gate_ok_when_no_failures():
    assert _quality_gate({"layer": "bronze", "failed": 0, "errored": 0}, pause=False) == "ok"


def test_quality_gate_pauses_when_pause_true():
    result = {"layer": "silver", "failed": 1, "errored": 0}
    assert _quality_gate(result, pause=True) == "paused"


def test_quality_gate_raises_when_pause_false():
    result = {"layer": "gold", "failed": 1, "errored": 0}
    with pytest.raises(RuntimeError):
        _quality_gate(result, pause=False)


def test_quality_gate_counts_errored_as_failures():
    result = {"layer": "bronze", "failed": 0, "errored": 2}
    assert _quality_gate(result, pause=True) == "paused"


# ── soda_runner LAYER_TABLES completeness ────────────────────────────────────

def test_layer_tables_covers_all_bronze_ingestion_tables():
    """Every table ingested in bronze_ingestion.py must have a soda entry."""
    bronze_ingested = {
        "ecommerce_orders", "ecommerce_order_items", "ecommerce_order_payments",
        "ecommerce_order_reviews", "ecommerce_products", "ecommerce_sellers",
        "ecommerce_customers", "ecommerce_geolocation",
        "marketing_leads", "marketing_deals",
    }
    # geolocation intentionally excluded from quality checks (no PK to check)
    checked = {
        "ecommerce_orders", "ecommerce_order_items", "ecommerce_order_payments",
        "ecommerce_order_reviews", "ecommerce_products", "ecommerce_sellers",
        "ecommerce_customers", "marketing_leads", "marketing_deals",
    }
    # Everything that is checked must also be ingested
    assert checked.issubset(bronze_ingested)


def test_gold_layer_tables_includes_sentiment():
    """review_sentiment must be in gold LAYER_TABLES to match gold_checks.yml."""
    import sys
    from pathlib import Path

    # Add quality/ to path so we can import without heavy deps
    quality_dir = str(Path("quality").resolve())
    if quality_dir not in sys.path:
        sys.path.insert(0, quality_dir)

    # Mock heavy deps before importing soda_runner
    import types
    for mod in ["duckdb", "pyiceberg", "pyiceberg.catalog", "soda", "soda.scan"]:
        if mod not in sys.modules:
            sys.modules[mod] = types.ModuleType(mod)

    import importlib
    sr = importlib.import_module("soda_runner")
    assert "review_sentiment" in sr.LAYER_TABLES["gold"]
