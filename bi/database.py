"""DuckDB + PyIceberg connection, schema context builder, KPI queries."""
import duckdb
import pandas as pd
import streamlit as st
from pyiceberg.catalog import load_catalog
from config import ICEBERG_URI, S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION, GOLD_TABLES


# ── Column descriptions — explicit BRL units prevent LLM hallucination ─────────

_COL_DESC: dict[str, str] = {
    # fct_orders ─────────────────────────────────────────────────────────────
    "order_id":                "Unique order identifier (UUID string)",
    "customer_id":             "Per-order customer ID — same person may have many",
    "customer_unique_id":      "Unique person ID across all orders",
    "order_status":            "Status: delivered | shipped | canceled | processing | unavailable",
    "order_date":              "Date of purchase (DATE type, format YYYY-MM-DD)",
    "order_month":             "Purchase month as 'YYYY-MM' string",
    "purchased_at":            "Full purchase timestamp (TIMESTAMP)",
    "actual_delivery_days":    "Days from purchase to actual delivery (INTEGER)",
    "estimated_delivery_days": "Days from purchase to promised delivery (INTEGER)",
    "delivery_delay_days":     "actual_delivery_days minus estimated_delivery_days — positive = late",
    "delivery_status":         "early | on_time | late | unknown",
    "customer_state":          "Brazilian state code of buyer (e.g. SP, RJ, MG)",
    "customer_city":           "City of buyer (string)",
    "order_revenue":           "Sum of item prices in BRL R$ — e.g. 150.00 means R$150",
    "order_freight":           "Freight cost in BRL R$ — e.g. 15.50 means R$15.50",
    "item_count":              "Number of items in the order (INTEGER)",
    "payment_value":           "Total payment in BRL R$ (includes freight) — e.g. 165.50 means R$165.50",
    "payment_type":            "Payment method: credit_card | boleto | debit_card | voucher",
    "review_score":            "Customer review score from 1 (worst) to 5 (best)",

    # fct_funnel ──────────────────────────────────────────────────────────────
    "mql_id":             "Marketing Qualified Lead unique ID",
    "first_contact_date": "Date of first contact with lead (DATE)",
    "origin":             "Lead acquisition channel (organic, paid_search, social, etc.)",
    "business_segment":   "Business category of the lead (furniture, electronics, etc.)",
    "lead_type":          "online_medium | direct_traffic | organic_search | ...",
    "days_to_close":      "Days from first contact to deal won (INTEGER, NULL if not converted)",
    "is_converted":       "1 = became a seller on Olist, 0 = lead not converted",

    # dim_sellers ─────────────────────────────────────────────────────────────
    "seller_id":        "Unique seller identifier (UUID string)",
    "seller_city":      "City where seller is located",
    "seller_state":     "Brazilian state code of seller (e.g. SP, PR, MG)",
    "total_orders":     "Total orders fulfilled by this seller (INTEGER)",
    "total_revenue":    "Total revenue generated in BRL R$ — e.g. 25000.00 means R$25,000",
    "avg_review_score": "Average customer review score across all seller's orders (FLOAT 1.0–5.0)",
    "delivered_orders": "Count of successfully delivered orders (INTEGER)",

    # dim_customers ───────────────────────────────────────────────────────────
    "total_spend":          "Total BRL R$ spent across all orders — e.g. 500.00 means R$500",
    "avg_order_value":      "Average order value in BRL R$ per purchase",
    "days_since_last_order": "Days elapsed since the last purchase (INTEGER)",
    "is_churned":           "1 = no order in last 90 days (churned), 0 = still active",
    "is_repeat_customer":   "1 = placed more than 1 order, 0 = one-time buyer",

    # review_sentiment ────────────────────────────────────────────────────────
    "review_id":                   "Unique review identifier",
    "review_text":                 "Raw review comment text (Portuguese)",
    "overall_sentiment":           "Overall sentiment: negative | neutral | positive",
    "overall_confidence":          "Model confidence score for overall sentiment (0.0–1.0)",
    "product_quality_sentiment":   "Sentiment about product quality: negative | neutral | positive",
    "product_quality_confidence":  "Confidence for product_quality prediction (0.0–1.0)",
    "delivery_speed_sentiment":    "Sentiment about delivery speed: negative | neutral | positive",
    "delivery_speed_confidence":   "Confidence for delivery_speed prediction (0.0–1.0)",
    "seller_service_sentiment":    "Sentiment about seller service: negative | neutral | positive",
    "seller_service_confidence":   "Confidence for seller_service prediction (0.0–1.0)",
    "price_value_sentiment":       "Sentiment about price/value: negative | neutral | positive",
    "price_value_confidence":      "Confidence for price_value prediction (0.0–1.0)",
    "model_version":               "ML model version used for scoring (e.g. v1)",
    "scored_at":                   "Timestamp when the review was scored (UTC)",
}


@st.cache_resource(show_spinner="Connecting to Iceberg Gold tables...")
def get_db_connection() -> tuple[duckdb.DuckDBPyConnection, list, list]:
    """Load Gold tables from Iceberg into DuckDB in-memory views."""
    catalog = load_catalog(
        "rest",
        uri=ICEBERG_URI,
        **{
            "s3.endpoint":          S3_ENDPOINT,
            "s3.access-key-id":     S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region":            S3_REGION,
        },
    )
    con = duckdb.connect()
    loaded, errors = [], []
    for tbl in GOLD_TABLES:
        try:
            df = catalog.load_table(f"gold.{tbl}").scan().to_pandas()
            con.register(tbl, df)
            loaded.append((tbl, len(df)))
        except Exception as e:
            errors.append((tbl, str(e)))
    return con, loaded, errors


def build_schema_context(con: duckdb.DuckDBPyConnection, loaded: list) -> str:
    """Return schema string with column types, descriptions, and BRL unit notes."""
    header = (
        "NOTE: All monetary values are in Brazilian Real (BRL, R$).\n"
        "Examples: payment_value=165.50 means R$165.50, total_revenue=25000 means R$25,000.\n"
        "When reporting monetary totals, always state 'R$' and format large numbers "
        "(>1M → 'X triệu R$', >1B → 'X tỷ R$').\n\n"
    )
    parts = []
    for tbl, row_count in loaded:
        try:
            desc = con.execute(f"DESCRIBE {tbl}").fetchdf()
            cols = []
            for _, row in desc.iterrows():
                name, dtype = row["column_name"], row["column_type"]
                note = _COL_DESC.get(name, "")
                cols.append(f"  {name} {dtype}" + (f"  -- {note}" if note else ""))
            parts.append(f"TABLE {tbl}  ({row_count:,} rows)\n" + "\n".join(cols))
        except Exception:
            parts.append(f"TABLE {tbl}  ({row_count:,} rows)  [schema unavailable]")
    return header + "\n\n".join(parts)


def get_kpi_metrics(con: duckdb.DuckDBPyConnection) -> dict:
    """Quick KPI summary for sidebar display."""
    try:
        row = con.execute("""
            SELECT
                COUNT(*)                                        AS total_orders,
                ROUND(SUM(payment_value), 2)                   AS total_revenue,
                ROUND(AVG(review_score), 2)                    AS avg_review,
                COUNT(DISTINCT customer_unique_id)             AS unique_customers
            FROM fct_orders
            WHERE order_status = 'delivered'
        """).fetchone()
        return {
            "total_orders":      f"{row[0]:,}",
            "total_revenue":     f"R$ {row[1]:,.0f}",
            "avg_review":        f"{row[2]:.2f} ⭐",
            "unique_customers":  f"{row[3]:,}",
        }
    except Exception:
        return {}
