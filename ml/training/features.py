"""
Feature pipelines: Silver Iceberg → ML-ready DataFrames.

Each function reads directly from Silver layer and computes ML-specific
aggregations, temporal windows, and joins that Gold layer doesn't provide.

Pipelines:
  build_churn_features(catalog)    → DataFrame for customer-churn-xgb
  build_lead_features(catalog)     → DataFrame for lead-scoring-xgb
  build_delivery_features(catalog) → DataFrame for delivery-delay-xgb
"""
import logging

import pandas as pd

log = logging.getLogger(__name__)

CHURN_WINDOW_DAYS = 90


# ── Churn ─────────────────────────────────────────────────────────────────────

def build_churn_features(catalog) -> pd.DataFrame:
    """
    Temporal split approach (avoids label leakage):
      snapshot_date = max(purchased_at) - CHURN_WINDOW_DAYS
      features      = aggregations on orders BEFORE snapshot_date
      label         = did customer order in [snapshot_date, max_date]?

    Rolling-window features give the model recency + frequency signals
    that Gold's dim_customers (all-time aggregates) cannot provide.
    """
    log.info("Loading Silver tables for churn features...")

    orders = catalog.load_table("silver.stg_orders").scan().to_pandas()
    customers = catalog.load_table("silver.stg_customers").scan(
        selected_fields=("customer_id", "customer_unique_id", "customer_state")
    ).to_pandas()
    payments = catalog.load_table("silver.stg_order_payments").scan(
        selected_fields=("order_id", "payment_value")
    ).to_pandas()

    orders["purchased_at"] = pd.to_datetime(orders["purchased_at"])
    ref_date = orders["purchased_at"].max()
    snapshot_date = ref_date - pd.Timedelta(days=CHURN_WINDOW_DAYS)
    log.info("ref_date=%s  snapshot_date=%s", ref_date.date(), snapshot_date.date())

    df = (
        orders
        .merge(customers, on="customer_id", how="left")
        .merge(payments,  on="order_id",    how="left")
    )
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(0)

    # Label: customers who ordered in the churn window are NOT churned
    active_in_window = set(
        df.loc[df["purchased_at"] >= snapshot_date, "customer_unique_id"].dropna()
    )

    # Features: history before snapshot_date only
    hist = df[df["purchased_at"] < snapshot_date].copy()

    base = (
        hist.groupby("customer_unique_id")
        .agg(
            customer_state=("customer_state", "last"),
            total_orders=("order_id",       "nunique"),
            total_spend=("payment_value",   "sum"),
            avg_order_value=("payment_value", "mean"),
        )
        .reset_index()
    )

    # Days since last purchase (before snapshot) — recency without leakage
    recency = (
        hist.groupby("customer_unique_id")["purchased_at"]
        .max()
        .reset_index()
    )
    recency["days_since_last_order"] = (
        snapshot_date - recency["purchased_at"]
    ).dt.days

    # Rolling window order counts
    for window in [30, 90, 180]:
        cutoff = snapshot_date - pd.Timedelta(days=window)
        cnt = (
            hist[hist["purchased_at"] >= cutoff]
            .groupby("customer_unique_id")["order_id"]
            .nunique()
            .reset_index()
            .rename(columns={"order_id": f"orders_last_{window}d"})
        )
        base = base.merge(cnt, on="customer_unique_id", how="left")
        base[f"orders_last_{window}d"] = (
            base[f"orders_last_{window}d"].fillna(0).astype(int)
        )

    # Average days between consecutive orders (purchase frequency)
    def _avg_gap(dates: pd.Series) -> float:
        s = dates.sort_values()
        return s.diff().dt.days.mean() if len(s) >= 2 else 0.0

    freq = (
        hist.groupby("customer_unique_id")["purchased_at"]
        .apply(_avg_gap)
        .reset_index()
        .rename(columns={"purchased_at": "avg_days_between_orders"})
    )

    feat = (
        base
        .merge(recency[["customer_unique_id", "days_since_last_order"]],
               on="customer_unique_id", how="left")
        .merge(freq, on="customer_unique_id", how="left")
    )
    feat["is_repeat_customer"]    = (feat["total_orders"] > 1).astype(int)
    feat["avg_order_value"]       = feat["avg_order_value"].fillna(0)
    feat["days_since_last_order"] = feat["days_since_last_order"].fillna(9999)
    feat["avg_days_between_orders"] = feat["avg_days_between_orders"].fillna(0)

    feat["is_churned"] = (
        ~feat["customer_unique_id"].isin(active_in_window)
    ).astype(int)

    pos = int(feat["is_churned"].sum())
    neg = int((feat["is_churned"] == 0).sum())
    log.info(
        "Churn features: %d customers  churned=%d (%.1f%%)  active=%d (%.1f%%)",
        len(feat), pos, pos / len(feat) * 100, neg, neg / len(feat) * 100,
    )
    return feat


# ── Lead scoring ───────────────────────────────────────────────────────────────

def build_lead_features(catalog) -> pd.DataFrame:
    """
    Reads stg_marketing_leads + stg_marketing_deals from Silver.

    Only pre-conversion features used (post-conversion cols are null
    for non-converted leads and would cause leakage if included).
    Added: first_contact_dayofweek (Silver-level detail, not in Gold).
    """
    log.info("Loading Silver tables for lead scoring features...")

    leads = catalog.load_table("silver.stg_marketing_leads").scan().to_pandas()
    deals = catalog.load_table("silver.stg_marketing_deals").scan(
        selected_fields=("mql_id", "seller_id")
    ).to_pandas()

    first_contact = pd.to_datetime(leads["first_contact_date"], errors="coerce")
    leads["first_contact_month"]     = first_contact.dt.month.fillna(1).astype(int)
    leads["first_contact_dayofweek"] = first_contact.dt.dayofweek.fillna(0).astype(int)
    leads["origin"]                  = leads["origin"].fillna("unknown").astype(str)

    converted_ids = set(deals["mql_id"].dropna())
    leads["is_converted"] = leads["mql_id"].isin(converted_ids).astype(int)

    pos = int(leads["is_converted"].sum())
    neg = int((leads["is_converted"] == 0).sum())
    log.info(
        "Lead features: %d leads  converted=%d (%.1f%%)  not_converted=%d (%.1f%%)",
        len(leads), pos, pos / len(leads) * 100, neg, neg / len(leads) * 100,
    )
    return leads


# ── Delivery delay ─────────────────────────────────────────────────────────────

def build_delivery_features(catalog) -> pd.DataFrame:
    """
    Reads stg_orders + stg_order_items + stg_products + stg_sellers
    + stg_customers + stg_order_payments from Silver.

    New vs Gold:
      - seller_customer_same_state: proxy for shipping distance
      - n_product_categories: order diversity
      - avg_product_volume_cm3: bulkiness signal
    """
    log.info("Loading Silver tables for delivery features...")

    orders = catalog.load_table("silver.stg_orders").scan(
        selected_fields=(
            "order_id", "customer_id", "order_status",
            "purchased_at", "actual_delivery_days", "estimated_delivery_days",
        )
    ).to_pandas()

    items = catalog.load_table("silver.stg_order_items").scan(
        selected_fields=("order_id", "product_id", "seller_id", "price", "freight_value")
    ).to_pandas()

    products = catalog.load_table("silver.stg_products").scan(
        selected_fields=(
            "product_id", "product_category_name",
            "product_weight_g", "product_volume_cm3",
        )
    ).to_pandas()

    sellers = catalog.load_table("silver.stg_sellers").scan(
        selected_fields=("seller_id", "seller_state")
    ).to_pandas()

    customers = catalog.load_table("silver.stg_customers").scan(
        selected_fields=("customer_id", "customer_state")
    ).to_pandas()

    payments = catalog.load_table("silver.stg_order_payments").scan(
        selected_fields=("order_id", "payment_value", "payment_type", "max_installments")
    ).to_pandas()

    # Item-level join
    items_enriched = (
        items
        .merge(products, on="product_id", how="left")
        .merge(sellers,  on="seller_id",   how="left")
    )

    # Aggregate to order level
    items_agg = (
        items_enriched
        .groupby("order_id")
        .agg(
            item_count=("product_id",          "count"),
            total_weight_g=("product_weight_g", "sum"),
            order_revenue=("price",             "sum"),
            order_freight=("freight_value",     "sum"),
            avg_product_volume_cm3=("product_volume_cm3", "mean"),
            n_product_categories=("product_category_name", "nunique"),
            seller_state=("seller_state",       "first"),
        )
        .reset_index()
    )

    orders["purchased_at"] = pd.to_datetime(orders["purchased_at"])
    orders["order_month_num"] = orders["purchased_at"].dt.month

    df = (
        orders
        .merge(customers, on="customer_id",  how="left")
        .merge(items_agg, on="order_id",     how="left")
        .merge(payments,  on="order_id",     how="left")
    )

    # delivery_delay_days: positive = late, negative = early
    df["actual_delivery_days"]    = pd.to_numeric(df["actual_delivery_days"],    errors="coerce")
    df["estimated_delivery_days"] = pd.to_numeric(df["estimated_delivery_days"], errors="coerce")
    df["delivery_delay_days"] = df["actual_delivery_days"] - df["estimated_delivery_days"]

    # New feature: same-state → shorter distance → faster delivery
    df["seller_customer_same_state"] = (
        df["seller_state"] == df["customer_state"]
    ).astype(int)

    # Keep only delivered orders with a valid label
    df = df[
        (df["order_status"] == "delivered") &
        df["delivery_delay_days"].notna()
    ].copy()

    for col in ["total_weight_g", "max_installments", "avg_product_volume_cm3",
                "n_product_categories"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["seller_state"]   = df["seller_state"].fillna("unknown").astype(str)
    df["customer_state"] = df["customer_state"].fillna("unknown").astype(str)
    df["payment_type"]   = df["payment_type"].fillna("unknown").astype(str)
    df["max_installments"] = df["max_installments"].fillna(1)

    log.info("Delivery features: %d delivered orders", len(df))
    return df
