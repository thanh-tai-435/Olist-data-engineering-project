"""
ML Predictions — Interactive simulators for 3 XGBoost models.

Tabs:
  📦 Delivery Delay   → will this order arrive late?
  👥 Churn Risk       → will this customer stop buying?
  🎯 Lead Scoring     → will this lead convert to a seller?
"""
import os
import sys

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import ICEBERG_URI, S3_ACCESS_KEY, S3_ENDPOINT, S3_REGION, S3_SECRET_KEY

ML_SERVING_URL = os.environ.get("ML_SERVING_URL", "http://ml-serving:8080")

BR_STATES = [
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA",
    "MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN",
    "RO","RR","RS","SC","SE","SP","TO",
]
PAYMENT_TYPES = ["credit_card", "boleto", "debit_card", "voucher"]
ORIGINS = [
    "organic_search", "paid_search", "social",
    "direct_traffic", "email", "other_publicities",
    "unknown",
]
DAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

st.set_page_config(
    page_title="ML Predictions",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  div[data-testid="metric-container"] {
    background:#1e1e2e; border-radius:10px; padding:12px;
  }
  .risk-high   { background:#450a0a; color:#fca5a5; padding:6px 14px;
                 border-radius:20px; font-weight:700; display:inline-block; }
  .risk-medium { background:#431407; color:#fdba74; padding:6px 14px;
                 border-radius:20px; font-weight:700; display:inline-block; }
  .risk-low    { background:#052e16; color:#86efac; padding:6px 14px;
                 border-radius:20px; font-weight:700; display:inline-block; }
  .insight-box {
    background:#1e293b; border-left:3px solid #3b82f6;
    border-radius:6px; padding:12px 16px; margin:8px 0;
    font-size:0.95rem; color:#e2e8f0; line-height:1.6;
  }
</style>
""", unsafe_allow_html=True)

st.title("🤖 ML Predictions")
st.caption(f"Models served via `{ML_SERVING_URL}` — XGBoost trained on Silver Iceberg layer")


# ── Health check ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def check_health() -> dict:
    try:
        r = requests.get(f"{ML_SERVING_URL}/health", timeout=5)
        return r.json()
    except Exception as e:
        return {"status": "unavailable", "error": str(e), "models": {}}


health = check_health()

if health.get("status") == "unavailable":
    st.error(
        f"**ML Serving API không khả dụng** — `{ML_SERVING_URL}`\n\n"
        "Khởi động:\n```\ndocker compose up -d ml-serving\n```"
    )
    st.stop()

model_status = health.get("models", {})
c1, c2, c3, c4 = st.columns(4)
status_color = "🟢" if health.get("status") == "ok" else "🟡"
c1.metric("API Status", f"{status_color} {health.get('status', '?').upper()}")
c2.metric("Delivery Model", "✅ Loaded" if model_status.get("delivery") else "❌ Not loaded")
c3.metric("Churn Model",    "✅ Loaded" if model_status.get("churn")    else "❌ Not loaded")
c4.metric("Lead Model",     "✅ Loaded" if model_status.get("lead")     else "❌ Not loaded")

st.divider()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _call(endpoint: str, payload: dict) -> dict | None:
    try:
        r = requests.post(
            f"{ML_SERVING_URL}{endpoint}",
            json=payload,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


def _gauge(value: float, min_: float, max_: float,
           label: str, unit: str = "",
           thresholds: list | None = None) -> go.Figure:
    steps = thresholds or [
        {"range": [min_, max_ * 0.33], "color": "#166534"},
        {"range": [max_ * 0.33, max_ * 0.66], "color": "#854d0e"},
        {"range": [max_ * 0.66, max_], "color": "#7f1d1d"},
    ]
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        number={"suffix": unit, "font": {"size": 36, "color": "#f8fafc"}},
        title={"text": label, "font": {"size": 14, "color": "#94a3b8"}},
        gauge={
            "axis": {"range": [min_, max_], "tickcolor": "#64748b",
                     "tickfont": {"color": "#94a3b8"}},
            "bar": {"color": "#3b82f6", "thickness": 0.25},
            "bgcolor": "#1e293b",
            "bordercolor": "#334155",
            "steps": steps,
        },
    ))
    fig.update_layout(
        height=240,
        margin=dict(l=20, r=20, t=40, b=10),
        paper_bgcolor="#0e1117",
        font_color="#f8fafc",
    )
    return fig


def _risk_badge(level: str) -> str:
    css = {"high": "risk-high", "medium": "risk-medium", "low": "risk-low"}
    icons = {"high": "🔴", "medium": "🟡", "low": "🟢"}
    return (
        f'<span class="{css.get(level, "risk-low")}">'
        f'{icons.get(level, "")} {level.upper()} RISK</span>'
    )


# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_delivery, tab_churn, tab_lead, tab_batch = st.tabs([
    "📦 Delivery Delay",
    "👥 Churn Risk",
    "🎯 Lead Scoring",
    "📊 Batch Insights",
])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — Delivery Delay
# ════════════════════════════════════════════════════════════════════════════
with tab_delivery:
    st.subheader("Delivery Delay Predictor")
    st.caption("Predict whether an order will arrive early, on time, or late.")

    left, right = st.columns([1, 1], gap="large")

    with left:
        st.markdown("**Order Details**")
        c_state   = st.selectbox("Customer State", BR_STATES, index=BR_STATES.index("SP"), key="d_cstate")
        s_state   = st.selectbox("Seller State",   BR_STATES, index=BR_STATES.index("MG"), key="d_sstate")

        if c_state == s_state:
            st.success("✓ Same state — shorter shipping distance")

        pay_type  = st.selectbox("Payment Type", PAYMENT_TYPES, key="d_pay")
        col_a, col_b = st.columns(2)
        item_count  = col_a.slider("Item Count",         1, 10, 2,    key="d_items")
        month_num   = col_b.slider("Order Month",        1, 12, 8,    key="d_month")
        weight      = col_a.number_input("Total Weight (g)",  100.0, 30000.0, 1500.0, step=100.0, key="d_weight")
        installments= col_b.slider("Max Installments",   1, 12, 1,    key="d_inst")
        revenue     = col_a.number_input("Order Revenue (R$)", 10.0, 5000.0, 89.9,  step=10.0,  key="d_rev")
        freight     = col_b.number_input("Freight (R$)",       5.0,  500.0,  18.5,  step=1.0,   key="d_frt")
        col_c, col_d = st.columns(2)
        vol_cm3     = col_c.number_input("Avg Product Volume (cm³)", 0.0, 100000.0, 500.0, step=50.0, key="d_vol")
        n_cats      = col_d.slider("Product Categories", 1, 5, 1, key="d_cats")

        predict_btn = st.button("🔮 Predict Delivery", type="primary", key="btn_delivery",
                                use_container_width=True)

    with right:
        if predict_btn:
            payload = {
                "customer_state": c_state,
                "seller_state":   s_state,
                "payment_type":   pay_type,
                "item_count":     float(item_count),
                "total_weight_g": float(weight),
                "order_revenue":  float(revenue),
                "order_freight":  float(freight),
                "max_installments": float(installments),
                "order_month_num":  float(month_num),
                "avg_product_volume_cm3": float(vol_cm3),
                "n_product_categories":   float(n_cats),
            }
            with st.spinner("Calling model..."):
                result = _call("/predict/delivery-delay", payload)

            if result:
                delay = result["delay_days"]
                interp = result["interpretation"]

                gauge_steps = [
                    {"range": [-30, 0],  "color": "#14532d"},  # early = green
                    {"range": [0, 7],    "color": "#713f12"},  # slightly late = orange
                    {"range": [7, 30],   "color": "#7f1d1d"},  # very late = red
                ]
                st.plotly_chart(
                    _gauge(delay, -30, 30, "Delivery Delay (days)", " days",
                           thresholds=gauge_steps),
                    use_container_width=True,
                )

                if delay < 0:
                    st.success(f"✅ **{interp}** — Order likely to arrive before estimated date.")
                elif delay <= 7:
                    st.warning(f"⚠️ **{interp}** — Minor delay expected.")
                else:
                    st.error(f"🚨 **{interp}** — Significant delay. Consider alerting the customer.")

                st.markdown(
                    f'<div class="insight-box">'
                    f'<b>Key driver:</b> {"Same-state shipping" if c_state == s_state else "Cross-state shipping"} '
                    f'• {item_count} item(s) • {weight:,.0f}g • Month {month_num}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("Fill in order details on the left and click **Predict Delivery**.")
            st.markdown("""
**How it works:**
- Model trained on 96K delivered orders from Silver layer
- Key features: shipping distance (same state?), product weight, order month seasonality
- `seller_customer_same_state` was the 2nd most important feature (18.5% importance)
""")


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — Churn Risk
# ════════════════════════════════════════════════════════════════════════════
with tab_churn:
    st.subheader("Customer Churn Risk Scorer")
    st.caption("Estimate the probability that a customer will not return to buy again.")

    left, right = st.columns([1, 1], gap="large")

    with left:
        st.markdown("**Customer Profile**")
        ch_state = st.selectbox("Customer State", BR_STATES, index=BR_STATES.index("RJ"), key="ch_state")

        st.markdown("**Purchase History**")
        col_a, col_b = st.columns(2)
        total_orders  = col_a.slider("Total Orders (all time)", 1, 30, 3,     key="ch_orders")
        total_spend   = col_b.number_input("Total Spend (R$)", 10.0, 20000.0, 450.0, step=50.0, key="ch_spend")
        avg_val       = col_a.number_input("Avg Order Value (R$)", 10.0, 5000.0, 150.0, step=10.0, key="ch_avg")
        is_repeat     = col_b.checkbox("Repeat Customer (>1 order)", value=True, key="ch_repeat")

        st.markdown("**Recent Activity (rolling windows)**")
        col_c, col_d = st.columns(2)
        orders_30d    = col_c.slider("Orders last 30d",  0, 5,   0, key="ch_30d")
        orders_90d    = col_d.slider("Orders last 90d",  0, 10,  1, key="ch_90d")
        orders_180d   = col_c.slider("Orders last 180d", 0, 20,  2, key="ch_180d")
        avg_gap       = col_d.slider("Avg days between orders", 0, 365, 45, key="ch_gap")
        days_since    = st.slider("Days since last order", 0, 365, 60, key="ch_since")

        predict_btn2 = st.button("🔮 Predict Churn", type="primary", key="btn_churn",
                                 use_container_width=True)

    with right:
        if predict_btn2:
            payload = {
                "customer_state":          ch_state,
                "total_orders":            float(total_orders),
                "total_spend":             float(total_spend),
                "avg_order_value":         float(avg_val),
                "is_repeat_customer":      float(is_repeat),
                "orders_last_30d":         float(orders_30d),
                "orders_last_90d":         float(orders_90d),
                "orders_last_180d":        float(orders_180d),
                "avg_days_between_orders": float(avg_gap),
                "days_since_last_order":   float(days_since),
            }
            with st.spinner("Calling model..."):
                result = _call("/predict/churn", payload)

            if result:
                prob  = result["churn_probability"]
                level = result["risk_level"]

                gauge_steps = [
                    {"range": [0, 0.4],  "color": "#14532d"},
                    {"range": [0.4, 0.7],"color": "#713f12"},
                    {"range": [0.7, 1.0],"color": "#7f1d1d"},
                ]
                st.plotly_chart(
                    _gauge(prob * 100, 0, 100, "Churn Probability", "%",
                           thresholds=gauge_steps),
                    use_container_width=True,
                )

                st.markdown(_risk_badge(level), unsafe_allow_html=True)
                st.markdown("")

                actions = {
                    "high":   "🚨 **Immediate action:** Send personalized re-engagement offer or assign to retention team.",
                    "medium": "⚠️ **Monitor closely:** Include in next email campaign with special discount.",
                    "low":    "✅ **Healthy customer:** No action needed. Continue standard engagement.",
                }
                st.markdown(
                    f'<div class="insight-box">{actions[level]}<br><br>'
                    f'<b>Profile:</b> {total_orders} orders • R${total_spend:,.0f} lifetime spend '
                    f'• {days_since} days since last order'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("Fill in customer data and click **Predict Churn**.")
            st.markdown("""
**How it works:**
- Temporal split: features computed BEFORE the 90-day churn window
- Rolling window features (`orders_last_30d/90d/180d`) capture recency trends
- SMOTE oversampling used to handle class imbalance (90% churn rate in dataset)
""")


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — Lead Scoring
# ════════════════════════════════════════════════════════════════════════════
with tab_lead:
    st.subheader("Lead Conversion Scorer")
    st.caption("Predict the probability that a marketing lead will become an Olist seller.")

    left, right = st.columns([1, 1], gap="large")

    with left:
        st.markdown("**Lead Profile**")
        origin   = st.selectbox("Acquisition Channel (Origin)", ORIGINS, key="ld_origin")
        col_a, col_b = st.columns(2)
        month    = col_a.slider("First Contact Month", 1, 12, 6, key="ld_month",
                                format="%d",
                                help="1=Jan, 12=Dec")
        dow      = col_b.selectbox("First Contact Day", DAYS, key="ld_dow")
        dow_num  = DAYS.index(dow)

        st.markdown("---")
        st.markdown("""
**Why only 3 features?**

All post-conversion fields (`business_segment`, `lead_type`, `declared_monthly_revenue`)
are only available AFTER a lead converts — using them would be data leakage.

These 3 features are the only ones available at the moment a lead first contacts Olist.
""")

        predict_btn3 = st.button("🔮 Score Lead", type="primary", key="btn_lead",
                                 use_container_width=True)

    with right:
        if predict_btn3:
            payload = {
                "origin":                  origin,
                "first_contact_month":     float(month),
                "first_contact_dayofweek": float(dow_num),
            }
            with st.spinner("Calling model..."):
                result = _call("/predict/lead", payload)

            if result:
                prob = result["conversion_probability"]
                rec  = result["recommendation"]

                gauge_steps = [
                    {"range": [0, 30],  "color": "#7f1d1d"},
                    {"range": [30, 60], "color": "#713f12"},
                    {"range": [60, 100],"color": "#14532d"},
                ]
                st.plotly_chart(
                    _gauge(prob * 100, 0, 100, "Conversion Probability", "%",
                           thresholds=gauge_steps),
                    use_container_width=True,
                )

                crm_icon = "🔥" if prob >= 0.6 else ("📋" if prob >= 0.3 else "📬")
                st.markdown(
                    f'<div class="insight-box">'
                    f'{crm_icon} <b>CRM Action:</b> {rec}<br><br>'
                    f'<b>Lead source:</b> {origin} • '
                    f'<b>Contact:</b> {dow}, month {month}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                st.markdown("**Conversion rate by origin (training data)**")
                origin_rates = {
                    "organic_search": 0.127, "paid_search": 0.083,
                    "social": 0.097, "direct_traffic": 0.118,
                    "email": 0.091, "other_publicities": 0.074,
                    "unknown": 0.080,
                }
                baseline = origin_rates.get(origin, 0.10)
                delta    = prob - baseline
                st.metric(
                    f"Prediction vs {origin} baseline",
                    f"{prob:.1%}",
                    f"{delta:+.1%}",
                    delta_color="normal",
                )
        else:
            st.info("Select lead attributes and click **Score Lead**.")
            st.markdown("""
**How it works:**
- 10.5% overall conversion rate (heavily imbalanced)
- SMOTE oversampling to detect the minority (converted) class
- `first_contact_month` is most important (39%) — seasonality matters
- `origin` is 2nd (35%) — organic search converts better than paid
""")


# ════════════════════════════════════════════════════════════════════════════
# TAB 4 — Batch Insights
# ════════════════════════════════════════════════════════════════════════════
with tab_batch:
    st.subheader("Batch Lead Scoring — Marketing Funnel")
    st.caption("Score all unscored leads from the Gold fct_funnel table via the ML API.")

    if st.button("🚀 Run Batch Score", type="primary", key="btn_batch"):
        try:
            from pyiceberg.catalog import load_catalog

            with st.spinner("Loading fct_funnel from Gold layer..."):
                catalog = load_catalog("rest", **{
                    "uri":               ICEBERG_URI,
                    "s3.endpoint":       S3_ENDPOINT,
                    "s3.access-key-id":  S3_ACCESS_KEY,
                    "s3.secret-access-key": S3_SECRET_KEY,
                    "s3.region":         S3_REGION or "auto",
                    "s3.path-style-access": "false",
                })
                df = catalog.load_table("gold.fct_funnel").scan(
                    selected_fields=("mql_id", "origin", "first_contact_date",
                                     "is_converted", "business_segment")
                ).to_pandas()

            first_contact = pd.to_datetime(df["first_contact_date"], errors="coerce")
            df["first_contact_month"]     = first_contact.dt.month.fillna(1).astype(int)
            df["first_contact_dayofweek"] = first_contact.dt.dayofweek.fillna(0).astype(int)
            df["origin"]                  = df["origin"].fillna("unknown").astype(str)

            with st.spinner(f"Scoring {len(df):,} leads via API..."):
                records = df[["origin", "first_contact_month", "first_contact_dayofweek"]].to_dict("records")
                resp = _call("/predict/lead/batch", {"records": records})

            if resp:
                preds = resp["predictions"]
                df["conversion_prob"]    = [p["conversion_probability"] for p in preds]
                df["model_is_converted"] = [p["is_converted"] for p in preds]
                df["recommendation"]     = [p["recommendation"] for p in preds]

                st.success(f"✅ Scored {len(df):,} leads")

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Leads",         f"{len(df):,}")
                col2.metric("Actual Conversions",  f"{df['is_converted'].sum():,}")
                col3.metric("Model Flags as High", f"{df['model_is_converted'].sum():,}")

                st.divider()

                import plotly.express as px

                col_hist, col_origin = st.columns(2)
                with col_hist:
                    st.markdown("**Conversion Probability Distribution**")
                    fig = px.histogram(
                        df, x="conversion_prob", nbins=30,
                        color_discrete_sequence=["#818cf8"],
                        labels={"conversion_prob": "Conversion Probability"},
                    )
                    fig.update_layout(
                        height=300, margin=dict(l=0, r=0, t=10, b=0),
                        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                        font_color="#fafafa",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col_origin:
                    st.markdown("**Avg Conversion Prob by Origin**")
                    origin_df = (
                        df.groupby("origin")["conversion_prob"]
                        .mean()
                        .reset_index()
                        .sort_values("conversion_prob", ascending=False)
                    )
                    fig2 = px.bar(
                        origin_df, x="conversion_prob", y="origin",
                        orientation="h",
                        color="conversion_prob",
                        color_continuous_scale="Purples",
                        labels={"conversion_prob": "Avg Conv. Prob", "origin": ""},
                    )
                    fig2.update_layout(
                        height=300, margin=dict(l=0, r=0, t=10, b=0),
                        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                        font_color="#fafafa", coloraxis_showscale=False,
                        yaxis=dict(autorange="reversed"),
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                st.markdown("**🔥 Top 20 High-Priority Leads**")
                top = (
                    df[df["is_converted"] == 0]          # not yet converted
                    .sort_values("conversion_prob", ascending=False)
                    .head(20)[["mql_id", "origin", "first_contact_month",
                               "conversion_prob", "recommendation"]]
                    .reset_index(drop=True)
                )
                top.index += 1
                top["conversion_prob"] = top["conversion_prob"].apply(lambda v: f"{v:.1%}")
                st.dataframe(top, use_container_width=True, height=380)

        except Exception as e:
            st.error(f"Error: {e}")
    else:
        st.markdown("""
This tab loads all **8,000 leads** from the Gold `fct_funnel` table,
scores them through the ML serving API in a single batch call,
and shows:

- Distribution of conversion probabilities
- Which acquisition channels have the highest predicted conversion rate
- Top 20 non-converted leads to prioritize for outreach

Click **Run Batch Score** to start.
""")
