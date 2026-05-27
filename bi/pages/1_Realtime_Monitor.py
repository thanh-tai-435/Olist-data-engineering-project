"""
Olist Real-Time Order Monitor — Streamlit multipage wrapper.
Nội dung giống realtime_dashboard.py, tích hợp vào multipage navigation.
"""
import os
import json
import time
import logging
import pandas as pd
import plotly.express as px
import streamlit as st
from collections import deque
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError

BROKERS          = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "3"))
MAX_EVENTS       = int(os.environ.get("MAX_EVENTS", "2000"))

logging.getLogger("confluent_kafka").setLevel(logging.WARNING)

st.set_page_config(
    page_title="Olist Live Monitor",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  div[data-testid="metric-container"] { background:#1e1e2e; border-radius:10px; padding:12px; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_consumer() -> Consumer:
    c = Consumer({
        "bootstrap.servers":  BROKERS,
        "group.id":           "streamlit-live-monitor",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
        "fetch.max.bytes":    1_048_576,
    })
    c.subscribe(["olist.orders"])
    return c


@st.cache_resource
def get_shared_state() -> dict:
    return {
        "orders":         deque(maxlen=MAX_EVENTS),
        "started":        datetime.now(timezone.utc),
        "total_received": 0,
    }


def poll_messages(consumer: Consumer, max_msgs: int = 300) -> list:
    msgs, deadline = [], time.monotonic() + 0.5
    while len(msgs) < max_msgs and time.monotonic() < deadline:
        msg = consumer.poll(0.05)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                st.warning(f"Kafka error: {msg.error()}")
            break
        try:
            payload = json.loads(msg.value().decode())
            payload["_received_at"] = datetime.now(timezone.utc).isoformat()
            msgs.append(payload)
        except Exception:
            pass
    return msgs


shared   = get_shared_state()
consumer = get_consumer()

for msg in poll_messages(consumer):
    shared["orders"].append(msg)
    shared["total_received"] += 1

orders_list = list(shared["orders"])

if not orders_list:
    st.title("🛒 Olist Real-Time Order Monitor")
    st.info("Đang chờ events từ Redpanda... Hãy chạy producer:\n\n"
            "```\ndocker exec olist-prefect-worker python /app/streaming/producer.py\n```")
    time.sleep(REFRESH_INTERVAL)
    st.rerun()

df = pd.DataFrame(orders_list)

if "order_purchase_timestamp" in df.columns:
    df["ts"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
else:
    df["ts"] = pd.to_datetime(df["_received_at"], errors="coerce")

df["payment_value"] = pd.to_numeric(
    df.get("payment_value", pd.Series(dtype=float)), errors="coerce"
).fillna(0.0)

elapsed = (datetime.now(timezone.utc) - shared["started"]).total_seconds()
rate    = round(shared["total_received"] / max(elapsed / 60, 0.01), 1)

col_title, col_metric = st.columns([5, 1])
with col_title:
    st.title("🛒 Olist Real-Time Order Monitor")
    st.caption(
        f"Last updated: {datetime.now().strftime('%H:%M:%S')}  •  "
        f"Buffered: {len(df):,}  •  Total received: {shared['total_received']:,}"
    )
with col_metric:
    st.metric("Events/min", f"{rate:,.1f}")

st.divider()

status_counts = df["order_status"].value_counts() if "order_status" in df.columns else pd.Series()
total_revenue = df["payment_value"].sum()
avg_val       = df.loc[df["payment_value"] > 0, "payment_value"].mean()

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Orders",      f"{len(df):,}")
k2.metric("Revenue (buffered)", f"R$ {total_revenue:,.0f}")
k3.metric("Avg Order Value",   f"R$ {avg_val:,.2f}" if avg_val == avg_val else "—")
k4.metric("Delivered",         f"{status_counts.get('delivered', 0):,}")
k5.metric("Canceled",          f"{status_counts.get('canceled', 0):,}")

st.divider()

col_pay, col_pie = st.columns([3, 1])

with col_pay:
    st.subheader("Revenue by Payment Type")
    if "payment_type" in df.columns and df["payment_value"].sum() > 0:
        pay_df = (
            df[df["payment_value"] > 0]
            .groupby("payment_type")
            .agg(revenue=("payment_value", "sum"), orders=("order_id", "count"))
            .reset_index()
            .sort_values("revenue", ascending=False)
        )
        fig = px.bar(pay_df, x="payment_type", y="revenue",
                     text=pay_df["orders"].apply(lambda v: f"{v:,} orders"),
                     color="payment_type",
                     color_discrete_sequence=px.colors.qualitative.Pastel,
                     labels={"payment_type": "", "revenue": "Revenue (R$)"})
        fig.update_traces(textposition="outside")
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0),
                          plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                          font_color="#fafafa", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Đang tải dữ liệu payment...")

with col_pie:
    st.subheader("Order Status")
    if not status_counts.empty:
        fig = px.pie(values=status_counts.values, names=status_counts.index,
                     hole=0.5, color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0),
                          plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                          font_color="#fafafa")
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu status")

col_states, col_rev = st.columns(2)

with col_states:
    st.subheader("Top 10 Customer States")
    if "customer_state" in df.columns:
        state_df = df["customer_state"].value_counts().head(10).reset_index()
        state_df.columns = ["state", "count"]
        fig = px.bar(state_df, x="count", y="state", orientation="h",
                     color="count", color_continuous_scale="Purples",
                     labels={"state": "", "count": "Orders"})
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0),
                          plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                          font_color="#fafafa", yaxis=dict(autorange="reversed"),
                          coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("customer_state không có trong stream. Producer cần join customers CSV.")

with col_rev:
    st.subheader("Cumulative Revenue")
    df_s = df[df["payment_value"] > 0].sort_values("ts")
    if not df_s.empty:
        df_s = df_s.copy()
        df_s["cumulative"] = df_s["payment_value"].cumsum()
        fig = px.area(df_s, x="ts", y="cumulative",
                      color_discrete_sequence=["#2ecc71"],
                      labels={"ts": "", "cumulative": "R$ Cumulative"})
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0),
                          plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                          font_color="#fafafa")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Chưa có orders có payment_value > 0.")

st.subheader("Latest Orders")
display_cols = [c for c in [
    "order_id", "customer_id", "order_status",
    "order_purchase_timestamp", "payment_value", "_received_at",
] if c in df.columns]
latest = df[display_cols].sort_values("_received_at", ascending=False).head(20).reset_index(drop=True)
latest.index += 1
st.dataframe(latest, use_container_width=True, height=400)

time.sleep(REFRESH_INTERVAL)
st.rerun()
