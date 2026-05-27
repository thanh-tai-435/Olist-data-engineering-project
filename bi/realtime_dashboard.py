"""
Olist Real-Time Order Monitor
Đọc trực tiếp từ Redpanda topic olist.orders và hiển thị metrics live.

Tự động refresh mỗi REFRESH_INTERVAL giây.
"""
import os
import json
import time
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from collections import deque
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError

BROKERS          = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "3"))
MAX_EVENTS       = int(os.environ.get("MAX_EVENTS", "2000"))

logging.getLogger("confluent_kafka").setLevel(logging.WARNING)

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Olist Live Monitor",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  .metric-card { background:#1e1e2e; border-radius:12px; padding:16px; }
  div[data-testid="metric-container"] { background:#1e1e2e; border-radius:10px; padding:12px; }
  .stDataFrame { font-size: 13px; }
</style>
""", unsafe_allow_html=True)

# ── Kafka consumer (khởi tạo 1 lần, lưu trong session_state) ─────────────────

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
    """
    Dùng cache_resource thay session_state để buffer tồn tại qua reload.
    Trả về dict dùng chung toàn bộ sessions.
    """
    return {
        "orders":         deque(maxlen=MAX_EVENTS),
        "started":        datetime.now(timezone.utc),
        "total_received": 0,
    }


def poll_messages(consumer: Consumer, max_msgs: int = 300) -> list[dict]:
    """Non-blocking poll: đọc tối đa max_msgs trong 0.5s."""
    msgs = []
    deadline = time.monotonic() + 0.5
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
            payload["_topic"]      = msg.topic()
            payload["_received_at"] = datetime.now(timezone.utc).isoformat()
            msgs.append(payload)
        except Exception:
            pass
    return msgs


# ── Session state init ────────────────────────────────────────────────────────

# shared_state tồn tại suốt vòng đời Streamlit server, không reset khi reload
shared = get_shared_state()

# ── Poll new messages ─────────────────────────────────────────────────────────

consumer = get_consumer()
new_msgs = poll_messages(consumer)

for msg in new_msgs:
    shared["orders"].append(msg)
    shared["total_received"] += 1

# ── Build DataFrame ───────────────────────────────────────────────────────────

orders_list = list(shared["orders"])

if not orders_list:
    st.title("🛒 Olist Real-Time Order Monitor")
    st.info("Đang chờ events từ Redpanda... Hãy chạy producer:\n\n"
            "```\ndocker exec olist-prefect-worker python /app/streaming/producer.py\n```")
    time.sleep(REFRESH_INTERVAL)
    st.rerun()

df = pd.DataFrame(orders_list)

# Timestamp
if "order_purchase_timestamp" in df.columns:
    df["ts"] = pd.to_datetime(df["order_purchase_timestamp"], errors="coerce")
else:
    df["ts"] = pd.to_datetime(df["_received_at"], errors="coerce")

df["minute"] = df["ts"].dt.floor("min")

# payment_value đã có trong message (producer join từ payments CSV)
if "payment_value" in df.columns:
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(0.0)
else:
    df["payment_value"] = 0.0

# ── Header ────────────────────────────────────────────────────────────────────

elapsed = (datetime.now(timezone.utc) - shared["started"]).total_seconds()
rate    = round(shared["total_received"] / max(elapsed / 60, 0.01), 1)

col_title, col_refresh = st.columns([5, 1])
with col_title:
    st.title("🛒 Olist Real-Time Order Monitor")
    st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}  •  "
               f"Buffered: {len(df):,} events  •  "
               f"Total received: {shared['total_received']:,}")
with col_refresh:
    st.metric("Events/min", f"{rate:,.1f}")

st.divider()

# ── KPI metrics ───────────────────────────────────────────────────────────────

status_counts = df["order_status"].value_counts() if "order_status" in df.columns else pd.Series()

total_revenue = df["payment_value"].sum()
avg_order_val = df.loc[df["payment_value"] > 0, "payment_value"].mean()

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Orders",      f"{len(df):,}")
k2.metric("Revenue (buffered)", f"R$ {total_revenue:,.0f}")
k3.metric("Avg Order Value",   f"R$ {avg_order_val:,.2f}" if avg_order_val == avg_order_val else "—")
k4.metric("Delivered",         f"{status_counts.get('delivered', 0):,}")
k5.metric("Canceled",          f"{status_counts.get('canceled', 0):,}")

st.divider()

# ── Charts – row 1 ────────────────────────────────────────────────────────────

col_line, col_pie = st.columns([3, 1])

with col_line:
    st.subheader("Revenue by Payment Type")
    if "payment_type" in df.columns and df["payment_value"].sum() > 0:
        pay_df = (
            df[df["payment_value"] > 0]
            .groupby("payment_type")
            .agg(revenue=("payment_value", "sum"), orders=("order_id", "count"))
            .reset_index()
            .sort_values("revenue", ascending=False)
        )
        pay_df["label"] = pay_df.apply(
            lambda r: f"{r['payment_type']}<br>{r['orders']:,} orders", axis=1
        )
        fig_pay = px.bar(
            pay_df, x="payment_type", y="revenue",
            text=pay_df["orders"].apply(lambda v: f"{v:,} orders"),
            color="payment_type",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            labels={"payment_type": "", "revenue": "Revenue (R$)"},
        )
        fig_pay.update_traces(textposition="outside")
        fig_pay.update_layout(
            height=300, margin=dict(l=0, r=0, t=30, b=0),
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            font_color="#fafafa", showlegend=False,
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        st.plotly_chart(fig_pay, use_container_width=True)
    else:
        st.info("Đang tải dữ liệu payment...")

with col_pie:
    st.subheader("Order Status")
    if not status_counts.empty:
        fig_pie = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            hole=0.5,
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig_pie.update_layout(
            height=300, margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            font_color="#fafafa", showlegend=True,
            legend=dict(orientation="v", x=1.0, y=0.5),
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu status")

# ── Charts – row 2 ────────────────────────────────────────────────────────────

col_bar, col_revenue = st.columns(2)

with col_bar:
    st.subheader("Top 10 Customer States")
    if "customer_state" in df.columns:
        state_counts = (
            df["customer_state"]
              .value_counts()
              .head(10)
              .reset_index()
        )
        state_counts.columns = ["state", "count"]
        fig_bar = px.bar(
            state_counts, x="count", y="state",
            orientation="h",
            color="count",
            color_continuous_scale="Purples",
            labels={"state": "", "count": "Orders"},
        )
        fig_bar.update_layout(
            height=320, margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            font_color="#fafafa",
            yaxis=dict(autorange="reversed"),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Không có customer_state trong stream orders.\n\n"
                "customer_state nằm trong bảng customers—không được gửi qua topic olist.orders.")

with col_revenue:
    st.subheader("Cumulative Revenue")
    df_sorted = df[df["payment_value"] > 0].sort_values("ts")
    if not df_sorted.empty:
        df_sorted["cumulative_revenue"] = df_sorted["payment_value"].cumsum()
        fig_rev = px.area(
            df_sorted, x="ts", y="cumulative_revenue",
            color_discrete_sequence=["#2ecc71"],
            labels={"ts": "", "cumulative_revenue": "R$ Cumulative"},
        )
        fig_rev.update_layout(
            height=320, margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            font_color="#fafafa",
            xaxis=dict(gridcolor="#333"), yaxis=dict(gridcolor="#333"),
        )
        st.plotly_chart(fig_rev, use_container_width=True)
    else:
        st.info("Chưa có orders có payment_value > 0.")

# ── Latest orders table ───────────────────────────────────────────────────────

st.subheader("Latest Orders")
display_cols = [c for c in [
    "order_id", "customer_id", "order_status",
    "order_purchase_timestamp", "payment_value", "_received_at",
] if c in df.columns]

latest = (
    df[display_cols]
      .sort_values("_received_at", ascending=False)
      .head(20)
      .reset_index(drop=True)
)
latest.index += 1
st.dataframe(latest, use_container_width=True, height=400)

# ── Auto-refresh ──────────────────────────────────────────────────────────────

time.sleep(REFRESH_INTERVAL)
st.rerun()
