"""
Sentiment Analysis Dashboard — ABSA results from gold.review_sentiment
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from database import get_db_connection

st.set_page_config(page_title="Sentiment Analysis", page_icon="💬", layout="wide")

st.title("💬 Review Sentiment Analysis")
st.caption("ABSA — BERTimbau-large fine-tuned trên 40k Olist reviews (Portuguese)")

# ── Load data ──────────────────────────────────────────────────────────────────

try:
    con, loaded_tables, _ = get_db_connection()
    loaded_names = [t for t, _ in loaded_tables]
except Exception as e:
    st.error(f"Không kết nối được Iceberg: {e}")
    st.stop()

if "review_sentiment" not in loaded_names:
    st.warning(
        "Chưa có bảng `review_sentiment`.\n\n"
        "Chạy inference trước:\n"
        "```\npython ml/sentiment/infer_sentiment.py\n```"
    )
    st.stop()

df = con.execute("SELECT * FROM review_sentiment").df()

ASPECTS   = ["product_quality", "delivery_speed", "seller_service", "price_value"]
SENTIMENTS = ["positive", "neutral", "negative"]
COLORS    = {"positive": "#22c55e", "neutral": "#94a3b8", "negative": "#ef4444"}

# ── KPI row ────────────────────────────────────────────────────────────────────

total      = len(df)
pos_pct    = (df["overall_sentiment"] == "positive").mean() * 100
neg_pct    = (df["overall_sentiment"] == "negative").mean() * 100
neu_pct    = (df["overall_sentiment"] == "neutral").mean()  * 100
avg_conf   = df["overall_confidence"].mean()

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Reviews Scored", f"{total:,}")
c2.metric("Positive 😊", f"{pos_pct:.1f}%")
c3.metric("Neutral 😐",  f"{neu_pct:.1f}%")
c4.metric("Negative 😞", f"{neg_pct:.1f}%")
c5.metric("Avg Confidence", f"{avg_conf:.2f}")

st.divider()

# ── Row 1: Overall distribution + Aspect heatmap ──────────────────────────────

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Overall Sentiment")
    counts = df["overall_sentiment"].value_counts().reindex(SENTIMENTS, fill_value=0)
    fig_pie = px.pie(
        values=counts.values,
        names=counts.index,
        color=counts.index,
        color_discrete_map=COLORS,
        hole=0.45,
    )
    fig_pie.update_traces(textposition="inside", textinfo="percent+label")
    fig_pie.update_layout(showlegend=False, margin=dict(t=10, b=10, l=10, r=10), height=300)
    st.plotly_chart(fig_pie, use_container_width=True)

with col2:
    st.subheader("Sentiment by Aspect")
    aspect_data = []
    for asp in ASPECTS:
        col = f"{asp}_sentiment"
        if col in df.columns:
            vc = df[col].value_counts(normalize=True) * 100
            for sent in SENTIMENTS:
                aspect_data.append({"Aspect": asp.replace("_", " ").title(),
                                    "Sentiment": sent,
                                    "Percentage": vc.get(sent, 0)})
    asp_df = pd.DataFrame(aspect_data)
    fig_bar = px.bar(
        asp_df, x="Aspect", y="Percentage", color="Sentiment",
        color_discrete_map=COLORS,
        barmode="stack", text_auto=".1f",
    )
    fig_bar.update_layout(margin=dict(t=10, b=10), height=300, legend_title="")
    st.plotly_chart(fig_bar, use_container_width=True)

# ── Row 2: Sentiment trend over time ──────────────────────────────────────────

st.subheader("Sentiment Trend")

if "scored_at" in df.columns:
    # Join với fct_orders để lấy order_date
    try:
        trend_df = con.execute("""
            SELECT
                DATE_TRUNC('month', o.purchased_at) AS month,
                r.overall_sentiment,
                COUNT(*) AS cnt
            FROM review_sentiment r
            JOIN fct_orders o ON r.order_id = o.order_id
            WHERE o.purchased_at IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1
        """).df()
        trend_df["month"] = pd.to_datetime(trend_df["month"])
        pivot = trend_df.pivot(index="month", columns="overall_sentiment", values="cnt").fillna(0)
        for s in SENTIMENTS:
            if s not in pivot.columns:
                pivot[s] = 0
        pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

        fig_trend = go.Figure()
        for sent in SENTIMENTS:
            fig_trend.add_trace(go.Scatter(
                x=pivot_pct.index, y=pivot_pct[sent],
                name=sent.capitalize(), mode="lines+markers",
                line=dict(color=COLORS[sent], width=2),
            ))
        fig_trend.update_layout(
            yaxis_title="% of reviews", xaxis_title="",
            height=300, margin=dict(t=10, b=10), legend_title="",
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    except Exception as e:
        st.info(f"Không load được trend: {e}")

# ── Row 3: Top negative sellers + aspect confidence ───────────────────────────

col3, col4 = st.columns(2)

with col3:
    st.subheader("Negative Sentiment by State")
    try:
        neg_state = con.execute("""
            SELECT
                o.customer_state AS state,
                COUNT(*) AS total_reviews,
                ROUND(AVG(CASE WHEN r.overall_sentiment='negative' THEN 1.0 ELSE 0.0 END)*100, 1) AS neg_pct
            FROM review_sentiment r
            JOIN fct_orders o ON r.order_id = o.order_id
            WHERE o.customer_state IS NOT NULL
            GROUP BY o.customer_state
            HAVING COUNT(*) >= 50
            ORDER BY neg_pct DESC
            LIMIT 10
        """).df()
        fig_neg = px.bar(
            neg_state, x="neg_pct", y="state", orientation="h",
            color="neg_pct", color_continuous_scale=["#fef2f2", "#ef4444"],
            labels={"neg_pct": "% Negative", "state": "State"},
            text="neg_pct",
        )
        fig_neg.update_layout(height=350, margin=dict(t=10, b=10), coloraxis_showscale=False)
        st.plotly_chart(fig_neg, use_container_width=True)
    except Exception as e:
        st.info(f"Lỗi join fct_orders: {e}")

with col4:
    st.subheader("Aspect Confidence Distribution")
    conf_data = []
    for asp in ASPECTS:
        col_c = f"{asp}_confidence"
        if col_c in df.columns:
            conf_data.append({"Aspect": asp.replace("_", " ").title(),
                               "Confidence": df[col_c].mean()})
    conf_df = pd.DataFrame(conf_data)
    fig_conf = px.bar(
        conf_df, x="Confidence", y="Aspect", orientation="h",
        color="Confidence", color_continuous_scale=["#fef9c3", "#22c55e"],
        range_x=[0, 1], text=conf_df["Confidence"].round(3),
    )
    fig_conf.update_layout(height=350, margin=dict(t=10, b=10), coloraxis_showscale=False)
    st.plotly_chart(fig_conf, use_container_width=True)

# ── Row 4: Sample reviews ──────────────────────────────────────────────────────

st.divider()
st.subheader("Sample Reviews")

tab_pos, tab_neu, tab_neg = st.tabs(["😊 Positive", "😐 Neutral", "😞 Negative"])

for tab, sentiment in [(tab_pos, "positive"), (tab_neu, "neutral"), (tab_neg, "negative")]:
    with tab:
        sample = (
            df[df["overall_sentiment"] == sentiment]
            .dropna(subset=["review_text"])
            [["review_text", "overall_confidence", "product_quality_sentiment",
              "delivery_speed_sentiment", "seller_service_sentiment"]]
            .head(5)
        )
        sample.columns = ["Review Text", "Confidence", "Product", "Delivery", "Service"]
        st.dataframe(sample, use_container_width=True, hide_index=True)
