"""
Olist Agentic BI — Natural Language → SQL → Chart
Dùng Claude API để sinh SQL từ câu hỏi tự nhiên, thực thi trên DuckDB views của Gold tables.
"""
import os
import json
import re
import logging
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from pyiceberg.catalog import load_catalog

# ── Config ────────────────────────────────────────────────────────────────────

ICEBERG_URI   = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
S3_ENDPOINT   = os.environ.get("S3_ENDPOINT", "")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
S3_REGION     = os.environ.get("S3_REGION", "auto")

# Provider: "openrouter" | "groq" | "anthropic"
AI_PROVIDER   = os.environ.get("AI_PROVIDER", "openrouter")

# OpenRouter (free models) — https://openrouter.ai/keys
OPENROUTER_KEY   = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")

# Groq (free tier) — https://console.groq.com/keys
GROQ_KEY      = os.environ.get("GROQ_API_KEY", "")
GROQ_MODEL    = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")

# Anthropic — https://console.anthropic.com/
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL  = os.environ.get("CLAUDE_MODEL", "claude-3-5-haiku-20241022")

GOLD_TABLES = ["fct_orders", "fct_funnel", "dim_sellers", "dim_customers"]
MAX_ROWS    = 1000
MAX_HISTORY = 10

logging.basicConfig(level=logging.WARNING)

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Olist Agentic BI",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  .stChatMessage { border-radius: 10px; }
  div[data-testid="metric-container"] { background:#1e1e2e; border-radius:10px; padding:12px; }
</style>
""", unsafe_allow_html=True)

# ── Load Gold tables vào DuckDB ───────────────────────────────────────────────

@st.cache_resource(show_spinner="Loading Gold tables from Iceberg...")
def get_db_connection():
    catalog = load_catalog(
        "rest",
        uri=ICEBERG_URI,
        **{
            "s3.endpoint":           S3_ENDPOINT,
            "s3.access-key-id":      S3_ACCESS_KEY,
            "s3.secret-access-key":  S3_SECRET_KEY,
            "s3.region":             S3_REGION,
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


# ── Schema Context Builder ─────────────────────────────────────────────────────

_COL_DESC = {
    # fct_orders
    "order_id":               "Unique order identifier",
    "customer_id":            "Per-order customer ID (same person may have many)",
    "customer_unique_id":     "Unique person ID across all orders",
    "order_status":           "delivered | shipped | canceled | processing | ...",
    "order_date":             "Date of purchase (DATE type)",
    "order_month":            "Purchase month string 'yyyy-MM'",
    "purchased_at":           "Full purchase timestamp",
    "actual_delivery_days":   "Days from purchase to delivery",
    "estimated_delivery_days":"Days from purchase to promised delivery",
    "delivery_delay_days":    "actual minus estimated (positive = late)",
    "delivery_status":        "early | on_time | late | unknown",
    "customer_state":         "Brazilian state code of customer (e.g. SP, RJ)",
    "customer_city":          "City of customer",
    "order_revenue":          "Item prices total in BRL",
    "order_freight":          "Freight cost in BRL",
    "item_count":             "Number of items in the order",
    "payment_value":          "Total payment amount in BRL",
    "payment_type":           "credit_card | boleto | debit_card | voucher",
    "review_score":           "Customer review 1–5",
    # fct_funnel
    "mql_id":                 "Marketing Qualified Lead ID",
    "first_contact_date":     "Date of first contact with lead",
    "origin":                 "Lead acquisition channel",
    "business_segment":       "Business category of the lead",
    "lead_type":              "online_medium | direct_traffic | ...",
    "days_to_close":          "Days from first contact to deal won",
    "is_converted":           "1 = became a seller, 0 = not converted",
    # dim_sellers
    "seller_id":              "Unique seller identifier",
    "seller_city":            "City of seller",
    "seller_state":           "Brazilian state code of seller",
    "total_orders":           "Total number of orders fulfilled",
    "total_revenue":          "Total revenue generated (BRL)",
    "avg_review_score":       "Average customer review score",
    "delivered_orders":       "Count of successfully delivered orders",
    # dim_customers
    "total_spend":            "Total BRL spent by this customer across all orders",
    "avg_order_value":        "Average order value in BRL",
    "days_since_last_order":  "Days elapsed since the last purchase",
    "is_churned":             "1 = no order in last 90 days",
    "is_repeat_customer":     "1 = placed more than 1 order",
}


def build_schema_context(con: duckdb.DuckDBPyConnection, loaded: list) -> str:
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
    return "\n\n".join(parts)


# ── Prompts ───────────────────────────────────────────────────────────────────

_SYSTEM = """\
Bạn là SQL analyst chuyên về dữ liệu e-commerce Olist Brazil.

NHIỆM VỤ: Nhận câu hỏi, sinh SQL SELECT để trả lời, trả về JSON.

QUY TẮC:
- Chỉ dùng SELECT. Không INSERT/UPDATE/DELETE/DROP/CREATE.
- Tự động thêm LIMIT {max_rows} nếu query không có LIMIT.
- Tên bảng sẵn có: {table_names}
- Số tiền tính bằng BRL (R$).
- Dùng date_trunc('month', col) hoặc strftime(col, '%Y-%m') để group theo tháng.
- Trả lời explanation bằng tiếng Việt.

OUTPUT (chỉ JSON thuần, không markdown code block):
{{"sql": "SELECT ...", "explanation": "Giải thích ngắn bằng tiếng Việt", "chart_type": "bar|line|pie|scatter|none"}}

CHART TYPE:
- bar: so sánh danh mục (top N, phân bố theo state/seller)
- line: xu hướng thời gian (doanh thu theo tháng)
- pie: tỷ lệ phần trăm (% payment type, % status)
- scatter: tương quan 2 biến số
- none: bảng nhiều cột hoặc câu hỏi text thuần

SCHEMA:
{schema}

FEW-SHOT EXAMPLES:

Q: Doanh thu theo tháng năm 2018?
A: {{"sql": "SELECT strftime(purchased_at, '%Y-%m') AS month, SUM(payment_value) AS revenue FROM fct_orders WHERE purchased_at >= '2018-01-01' AND purchased_at < '2019-01-01' GROUP BY 1 ORDER BY 1", "explanation": "Tổng doanh thu (payment_value) theo từng tháng trong năm 2018.", "chart_type": "line"}}

Q: Top 10 bang có nhiều đơn hàng nhất?
A: {{"sql": "SELECT customer_state, COUNT(*) AS orders FROM fct_orders GROUP BY customer_state ORDER BY orders DESC LIMIT 10", "explanation": "10 bang Brazil có số lượng đơn hàng cao nhất.", "chart_type": "bar"}}

Q: Tỷ lệ các loại thanh toán?
A: {{"sql": "SELECT payment_type, COUNT(*) AS cnt FROM fct_orders WHERE payment_type IS NOT NULL GROUP BY payment_type ORDER BY cnt DESC", "explanation": "Số lượng đơn hàng theo từng phương thức thanh toán.", "chart_type": "pie"}}

Q: Seller nào có review tốt nhất với hơn 100 đơn?
A: {{"sql": "SELECT seller_id, seller_state, avg_review_score, delivered_orders, total_revenue FROM dim_sellers WHERE delivered_orders > 100 ORDER BY avg_review_score DESC LIMIT 10", "explanation": "Top 10 seller có điểm đánh giá trung bình cao nhất trong số các seller có hơn 100 đơn hàng.", "chart_type": "bar"}}

Q: Phân tích churn: bao nhiêu % khách hàng là churned?
A: {{"sql": "SELECT is_churned, COUNT(*) AS customers, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct FROM dim_customers GROUP BY is_churned", "explanation": "Tỷ lệ khách hàng churned (không mua hàng trong 90 ngày) so với tổng số khách hàng.", "chart_type": "pie"}}
"""

_FIX_MSG = """\
SQL sau gặp lỗi khi thực thi trên DuckDB:

SQL:
{sql}

Lỗi:
{error}

Hãy sửa lại SQL. Chỉ trả về JSON (không markdown):
{{"sql": "...", "explanation": "...", "chart_type": "bar|line|pie|scatter|none"}}"""


# ── Claude API ────────────────────────────────────────────────────────────────

def _openai_compat(url: str, key: str, model: str, messages: list, system: str) -> str:
    import requests
    all_msgs = ([{"role": "system", "content": system}] if system else []) + messages
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={"model": model, "messages": all_msgs, "max_tokens": 1024, "temperature": 0},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _call_llm(messages: list, system: str) -> str:
    if AI_PROVIDER == "openrouter":
        return _openai_compat(
            "https://openrouter.ai/api/v1/chat/completions",
            OPENROUTER_KEY, OPENROUTER_MODEL, messages, system,
        )
    if AI_PROVIDER == "groq":
        return _openai_compat(
            "https://api.groq.com/openai/v1/chat/completions",
            GROQ_KEY, GROQ_MODEL, messages, system,
        )
    # anthropic
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    resp = client.messages.create(
        model=CLAUDE_MODEL, max_tokens=1024, system=system, messages=messages,
    )
    return resp.content[0].text


def _parse_json(text: str) -> dict:
    text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`")
    m = re.search(r"\{.*\}", text, re.DOTALL)
    return json.loads(m.group() if m else text)


def query(
    question: str,
    history: list,
    schema: str,
    con: duckdb.DuckDBPyConnection,
) -> dict:
    """Sinh SQL → thực thi → self-correct tối đa 3 lần. Trả về result dict."""
    system = _SYSTEM.format(
        max_rows=MAX_ROWS,
        table_names=", ".join(GOLD_TABLES),
        schema=schema,
    )
    base_msgs = list(history[-MAX_HISTORY:]) + [{"role": "user", "content": question}]

    raw_sql      = ""
    prev_raw     = ""
    last_error   = ""

    for attempt in range(3):
        try:
            if attempt == 0:
                msgs = base_msgs
            else:
                msgs = base_msgs + [
                    {"role": "assistant", "content": prev_raw},
                    {"role": "user", "content": _FIX_MSG.format(sql=raw_sql, error=last_error)},
                ]
            raw = _call_llm(msgs, system)
            prev_raw = raw
            parsed = _parse_json(raw)
            raw_sql = parsed.get("sql", "").strip()

            if not raw_sql:
                return {"error": "Claude không sinh được SQL.", "sql": "", "explanation": raw, "chart_type": "none", "result_df": None}

            if "limit" not in raw_sql.lower():
                raw_sql += f" LIMIT {MAX_ROWS}"
            parsed["sql"] = raw_sql

            df = con.execute(raw_sql).df()
            return {**parsed, "result_df": df, "error": None}

        except json.JSONDecodeError as e:
            return {"error": f"Không parse được JSON từ Claude: {e}", "sql": raw_sql, "explanation": "", "chart_type": "none", "result_df": None}
        except Exception as e:
            last_error = str(e)
            if attempt == 2:
                return {"error": f"SQL thất bại sau 3 lần thử: {last_error}", "sql": raw_sql, "explanation": "", "chart_type": "none", "result_df": None}

    return {"error": "Không thực thi được.", "sql": "", "explanation": "", "chart_type": "none", "result_df": None}


# ── Chart ─────────────────────────────────────────────────────────────────────

def render_chart(df: pd.DataFrame, chart_type: str) -> None:
    if df is None or df.empty or chart_type == "none":
        return
    cols = list(df.columns)
    layout = dict(
        height=380,
        margin=dict(l=0, r=0, t=30, b=0),
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        font_color="#fafafa",
    )
    try:
        if chart_type == "bar" and len(cols) >= 2:
            fig = px.bar(df, x=cols[0], y=cols[1],
                         color_discrete_sequence=px.colors.qualitative.Pastel)
        elif chart_type == "line" and len(cols) >= 2:
            fig = px.line(df, x=cols[0], y=cols[1], markers=True)
        elif chart_type == "pie" and len(cols) >= 2:
            fig = px.pie(df, names=cols[0], values=cols[1], hole=0.4,
                         color_discrete_sequence=px.colors.qualitative.Pastel)
        elif chart_type == "scatter" and len(cols) >= 2:
            fig = px.scatter(df, x=cols[0], y=cols[1],
                             color=cols[2] if len(cols) > 2 else None)
        else:
            return
        fig.update_layout(**layout)
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.caption(f"Không render được chart: {e}")


# ── UI ─────────────────────────────────────────────────────────────────────────

# Check API key theo provider
_key_map = {"openrouter": OPENROUTER_KEY, "groq": GROQ_KEY, "anthropic": ANTHROPIC_KEY}
if not _key_map.get(AI_PROVIDER, ""):
    _hints = {
        "openrouter": "AI_PROVIDER=openrouter\nOPENROUTER_API_KEY=sk-or-...",
        "groq":       "AI_PROVIDER=groq\nGROQ_API_KEY=gsk_...",
        "anthropic":  "AI_PROVIDER=anthropic\nANTHROPIC_API_KEY=sk-ant-...",
    }
    st.error(
        f"**API key cho `{AI_PROVIDER}` chưa được điền.**\n\n"
        f"Thêm vào `.env`:\n```\n{_hints.get(AI_PROVIDER, '')}\n```\n"
        "Sau đó restart:\n```\ndocker compose --profile bi up -d streamlit\n```"
    )
    st.stop()

# Load data
try:
    con, loaded_tables, load_errors = get_db_connection()
except Exception as e:
    st.error(f"Không kết nối được Iceberg catalog: {e}")
    st.stop()

if not loaded_tables:
    st.error(
        "Không có Gold table nào.\n\n"
        "Chạy Gold transform:\n"
        "```\ndocker exec olist-prefect-worker python /app/spark/jobs/gold_transform.py\n```"
    )
    st.stop()

schema_ctx = build_schema_context(con, loaded_tables)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🤖 Agentic BI")
    _model_label = {
        "openrouter": OPENROUTER_MODEL,
        "groq":       GROQ_MODEL,
        "anthropic":  CLAUDE_MODEL,
    }.get(AI_PROVIDER, AI_PROVIDER)
    st.caption(f"Model: `{_model_label}` via {AI_PROVIDER}")
    st.divider()

    st.subheader("Tables loaded")
    for tbl, rows in loaded_tables:
        st.caption(f"✅ `{tbl}` — {rows:,} rows")
    for tbl, err in load_errors:
        st.caption(f"❌ `{tbl}` — load failed")

    st.divider()
    st.subheader("Sample questions")
    SAMPLES = [
        "Doanh thu theo tháng năm 2018?",
        "Top 10 bang có nhiều đơn hàng nhất?",
        "Tỷ lệ giao hàng trễ theo bang?",
        "Seller nào review tốt nhất với hơn 100 đơn?",
        "Phân tích churn: bao nhiêu % khách hàng là churned?",
        "So sánh doanh thu trung bình theo phương thức thanh toán?",
    ]
    for q in SAMPLES:
        if st.button(q, use_container_width=True, key=f"s_{hash(q)}"):
            st.session_state["_pending"] = q

    st.divider()
    if st.button("🗑 Clear conversation", use_container_width=True):
        st.session_state["messages"]     = []
        st.session_state["chat_history"] = []
        st.rerun()

# ── Main chat area ─────────────────────────────────────────────────────────────

st.title("🤖 Olist Agentic BI")
st.caption("Đặt câu hỏi về dữ liệu Olist — Claude sinh SQL, DuckDB thực thi, kết quả hiển thị ngay.")

if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "chat_history" not in st.session_state:
    st.session_state["chat_history"] = []

# Display history
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        if msg["role"] == "user":
            st.write(msg["content"])
        else:
            if msg.get("explanation"):
                st.write(msg["explanation"])
            if msg.get("sql"):
                with st.expander("🔍 SQL", expanded=False):
                    st.code(msg["sql"], language="sql")
            df_stored = msg.get("result_df")
            if df_stored is not None and not df_stored.empty:
                st.dataframe(df_stored, use_container_width=True, height=220)
                render_chart(df_stored, msg.get("chart_type", "none"))
            if msg.get("error"):
                st.error(msg["error"])

# Chat input
user_input = st.chat_input("Nhập câu hỏi... (ví dụ: Doanh thu theo tháng 2018?)")

# Sidebar sample button → inject into input
if "_pending" in st.session_state:
    user_input = st.session_state.pop("_pending")

if user_input:
    st.session_state["messages"].append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.write(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Đang phân tích..."):
            res = query(
                question=user_input,
                history=st.session_state["chat_history"],
                schema=schema_ctx,
                con=con,
            )

        # Update Claude conversation history
        st.session_state["chat_history"].append({"role": "user", "content": user_input})
        st.session_state["chat_history"].append({
            "role": "assistant",
            "content": res.get("explanation") or res.get("error") or "",
        })

        # Display
        if res.get("explanation"):
            st.write(res["explanation"])
        if res.get("sql"):
            with st.expander("🔍 SQL", expanded=False):
                st.code(res["sql"], language="sql")
        df_res = res.get("result_df")
        if df_res is not None and not df_res.empty:
            st.dataframe(df_res, use_container_width=True, height=220)
            render_chart(df_res, res.get("chart_type", "none"))
        if res.get("error"):
            st.error(res["error"])

    # Save to display history
    st.session_state["messages"].append({
        "role":        "assistant",
        "explanation": res.get("explanation", ""),
        "sql":         res.get("sql", ""),
        "chart_type":  res.get("chart_type", "none"),
        "result_df":   df_res,
        "error":       res.get("error"),
    })
