"""
Olist Agentic BI — Streamlit UI
Natural Language → Intent → SQL (tool use / text) → Chart + Insight
"""
import streamlit as st
from agent import run
from charts import render_chart
from config import AI_PROVIDER, active_llm_key, active_model_label
from database import build_schema_context, get_db_connection, get_kpi_metrics
from validator import classify_intent, validate

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Olist Agentic BI",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  div[data-testid="metric-container"] {
    background:#1e1e2e; border-radius:10px; padding:12px;
  }
  .insight-box {
    background: #1e293b;
    border-left: 3px solid #3b82f6;
    border-radius: 6px;
    padding: 12px 16px;
    margin: 8px 0 12px 0;
    font-size: 0.95rem;
    line-height: 1.6;
    color: #e2e8f0;
  }
  .intent-tag {
    font-size: 0.7rem; font-weight: 700; letter-spacing: 0.05em;
    padding: 2px 8px; border-radius: 4px; margin-bottom: 8px;
    display: inline-block;
  }
  .tag-data   { background:#1e3a5f; color:#93c5fd; }
  .tag-follow { background:#14532d; color:#86efac; }
  .tag-small  { background:#3b0764; color:#d8b4fe; }
  .query-label {
    font-size: 0.75rem; color: #94a3b8;
    margin-bottom: 4px;
  }
</style>
""", unsafe_allow_html=True)

_INTENT_TAG = {
    "DATA_QUERY": '<span class="intent-tag tag-data">DATA QUERY</span>',
    "FOLLOWUP":   '<span class="intent-tag tag-follow">FOLLOW-UP</span>',
    "SMALLTALK":  '<span class="intent-tag tag-small">SMALLTALK</span>',
}


# ── Render helpers ────────────────────────────────────────────────────────────

def _render_multi_query(all_queries: list, question: str) -> None:
    """Render results from multiple tool_use calls as tabs."""
    tabs = st.tabs([f"Query {i + 1}" for i in range(len(all_queries))])
    for tab, q in zip(tabs, all_queries):
        with tab:
            if q.get("explanation"):
                st.caption(q["explanation"])
            if q.get("sql"):
                with st.expander("SQL", expanded=False):
                    st.code(q["sql"], language="sql")
            df = q.get("result_df")
            if df is not None and not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)
            if q.get("error"):
                st.error(q["error"])

    # Chart for last successful query
    last_ok = next((q for q in reversed(all_queries) if q.get("result_df") is not None), None)
    if last_ok and last_ok["result_df"] is not None and not last_ok["result_df"].empty:
        render_chart(
            last_ok["result_df"],
            last_ok.get("chart_type", "none"),
            question=question,
            llm_hint=last_ok.get("chart_type", "none"),
        )


def _render_single_query(q: dict, question: str) -> None:
    """Render a single query result (tool_use with 1 call, or text-based fallback)."""
    if q.get("sql"):
        with st.expander("SQL", expanded=False):
            st.code(q["sql"], language="sql")
    df = q.get("result_df")
    if df is not None and not df.empty:
        with st.expander(f"Dữ liệu — {len(df):,} dòng", expanded=False):
            st.dataframe(df, use_container_width=True, hide_index=True)
        render_chart(
            df,
            q.get("chart_type", "none"),
            question=question,
            llm_hint=q.get("chart_type", "none"),
        )
    if q.get("error"):
        st.error(q["error"])


def _render_assistant(msg: dict) -> None:
    intent = msg.get("intent", "DATA_QUERY")
    st.markdown(_INTENT_TAG.get(intent, ""), unsafe_allow_html=True)

    # Text response (SMALLTALK / FOLLOWUP)
    if msg.get("text"):
        st.markdown(msg["text"])

    # Business insight
    if msg.get("summary"):
        st.markdown(
            f'<div class="insight-box">{msg["summary"]}</div>',
            unsafe_allow_html=True,
        )

    all_queries = msg.get("all_queries") or []

    if len(all_queries) > 1:
        _render_multi_query(all_queries, msg.get("question", ""))
    elif len(all_queries) == 1:
        _render_single_query(all_queries[0], msg.get("question", ""))
    else:
        # Text-based fallback path: result stored directly in msg
        _render_single_query(
            {"sql": msg.get("sql"), "result_df": msg.get("result_df"),
             "chart_type": msg.get("chart_type", "none"), "error": msg.get("error")},
            msg.get("question", ""),
        )


# ── API key check ─────────────────────────────────────────────────────────────

if not active_llm_key():
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

# ── Load data ─────────────────────────────────────────────────────────────────

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

# ── Session state ─────────────────────────────────────────────────────────────

if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "chat_history" not in st.session_state:
    st.session_state["chat_history"] = []

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("Olist Agentic BI")
    st.caption(f"LLM: `{active_model_label()}`")
    if AI_PROVIDER == "anthropic":
        st.caption("Mode: **Tool Use** (multi-step agentic)")
    else:
        st.caption("Mode: Text-based SQL")
    st.divider()

    with st.expander("Stack", expanded=False):
        st.caption("**Storage** — Cloudflare R2 (S3-compatible)")
        st.caption("**Format** — Apache Iceberg (ACID, time-travel)")
        st.caption("**Streaming** — Redpanda → Bronze Iceberg")
        st.caption("**Transform** — PySpark local[2] → Silver / Gold")
        st.caption("**Query** — PyIceberg → DuckDB in-memory")
        st.caption(f"**LLM** — Claude API (`{active_model_label()}`)")

    st.divider()

    kpis = get_kpi_metrics(con)
    if kpis:
        col1, col2 = st.columns(2)
        col1.metric("Orders",    kpis.get("total_orders", "–"))
        col2.metric("Revenue",   kpis.get("total_revenue", "–"))
        col1.metric("Avg ⭐",    kpis.get("avg_review", "–"))
        col2.metric("Customers", kpis.get("unique_customers", "–"))
        st.divider()

    st.subheader("Tables loaded")
    for tbl, rows in loaded_tables:
        st.caption(f"✅ `{tbl}` — {rows:,} rows")
    for tbl, _err in load_errors:
        st.caption(f"❌ `{tbl}` — load failed")

    st.divider()
    st.subheader("Sample questions")
    SAMPLES = [
        "Doanh thu theo tháng năm 2018?",
        "Top 10 bang có nhiều đơn hàng nhất?",
        "Tỷ lệ giao hàng trễ theo bang?",
        "Seller nào review tốt nhất với hơn 100 đơn?",
        "Phân tích churn: bao nhiêu % khách hàng là churned?",
        "So sánh doanh thu theo phương thức thanh toán?",
        "Tỷ lệ chuyển đổi lead theo kênh marketing?",
        "Tỷ lệ sentiment tích cực / tiêu cực của review?",
        # Multi-query examples (tool use)
        "Phân tích toàn diện top 5 seller: doanh thu, review score, tỷ lệ giao trễ",
        "So sánh Q1/2017 vs Q1/2018: số đơn, doanh thu, review score",
    ]
    for q in SAMPLES:
        if st.button(q, use_container_width=True, key=f"s_{hash(q)}"):
            st.session_state["_pending"] = q

    st.divider()
    if st.button("🗑 Clear conversation", use_container_width=True):
        st.session_state["messages"]     = []
        st.session_state["chat_history"] = []
        st.rerun()

# ── Main chat area ────────────────────────────────────────────────────────────

st.title("🤖 Olist Agentic BI")
st.caption("Đặt câu hỏi về dữ liệu Olist — AI phân tích intent, sinh SQL, thực thi và tóm tắt insight.")

for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        if msg["role"] == "user":
            st.markdown(msg["content"])
        else:
            _render_assistant(msg)

# ── Chat input ────────────────────────────────────────────────────────────────

user_input = st.chat_input("Nhập câu hỏi... (ví dụ: Doanh thu theo tháng 2018?)")
if "_pending" in st.session_state:
    user_input = st.session_state.pop("_pending")

if not user_input:
    st.stop()

# ── Process ───────────────────────────────────────────────────────────────────

st.session_state["messages"].append({"role": "user", "content": user_input})
with st.chat_message("user"):
    st.write(user_input)

with st.chat_message("assistant"):

    is_valid, reject_reason = validate(user_input)
    if not is_valid:
        st.warning(reject_reason)
        st.session_state["messages"].append({
            "role": "assistant", "intent": "SMALLTALK",
            "text": reject_reason, "sql": None, "explanation": None,
            "chart_type": "none", "result_df": None, "all_queries": [],
            "summary": None, "error": None, "question": user_input,
        })
        st.stop()

    intent = classify_intent(user_input, st.session_state["chat_history"])

    with st.status("Đang xử lý...", expanded=True) as status:
        if intent == "SMALLTALK":
            status.write("💬 Đang trả lời...")
        elif intent == "FOLLOWUP":
            status.write("🔗 Phân tích follow-up...")
        else:
            if AI_PROVIDER == "anthropic":
                status.write("🔍 Phân tích câu hỏi → gọi tool query_database...")
            else:
                status.write("🔍 Phân tích câu hỏi và sinh SQL...")

        query_count = [0]

        def _on_query(n: int, sql: str, explanation: str) -> None:
            query_count[0] = n
            status.write(f"**Query {n}** — {explanation}")

        res = run(
            intent=intent,
            question=user_input,
            history=st.session_state["chat_history"],
            schema=schema_ctx,
            con=con,
            on_query=_on_query,
        )

        n_q = query_count[0]
        if n_q > 1:
            status.update(label=f"✅ Hoàn thành — {n_q} queries", state="complete", expanded=False)
        else:
            status.update(label="✅ Hoàn thành", state="complete", expanded=False)

    _render_assistant({
        "intent":      intent,
        "text":        res.get("text"),
        "summary":     res.get("summary"),
        "sql":         res.get("sql"),
        "chart_type":  res.get("chart_type", "none"),
        "result_df":   res.get("result_df"),
        "all_queries": res.get("all_queries", []),
        "error":       res.get("error"),
        "question":    user_input,
    })

# ── Update conversation history ───────────────────────────────────────────────

st.session_state["chat_history"].append({"role": "user", "content": user_input})
st.session_state["chat_history"].append({
    "role": "assistant",
    "content": (
        res.get("summary") or res.get("text") or res.get("explanation") or res.get("error") or ""
    ),
})

st.session_state["messages"].append({
    "role":        "assistant",
    "intent":      intent,
    "text":        res.get("text"),
    "summary":     res.get("summary"),
    "explanation": res.get("explanation"),
    "sql":         res.get("sql"),
    "chart_type":  res.get("chart_type", "none"),
    "result_df":   res.get("result_df"),
    "all_queries": res.get("all_queries", []),
    "error":       res.get("error"),
    "question":    user_input,
})
