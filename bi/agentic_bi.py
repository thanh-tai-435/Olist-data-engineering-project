"""
Olist Agentic BI — Streamlit UI
Natural Language → Intent → SQL → Chart + Insight
"""
import streamlit as st

from config import active_llm_key, active_model_label, AI_PROVIDER
from database import get_db_connection, build_schema_context, get_kpi_metrics
from validator import validate, classify_intent
from agent import run
from charts import render_chart

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Olist Agentic BI",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  .stChatMessage { border-radius:10px; }
  div[data-testid="metric-container"] {
    background:#1e1e2e; border-radius:10px; padding:12px;
  }
  .intent-badge {
    display:inline-block; font-size:0.72rem; font-weight:600;
    padding:2px 8px; border-radius:12px; margin-bottom:6px;
  }
  .badge-data    { background:#1e3a5f; color:#60a5fa; }
  .badge-follow  { background:#1e3a2f; color:#34d399; }
  .badge-small   { background:#2d1e3a; color:#c084fc; }
</style>
""", unsafe_allow_html=True)

_INTENT_BADGE = {
    "DATA_QUERY": '<span class="intent-badge badge-data">📊 Data Query</span>',
    "FOLLOWUP":   '<span class="intent-badge badge-follow">🔗 Follow-up</span>',
    "SMALLTALK":  '<span class="intent-badge badge-small">💬 Smalltalk</span>',
}

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

if "messages"     not in st.session_state: st.session_state["messages"]     = []
if "chat_history" not in st.session_state: st.session_state["chat_history"] = []

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🤖 Olist Agentic BI")
    st.caption(f"Model: `{active_model_label()}` via {AI_PROVIDER}")
    st.divider()

    # KPI cards
    kpis = get_kpi_metrics(con)
    if kpis:
        col1, col2 = st.columns(2)
        col1.metric("Orders",   kpis.get("total_orders", "–"))
        col2.metric("Revenue",  kpis.get("total_revenue", "–"))
        col1.metric("Avg ⭐",   kpis.get("avg_review", "–"))
        col2.metric("Customers",kpis.get("unique_customers", "–"))
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
        "So sánh doanh thu theo phương thức thanh toán?",
        "Tỷ lệ chuyển đổi lead theo kênh marketing?",
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
st.caption("Đặt câu hỏi về dữ liệu Olist — AI phân tích intent, sinh SQL, thực thi và tóm tắt insight.")

# ── Render chat history ───────────────────────────────────────────────────────

for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        if msg["role"] == "user":
            st.write(msg["content"])
        else:
            intent = msg.get("intent", "DATA_QUERY")
            st.markdown(_INTENT_BADGE.get(intent, ""), unsafe_allow_html=True)

            if msg.get("text"):                    # SMALLTALK or FOLLOWUP
                st.write(msg["text"])

            if msg.get("summary"):                 # DATA_QUERY — insight first
                st.info(msg["summary"])

            if msg.get("explanation") and not msg.get("summary"):
                st.write(msg["explanation"])

            if msg.get("sql"):
                with st.expander("🔍 SQL được sinh", expanded=False):
                    st.code(msg["sql"], language="sql")

            df_s = msg.get("result_df")
            if df_s is not None and not df_s.empty:
                with st.expander(f"📋 Dữ liệu ({len(df_s)} dòng)", expanded=False):
                    st.dataframe(df_s, use_container_width=True, height=220)
                render_chart(df_s, msg.get("chart_type","none"),
                             question=msg.get("question",""),
                             llm_hint=msg.get("chart_type","none"))

            if msg.get("error"):
                st.error(msg["error"])

# ── Chat input ─────────────────────────────────────────────────────────────────

user_input = st.chat_input("Nhập câu hỏi... (ví dụ: Doanh thu theo tháng 2018?)")
if "_pending" in st.session_state:
    user_input = st.session_state.pop("_pending")

if not user_input:
    st.stop()

# ── Process user message ──────────────────────────────────────────────────────

st.session_state["messages"].append({"role": "user", "content": user_input})
with st.chat_message("user"):
    st.write(user_input)

with st.chat_message("assistant"):

    # 1. Validate
    is_valid, reject_reason = validate(user_input)
    if not is_valid:
        st.warning(reject_reason)
        st.session_state["messages"].append({
            "role": "assistant", "intent": "SMALLTALK",
            "text": reject_reason, "sql": None, "explanation": None,
            "chart_type": "none", "result_df": None, "summary": None,
            "error": None, "question": user_input,
        })
        st.stop()

    # 2. Classify intent
    intent = classify_intent(user_input, st.session_state["chat_history"])
    st.markdown(_INTENT_BADGE.get(intent, ""), unsafe_allow_html=True)

    # 3. Run agent with live progress
    with st.status("Đang xử lý...", expanded=True) as status:
        if intent == "SMALLTALK":
            status.write("💬 Câu chuyện thường...")
        elif intent == "FOLLOWUP":
            status.write("🔗 Phân tích follow-up từ lịch sử...")
        else:
            status.write("🔍 Phân tích câu hỏi...")
            status.write("⚙️ Sinh SQL...")

        res = run(
            intent=intent,
            question=user_input,
            history=st.session_state["chat_history"],
            schema=schema_ctx,
            con=con,
        )

        if res.get("summary"):
            status.write("📝 Tóm tắt insight...")

        status.update(label="✅ Hoàn thành", state="complete", expanded=False)

    # 4. Render result
    if res.get("text"):
        st.write(res["text"])

    if res.get("summary"):
        st.info(res["summary"])

    if res.get("explanation") and not res.get("summary"):
        st.write(res["explanation"])

    if res.get("sql"):
        with st.expander("🔍 SQL được sinh", expanded=False):
            st.code(res["sql"], language="sql")

    df_res = res.get("result_df")
    if df_res is not None and not df_res.empty:
        with st.expander(f"📋 Dữ liệu ({len(df_res)} dòng)", expanded=False):
            st.dataframe(df_res, use_container_width=True, height=220)
        render_chart(df_res, res.get("chart_type","none"),
                     question=user_input, llm_hint=res.get("chart_type","none"))

    if res.get("error"):
        st.error(res["error"])

# 5. Update conversation history
st.session_state["chat_history"].append({"role": "user", "content": user_input})
assistant_history_content = (
    res.get("summary") or res.get("text") or res.get("explanation") or res.get("error") or ""
)
st.session_state["chat_history"].append({
    "role": "assistant", "content": assistant_history_content,
})

# 6. Save to display history
st.session_state["messages"].append({
    "role":        "assistant",
    "intent":      intent,
    "text":        res.get("text"),
    "summary":     res.get("summary"),
    "explanation": res.get("explanation"),
    "sql":         res.get("sql"),
    "chart_type":  res.get("chart_type", "none"),
    "result_df":   df_res,
    "error":       res.get("error"),
    "question":    user_input,
})
