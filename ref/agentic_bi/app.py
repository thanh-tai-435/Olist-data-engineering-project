# ============================================================
# app.py v4
# Fix: truyền chart_key duy nhất (f"chart_{idx}") vào
#      render_smart_chart → tránh Streamlit duplicate ID crash
# ============================================================

import streamlit as st
import pandas as pd
from pathlib import Path
from agent import run_agent, AgentEvent
from database import get_kpi_summary
from charts import render_smart_chart

st.set_page_config(page_title="Agentic BI", page_icon=None,
                   layout="wide", initial_sidebar_state="expanded")

def _load_css(p):
    st.markdown(f"<style>{Path(p).read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

_load_css("static/style.css")

LOGO_SVG = """<svg width="36" height="36" viewBox="0 0 36 36" fill="none">
  <rect width="36" height="36" rx="8" fill="#0D1928"/>
  <rect x="5"  y="22" width="5" height="9"  rx="1.5" fill="#1E3A5F"/>
  <rect x="12" y="16" width="5" height="15" rx="1.5" fill="#2A5298"/>
  <rect x="19" y="10" width="5" height="21" rx="1.5" fill="#3B82F6"/>
  <rect x="26" y="14" width="5" height="17" rx="1.5" fill="#60A5FA"/>
  <polyline points="7.5,22 14.5,16 21.5,10 28.5,14"
    stroke="#10B981" stroke-width="1.5" stroke-linecap="round"
    stroke-linejoin="round" fill="none"/>
  <circle cx="21.5" cy="10" r="2" fill="#10B981"/>
</svg>"""

def _init():
    for k,v in {"messages":[],"date_filter":"Toàn bộ (180 ngày)"}.items():
        if k not in st.session_state: st.session_state[k]=v

_init()

def _fmt(v):
    if v>=1e12: return f"{v/1e12:.1f} Nghìn Tỷ"
    if v>=1e9:  return f"{v/1e9:.1f} Tỷ"
    if v>=1e6:  return f"{v/1e6:.0f} Triệu"
    return f"{v:,.0f}"

# ---- Sidebar ---------------------------------------------------
with st.sidebar:
    st.markdown(
        f'<div class="logo-wrap"><div>{LOGO_SVG}</div>'
        f'<div><div class="logo-text-main">Agentic BI</div>'
        f'<div class="logo-text-sub">Business Intelligence</div></div></div>',
        unsafe_allow_html=True)
    st.markdown("---")
    st.markdown('<p style="font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:2px;color:#2A4060;text-transform:uppercase;margin-bottom:6px;">Bộ lọc thời gian</p>', unsafe_allow_html=True)
    st.session_state["date_filter"] = st.selectbox("",
        ["Toàn bộ (180 ngày)","30 ngày gần nhất","7 ngày gần nhất"],
        label_visibility="collapsed")
    st.markdown("---")
    st.markdown('<p style="font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:2px;color:#2A4060;text-transform:uppercase;margin-bottom:8px;">Trạng thái hệ thống</p>', unsafe_allow_html=True)
    st.markdown(
        '<div class="status-row"><div class="dot-ok"></div>Kết nối Groq API</div>'
        '<div class="status-row"><div class="dot-ok"></div>DuckDB Gold Layer</div>'
        '<div class="status-row"><div class="dot-ok"></div>llama-3.3-70b-versatile</div>',
        unsafe_allow_html=True)
    st.markdown("---")
    st.markdown('<p style="font-family:IBM Plex Mono,monospace;font-size:9px;letter-spacing:2px;color:#2A4060;text-transform:uppercase;margin-bottom:8px;">Câu hỏi mẫu</p>', unsafe_allow_html=True)
    for s in ["Sản phẩm nào bán chạy nhất?","Doanh thu theo kênh Online?",
              "Kênh quảng cáo nào có tỷ lệ chuyển đổi cao nhất?",
              "Top 3 nhà môi giới doanh thu cao nhất?","Phễu marketing tuần gần nhất?"]:
        st.markdown(f'<div style="font-size:11px;color:#2A4A6A;padding:3px 0;">→ {s}</div>',
                    unsafe_allow_html=True)
    st.markdown("---")
    if st.button("Xóa lịch sử hội thoại", use_container_width=True):
        st.session_state["messages"] = []
        from agent import clear_history; clear_history()
        st.rerun()
    st.caption("v4.0  ·  DuckDB + Groq  ·  4 Gold Tables")

# ---- KPI cards -------------------------------------------------
kpis = get_kpi_summary()
c1,c2,c3,c4 = st.columns(4)
for col,label,value,sub,accent in [
    (c1,"TỔNG DOANH THU", _fmt(kpis["total_revenue"]),  "6 tháng gần nhất",  "#3B82F6"),
    (c2,"LƯỢT TIỀM NĂNG", f"{int(kpis['total_leads']):,}","Từ phễu marketing","#10B981"),
    (c3,"NHÀ MÔI GIỚI",   str(int(kpis["total_agents"])),"Đang hoạt động",    "#F59E0B"),
    (c4,"SẢN PHẨM",       str(int(kpis["total_products"])),"Trong danh mục",  "#8B5CF6"),
]:
    with col:
        st.markdown(
            f'<div class="kpi-card" style="--accent:{accent};border-top-color:{accent}">'
            f'<div class="kpi-label">{label}</div><div class="kpi-value">{value}</div>'
            f'<div class="kpi-sub" style="color:{accent}">{sub}</div></div>',
            unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ---- Chat timeline ---------------------------------------------
st.markdown('<div class="section-title">Trò chuyện với dữ liệu</div>', unsafe_allow_html=True)
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

for idx, msg in enumerate(st.session_state["messages"]):
    if msg["role"] == "user":
        st.markdown(
            f'<div class="chat-user"><div class="chat-label" style="color:#3B82F6">Câu hỏi</div>'
            f'{msg["content"]}</div>', unsafe_allow_html=True)
    else:
        content = msg["content"]
        is_err = "Xin lỗi" in content or "Lỗi:" in content or "chưa có dữ liệu" in content
        css_cls = "chat-error" if is_err else "chat-assistant"
        lbl_col = "#EF4444" if is_err else "#10B981"
        lbl_txt = "Thông báo" if is_err else "Phân tích"
        st.markdown(
            f'<div class="{css_cls}"><div class="chat-label" style="color:{lbl_col}">{lbl_txt}</div>'
            f'{content}</div>', unsafe_allow_html=True)

        data = msg.get("data", {})
        df: pd.DataFrame = data.get("df", pd.DataFrame())
        if not df.empty:
            # KEY FIX: chart_key duy nhất theo index → tránh duplicate ID crash
            render_smart_chart(df, data.get("tables",[]),
                               data.get("question",""),
                               chart_key=f"chart_{idx}")
            with st.expander("Xem bảng dữ liệu chi tiết", expanded=False):
                st.dataframe(df, use_container_width=True)
        if sql := data.get("sql",""):
            with st.expander("Câu lệnh SQL đã thực thi", expanded=False):
                st.code(sql, language="sql")

# ---- Chat input ------------------------------------------------
question = st.chat_input("Nhập câu hỏi về doanh thu, sản phẩm, marketing, môi giới...")

if question:
    st.session_state["messages"].append({"role":"user","content":question})
    with st.status("Agent đang phân tích...", expanded=True) as sb:
        final = None
        for ev in run_agent(question):
            st.write(ev.message)
            if ev.stage in ("done","error"): final = ev
        ok = final and final.stage == "done"
        sb.update(label="Hoàn thành." if ok else "Có lỗi.",
                  state="complete" if ok else "error", expanded=False)
    if final:
        if final.stage == "done":
            st.session_state["messages"].append({
                "role":"assistant",
                "content": final.payload.get("summary",""),
                "data": final.payload,
            })
        else:
            st.session_state["messages"].append({
                "role":"assistant","content":final.message,"data":{}})
    st.rerun()