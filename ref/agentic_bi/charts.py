# ============================================================
# charts.py v3
# Fix: thêm chart_key param vào render_smart_chart và
#      st.plotly_chart để tránh duplicate element ID crash
# ============================================================

import re
import unicodedata
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

COLORS = ["#3B82F6","#10B981","#F59E0B","#8B5CF6","#EF4444",
          "#06B6D4","#F97316","#EC4899","#84CC16","#A78BFA"]

LAYOUT_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="#050A12",
    font=dict(color="#64748B", family="IBM Plex Sans, sans-serif", size=12),
    title_font=dict(color="#94A3B8", size=13, family="IBM Plex Mono, monospace"),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#64748B", size=11)),
    margin=dict(l=16, r=16, t=44, b=16),
)
AXIS_STYLE = dict(
    gridcolor="#0D1828", linecolor="#0F1E2E",
    tickcolor="#0F1E2E", tickfont=dict(color="#475569", size=11), zeroline=False,
)

def _no_acc(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", t) if not unicodedata.combining(c)).lower()

def _base(fig: go.Figure, title="") -> go.Figure:
    fig.update_layout(**LAYOUT_BASE,
        title=dict(text=title, font=dict(color="#94A3B8", size=13)),
        xaxis=AXIS_STYLE, yaxis=AXIS_STYLE)
    return fig

# ---- Intent detection from question ----------------------------

def _wants_pie(q):    return any(k in _no_acc(q) for k in ["bieu do tron","pie","donut","ty trong","co cau","phan bo","phan tram"])
def _wants_funnel(q): return any(k in _no_acc(q) for k in ["pheu","funnel","chuyen doi","tien trinh"])
def _wants_line(q):   return any(k in _no_acc(q) for k in ["xu huong","trend","theo thoi gian","theo ngay","theo tuan","theo thang"])
def _wants_bar(q):    return any(k in _no_acc(q) for k in ["cot","thanh","bar chart","bieu do cot"])
def _wants_radar(q):  return any(k in _no_acc(q) for k in ["radar","da chieu","nhieu chi so"])

def _auto_type(df: pd.DataFrame) -> str:
    cols     = [c.lower() for c in df.columns]
    num_c    = df.select_dtypes(include="number").columns.tolist()
    date_c   = [c for c in df.columns if any(k in c.lower() for k in ["date","week","month","ngay","tuan"])]
    cat_c    = df.select_dtypes(include="object").columns.tolist()
    funnel_s = {"impressions","leads","qualified","converted"}
    if funnel_s.issubset(set(cols)):          return "funnel"
    if date_c and num_c and len(df) >= 5:     return "time_series"
    if len(num_c) >= 3 and cat_c and len(df) <= 8: return "radar"
    if cat_c and len(num_c) == 1 and len(df) <= 7: return "pie"
    if cat_c and len(num_c) == 1 and len(df) > 7:  return "bar_h"
    if len(num_c) >= 2 and not date_c:        return "scatter"
    return "bar"

# ---- Chart builders --------------------------------------------

def _time_series(df):
    num_c  = df.select_dtypes(include="number").columns.tolist()
    date_c = next(c for c in df.columns if any(k in c.lower() for k in ["date","week","month","ngay","tuan"]))
    cat_c  = df.select_dtypes(include="object").columns.tolist()
    y = num_c[0]
    if cat_c:
        fig = px.line(df, x=date_c, y=y, color=cat_c[0], color_discrete_sequence=COLORS)
    else:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df[date_c], y=df[y], mode="lines",
            line=dict(color=COLORS[0], width=2),
            fill="tozeroy", fillcolor="rgba(59,130,246,0.07)", name=y))
    fig.update_traces(line_width=2)
    return _base(fig, f"Xu hướng: {y}")

def _bar(df, horizontal=False):
    num_c = df.select_dtypes(include="number").columns.tolist()
    cat_c = df.select_dtypes(include="object").columns.tolist()
    date_c = [c for c in df.columns if any(k in c.lower() for k in ["date","week","ngay"])]
    x = cat_c[0] if cat_c else (date_c[0] if date_c else df.columns[0])
    y = num_c[0]
    col = cat_c[1] if len(cat_c) > 1 else None
    if horizontal:
        fig = px.bar(df.head(20), x=y, y=x, orientation="h", color=col, color_discrete_sequence=COLORS)
    else:
        fig = px.bar(df.head(20), x=x, y=y, color=col, color_discrete_sequence=COLORS)
    fig.update_traces(marker_line_width=0)
    return _base(fig, f"Phân tích: {y}")

def _grouped_bar(df):
    cat_c = df.select_dtypes(include="object").columns.tolist()
    num_c = df.select_dtypes(include="number").columns.tolist()
    x = cat_c[0] if cat_c else df.columns[0]
    fig = go.Figure()
    for i, col in enumerate(num_c[:5]):
        fig.add_trace(go.Bar(name=col, x=df[x], y=df[col],
                             marker_color=COLORS[i], marker_line_width=0))
    fig.update_layout(barmode="group")
    return _base(fig, "So sánh đa chỉ số")

def _funnel(df):
    order    = ["impressions","leads","qualified","converted"]
    labels   = ["Hiển thị","Tiềm năng","Đủ tiêu chuẩn","Chuyển đổi"]
    cat_c = df.select_dtypes(include="object").columns.tolist()
    if cat_c and len(df) > 1:
        fig = go.Figure()
        for i, (_, row) in enumerate(df.iterrows()):
            vals = [row.get(c,0) for c in order if c in df.columns]
            lbls = [labels[j] for j, c in enumerate(order) if c in df.columns]
            fig.add_trace(go.Funnel(name=str(row[cat_c[0]]), y=lbls, x=vals,
                                    marker_color=COLORS[i % len(COLORS)]))
    else:
        total = {c: int(df[c].sum()) for c in order if c in df.columns}
        lbls  = [labels[i] for i, c in enumerate(order) if c in df.columns]
        fig = go.Figure(go.Funnel(y=lbls, x=list(total.values()),
            textinfo="value+percent previous", marker=dict(color=COLORS[:len(total)])))
    fig.update_layout(**LAYOUT_BASE,
        title=dict(text="Phễu Marketing", font=dict(color="#94A3B8", size=13)))
    return fig

def _pie(df):
    num_c = df.select_dtypes(include="number").columns.tolist()
    cat_c = df.select_dtypes(include="object").columns.tolist()
    if not num_c or not cat_c:
        return None
    fig = go.Figure(go.Pie(
        labels=df[cat_c[0]], values=df[num_c[0]], hole=0.42,
        marker=dict(colors=COLORS[:len(df)], line=dict(color="#080C14", width=2)),
        textfont=dict(color="#94A3B8", size=11), textinfo="label+percent"))
    fig.update_layout(**LAYOUT_BASE,
        title=dict(text=f"Tỷ trọng: {num_c[0]}", font=dict(color="#94A3B8", size=13)))
    return fig

def _scatter(df):
    num_c = df.select_dtypes(include="number").columns.tolist()
    cat_c = df.select_dtypes(include="object").columns.tolist()
    fig = px.scatter(df, x=num_c[0], y=num_c[1],
        color=cat_c[0] if cat_c else None,
        size=num_c[2] if len(num_c) > 2 else None,
        color_discrete_sequence=COLORS,
        hover_name=cat_c[0] if cat_c else None)
    return _base(fig, f"Tương quan: {num_c[0]} vs {num_c[1]}")

def _radar(df):
    num_c = df.select_dtypes(include="number").columns.tolist()
    cat_c = df.select_dtypes(include="object").columns.tolist()
    if not cat_c or len(num_c) < 3:
        return None
    fig = go.Figure()
    for i, (_, row) in enumerate(df.iterrows()):
        fig.add_trace(go.Scatterpolar(
            r=[row[c] for c in num_c], theta=num_c, fill="toself",
            line=dict(color=COLORS[i % len(COLORS)], width=1.5),
            name=str(row[cat_c[0]])))
    fig.update_layout(**LAYOUT_BASE,
        polar=dict(bgcolor="#050A12",
            radialaxis=dict(gridcolor="#0D1828", tickfont=dict(color="#334155", size=9)),
            angularaxis=dict(gridcolor="#0D1828", tickfont=dict(color="#475569", size=10))),
        title=dict(text="So sánh đa chiều", font=dict(color="#94A3B8", size=13)))
    return fig

# ---- Entry point (with unique key) ----------------------------

def render_smart_chart(df: pd.DataFrame, tables: list,
                       question: str = "", chart_key: str = "") -> None:
    """
    chart_key: chuỗi duy nhất mỗi lần gọi để tránh Streamlit duplicate ID.
    """
    if df.empty or len(df) < 1:
        return

    fig: Optional[go.Figure] = None

    if _wants_pie(question):       fig = _pie(df)
    elif _wants_funnel(question):  fig = _funnel(df)
    elif _wants_line(question):    fig = _time_series(df)
    elif _wants_radar(question):   fig = _radar(df)
    elif _wants_bar(question):     fig = _bar(df)
    else:
        t = _auto_type(df)
        if   t == "time_series": fig = _time_series(df)
        elif t == "funnel":      fig = _funnel(df)
        elif t == "radar":       fig = _radar(df)
        elif t == "grouped_bar": fig = _grouped_bar(df)
        elif t == "bar_h":       fig = _bar(df, horizontal=True)
        elif t == "scatter":     fig = _scatter(df)
        elif t == "pie":         fig = _pie(df)
        else:                    fig = _bar(df)

    if fig:
        # key duy nhất = tránh crash duplicate element ID
        st.plotly_chart(fig, use_container_width=True,
                        key=chart_key if chart_key else None)
