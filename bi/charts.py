"""Chart rendering with dual detection: question intent signals + data shape fallback."""
import re
import pandas as pd
import plotly.express as px
import streamlit as st

# ── Dark theme defaults ───────────────────────────────────────────────────────

_LAYOUT = dict(
    height=400,
    margin=dict(l=0, r=0, t=36, b=0),
    plot_bgcolor="#0e1117",
    paper_bgcolor="#0e1117",
    font_color="#fafafa",
)

_COLORS = px.colors.qualitative.Pastel

# ── Intent signal → chart type ────────────────────────────────────────────────

_INTENT_SIGNALS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(bi[eể]u\s*[đd][oồ]\s*tr[oò]n|pie|t[yỷ]\s*l[eệ]|ph[aầ]n\s*tr[aă]m)\b", re.I), "pie"),
    (re.compile(r"\b(ph[eễ]u|funnel|chuy[eể]n\s*[đd][oổ]i\s*theo\s*b[uư][oớ]c)\b", re.I),         "funnel"),
    (re.compile(r"\b(xu\s*h[uư][oớ]ng|th[eế]\s*theo\s*th[oờ]i|trend|time\s*series|theo\s*th[aá]ng|theo\s*n[aă]m)\b", re.I), "line"),
    (re.compile(r"\b(ph[aâ]n\s*t[aá]n|scatter|t[uư][oơ]ng\s*quan|correlation)\b", re.I),            "scatter"),
    (re.compile(r"\b(so\s*s[aá]nh|top\s*\d+|x[eế]p\s*h[aạ]ng|rank|h[oô]\s*c[oộ]t|bar)\b", re.I),  "bar"),
    (re.compile(r"\b(radar|web|spider|[đd]a\s*chi[eề]u)\b", re.I),                                   "radar"),
]


def detect_chart_type(question: str, llm_hint: str, df: pd.DataFrame) -> str:
    """
    Priority:
      1. Question intent signal (regex)
      2. LLM hint from SQL generation
      3. Data shape heuristic
    """
    # 1. Question intent signal
    for pattern, chart in _INTENT_SIGNALS:
        if pattern.search(question):
            return chart

    # 2. LLM hint (trust if valid)
    if llm_hint in ("bar", "line", "pie", "scatter", "none"):
        return llm_hint

    # 3. Data shape heuristic
    if df is None or df.empty:
        return "none"
    cols = list(df.columns)
    num_cols   = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols   = [c for c in cols if not pd.api.types.is_numeric_dtype(df[c])]

    if len(df) == 1:
        return "none"
    if len(cols) == 2 and len(cat_cols) == 1 and len(num_cols) == 1:
        if len(df) <= 8:
            return "pie"
        return "bar"
    if len(num_cols) >= 2 and len(cat_cols) == 0:
        return "scatter"
    if len(num_cols) >= 1 and len(cat_cols) >= 1:
        return "bar"
    return "none"


def render_chart(df: pd.DataFrame, chart_type: str, question: str = "", llm_hint: str = "") -> None:
    """Render a Plotly chart with auto type detection."""
    if df is None or df.empty:
        return

    effective_type = detect_chart_type(question, llm_hint, df)
    if effective_type == "none":
        return

    cols = list(df.columns)
    try:
        if effective_type == "bar" and len(cols) >= 2:
            fig = px.bar(df, x=cols[0], y=cols[1],
                         color_discrete_sequence=_COLORS,
                         title=None)

        elif effective_type == "line" and len(cols) >= 2:
            fig = px.line(df, x=cols[0], y=cols[1],
                          markers=True, color_discrete_sequence=_COLORS)

        elif effective_type == "pie" and len(cols) >= 2:
            fig = px.pie(df, names=cols[0], values=cols[1],
                         hole=0.4, color_discrete_sequence=_COLORS)

        elif effective_type == "scatter" and len(cols) >= 2:
            fig = px.scatter(df, x=cols[0], y=cols[1],
                             color=cols[2] if len(cols) > 2 else None,
                             color_discrete_sequence=_COLORS)

        elif effective_type == "funnel" and len(cols) >= 2:
            fig = px.funnel(df, x=cols[1], y=cols[0],
                            color_discrete_sequence=_COLORS)

        elif effective_type == "radar" and len(cols) >= 2:
            fig = px.line_polar(df, r=cols[1], theta=cols[0],
                                line_close=True, color_discrete_sequence=_COLORS)
            fig.update_traces(fill="toself")

        else:
            return

        fig.update_layout(**_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.caption(f"Không render được chart: {e}")
