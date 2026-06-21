"""Central config — reads from st.secrets (Streamlit Cloud) then env vars."""
import os
import sys


def _get(key: str, default: str = "") -> str:
    # st.secrets takes priority (Streamlit Cloud deployment)
    try:
        import streamlit as st
        val = st.secrets.get(key, "")
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(key, default)

def _require(key: str) -> str:
    val = _get(key)
    if not val:
        print(f"[config] ERROR: required env var {key!r} is not set.", file=sys.stderr)
        sys.exit(1)
    return val

# ── Iceberg / S3 ──────────────────────────────────────────────────────────────
ICEBERG_URI   = _get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
S3_ENDPOINT   = _get("S3_ENDPOINT")
S3_ACCESS_KEY = _get("S3_ACCESS_KEY")
S3_SECRET_KEY = _get("S3_SECRET_KEY")
S3_REGION     = _get("S3_REGION", "auto")

# ── LLM provider ──────────────────────────────────────────────────────────────
AI_PROVIDER      = _get("AI_PROVIDER", "anthropic")    # anthropic | openrouter | groq
ANTHROPIC_KEY    = _get("ANTHROPIC_API_KEY")
CLAUDE_MODEL     = _get("CLAUDE_MODEL", "claude-sonnet-4-6")
OPENROUTER_KEY   = _get("OPENROUTER_API_KEY")
OPENROUTER_MODEL = _get("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free")
GROQ_KEY         = _get("GROQ_API_KEY")
GROQ_MODEL       = _get("GROQ_MODEL", "llama-3.3-70b-versatile")

# ── App constants ──────────────────────────────────────────────────────────────
GOLD_TABLES  = ["fct_orders", "fct_funnel", "dim_sellers", "dim_customers", "review_sentiment"]
MAX_ROWS     = 1_000
MAX_HISTORY  = 10
MAX_RETRIES  = 3

def active_llm_key() -> str:
    return {"anthropic": ANTHROPIC_KEY, "openrouter": OPENROUTER_KEY, "groq": GROQ_KEY}.get(AI_PROVIDER, "")

def active_model_label() -> str:
    return {"anthropic": CLAUDE_MODEL, "openrouter": OPENROUTER_MODEL, "groq": GROQ_MODEL}.get(AI_PROVIDER, AI_PROVIDER)
