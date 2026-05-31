# ============================================================
# config.py - Centralized configuration loader
# Loads and validates all environment variables at startup.
# If any required variable is missing, the app exits early
# with a clear terminal message instead of failing silently.
# ============================================================

import os
import sys
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    """
    Fetch a required environment variable.
    Exits with a descriptive error if the variable is not set,
    preventing the app from running in a broken state.
    """
    value = os.getenv(key)
    if not value:
        print(f"[CONFIG ERROR] Bien moi truong bat buoc '{key}' chua duoc thiet lap.")
        print(f"[CONFIG ERROR] Vui long kiem tra file .env va thu lai.")
        sys.exit(1)
    return value


# ---- Required -------------------------------------------------------
GROQ_API_KEY: str = _require("GROQ_API_KEY")

# ---- Optional with defaults -----------------------------------------
GROQ_MODEL: str    = os.getenv("GROQ_MODEL", "llama3-70b-8192")
DUCKDB_PATH: str   = os.getenv("DUCKDB_PATH", "data/agentic_bi.duckdb")
MAX_SQL_RETRIES: int = int(os.getenv("MAX_SQL_RETRIES", "3"))
APP_LANG: str      = os.getenv("APP_LANG", "vi")

# ---- Derived --------------------------------------------------------
DATA_DIR: str = os.path.dirname(DUCKDB_PATH)
