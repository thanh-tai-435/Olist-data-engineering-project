"""
Watermark tracking for incremental Spark pipeline.

Stores per-table high-water marks in PostgreSQL (pipeline_watermarks).
Each Silver and Gold table has its own watermark so transforms are
independently resumable.
"""
import logging
import os
from datetime import datetime, timezone

import psycopg2

log = logging.getLogger(__name__)

_EPOCH = "1970-01-01 00:00:00+00"

_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    table_name  TEXT        PRIMARY KEY,
    watermark   TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def _url() -> str:
    pw   = os.environ.get("POSTGRES_PASSWORD", "olistpass")
    host = os.environ.get("POSTGRES_HOST",     "postgres")
    return f"postgresql://olist:{pw}@{host}:5432/postgres"


def _conn():
    return psycopg2.connect(_url())


def get(table_name: str) -> str:
    """Return stored watermark for *table_name*, or epoch if first run."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(_DDL)
        cur.execute(
            "SELECT watermark FROM pipeline_watermarks WHERE table_name = %s",
            (table_name,),
        )
        row = cur.fetchone()
        return str(row[0]) if row else _EPOCH


def save(table_name: str, ts) -> None:
    """Upsert watermark for *table_name*."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(_DDL)
        cur.execute(
            """
            INSERT INTO pipeline_watermarks (table_name, watermark, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (table_name)
            DO UPDATE SET watermark = EXCLUDED.watermark, updated_at = NOW()
            """,
            (table_name, str(ts)),
        )
    log.debug("watermark saved  %s = %s", table_name, ts)


def now_utc() -> str:
    return datetime.now(tz=timezone.utc).isoformat()
