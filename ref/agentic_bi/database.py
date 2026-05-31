# ============================================================
# database.py v2
# Fix: toàn bộ revenue trong Gold layer dùng đơn vị VND thuần.
#      Thêm comment rõ đơn vị vào mỗi bảng để LLM không bị
#      lẫn lộn triệu/tỷ khi đọc schema.
# ============================================================

import os
import duckdb
import pandas as pd
from typing import Optional
from config import DUCKDB_PATH, DATA_DIR

_conn: Optional[duckdb.DuckDBPyConnection] = None

def get_connection() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        os.makedirs(DATA_DIR, exist_ok=True)
        _conn = duckdb.connect(DUCKDB_PATH)
        _initialize_gold_tables(_conn)
    return _conn

def _table_has_data(conn, table: str) -> bool:
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] > 0
    except Exception:
        return False

def _initialize_gold_tables(conn: duckdb.DuckDBPyConnection) -> None:
    # ---- gold_daily_revenue (revenue đơn vị VND, từ 50tr đến 500tr/ngày) ----
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_daily_revenue (
            date      DATE,
            channel   VARCHAR,
            revenue   DOUBLE,   -- VND, ví dụ: 450000000 = 450 triệu đồng
            orders    INTEGER,
            avg_order DOUBLE    -- VND trung bình mỗi đơn
        )
    """)
    if not _table_has_data(conn, "gold_daily_revenue"):
        conn.execute("""
            INSERT INTO gold_daily_revenue
            SELECT
                CAST('2024-01-01' AS DATE) + INTERVAL (n) DAY,
                CASE (n % 3) WHEN 0 THEN 'Online' WHEN 1 THEN 'Offline' ELSE 'Partner' END,
                ROUND(50000000 + RANDOM() * 450000000, 0),
                CAST(10 + RANDOM() * 90 AS INTEGER),
                ROUND(3000000 + RANDOM() * 20000000, 0)
            FROM generate_series(0, 179) t(n)
        """)

    # ---- gold_product_performance (revenue đơn vị VND) ----------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_product_performance (
            product_id   VARCHAR,
            product_name VARCHAR,
            category     VARCHAR,
            units_sold   INTEGER,
            revenue      DOUBLE,   -- VND tổng doanh thu sản phẩm
            return_rate  DOUBLE    -- tỷ lệ 0.0-1.0
        )
    """)
    if not _table_has_data(conn, "gold_product_performance"):
        conn.execute("""
            INSERT INTO gold_product_performance VALUES
            ('P001','Căn hộ cao cấp Vinhomes','Chung cư',   42, 84000000000, 0.02),
            ('P002','Nhà phố liên kế Gamuda', 'Nhà phố',    31, 46500000000, 0.03),
            ('P003','Biệt thự đơn lập An Phú','Biệt thự',  12, 72000000000, 0.01),
            ('P004','Căn hộ dịch vụ Masteri', 'Chung cư',   67, 47000000000, 0.04),
            ('P005','Shophouse mặt phố Q7',   'Shophouse',  25, 37500000000, 0.02),
            ('P006','Căn hộ Studio cỡ nhỏ',  'Chung cư',   95, 28500000000, 0.05),
            ('P007','Đất nền dự án Long An',  'Đất nền',    58, 23200000000, 0.01),
            ('P008','Khu nghỉ dưỡng Mũi Né',  'Nghỉ dưỡng', 18, 54000000000, 0.02)
        """)

    # ---- gold_marketing_funnel ----------------------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_marketing_funnel (
            week_start  DATE,
            channel     VARCHAR,
            impressions INTEGER,
            leads       INTEGER,
            qualified   INTEGER,
            converted   INTEGER
        )
    """)
    if not _table_has_data(conn, "gold_marketing_funnel"):
        conn.execute("""
            INSERT INTO gold_marketing_funnel
            SELECT
                CAST('2024-01-01' AS DATE) + INTERVAL (n*7) DAY,
                CASE (n%4) WHEN 0 THEN 'Facebook Ads'
                           WHEN 1 THEN 'Google Ads'
                           WHEN 2 THEN 'Zalo Ads'
                           ELSE 'Email' END,
                CAST(10000 + RANDOM()*90000 AS INTEGER),
                CAST(500   + RANDOM()*2000  AS INTEGER),
                CAST(100   + RANDOM()*500   AS INTEGER),
                CAST(10    + RANDOM()*100   AS INTEGER)
            FROM generate_series(0,25) t(n)
        """)

    # ---- gold_agent_performance (revenue VND) --------------------------------
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold_agent_performance (
            agent_id   VARCHAR,
            agent_name VARCHAR,
            region     VARCHAR,
            listings   INTEGER,
            closed     INTEGER,
            revenue    DOUBLE,   -- VND tổng doanh thu môi giới
            rating     DOUBLE
        )
    """)
    if not _table_has_data(conn, "gold_agent_performance"):
        conn.execute("""
            INSERT INTO gold_agent_performance VALUES
            ('A001','Nguyễn Văn An', 'TP.HCM', 25,18,27000000000,4.8),
            ('A002','Trần Thị Bích', 'Hà Nội', 31,22,33000000000,4.9),
            ('A003','Lê Hoàng Cường','Đà Nẵng',19,11,16500000000,4.5),
            ('A004','Phạm Minh Đức', 'TP.HCM', 28,20,30000000000,4.7),
            ('A005','Hoàng Thị Em',  'Cần Thơ',15, 8,12000000000,4.3),
            ('A006','Vũ Quốc Hùng',  'Hà Nội', 22,17,25500000000,4.6)
        """)

    conn.commit()


def execute_query(sql: str) -> tuple[pd.DataFrame, Optional[str]]:
    try:
        return get_connection().execute(sql).df(), None
    except Exception as e:
        return pd.DataFrame(), str(e)


# Đơn vị rõ ràng trong schema giúp LLM không hallucinate
GOLD_SCHEMA: dict[str, dict] = {
    "gold_daily_revenue": {
        "description": "Doanh thu hàng ngày theo kênh bán hàng. ĐƠN VỊ REVENUE: VND (đồng). Ví dụ: 450000000 = 450 triệu đồng.",
        "columns": {
            "date":      "DATE — ngày giao dịch",
            "channel":   "VARCHAR — kênh bán: Online / Offline / Partner",
            "revenue":   "DOUBLE — doanh thu VND (đồng). 1 tỷ = 1,000,000,000",
            "orders":    "INTEGER — số đơn hàng",
            "avg_order": "DOUBLE — giá trị TB mỗi đơn VND",
        }
    },
    "gold_product_performance": {
        "description": "Hiệu suất từng sản phẩm BĐS. ĐƠN VỊ REVENUE: VND (đồng).",
        "columns": {
            "product_id":   "VARCHAR — mã sản phẩm",
            "product_name": "VARCHAR — tên sản phẩm",
            "category":     "VARCHAR — loại hình BĐS",
            "units_sold":   "INTEGER — số căn/lô đã bán",
            "revenue":      "DOUBLE — doanh thu VND. Ví dụ: 84000000000 = 84 tỷ đồng",
            "return_rate":  "DOUBLE — tỷ lệ trả hàng 0.0–1.0",
        }
    },
    "gold_marketing_funnel": {
        "description": "Phễu marketing theo kênh quảng cáo, theo tuần.",
        "columns": {
            "week_start":  "DATE — ngày đầu tuần",
            "channel":     "VARCHAR — kênh QC: Facebook Ads / Google Ads / Zalo Ads / Email",
            "impressions": "INTEGER — lượt hiển thị",
            "leads":       "INTEGER — lượt tiềm năng",
            "qualified":   "INTEGER — tiềm năng đủ tiêu chuẩn",
            "converted":   "INTEGER — đã chuyển đổi thành khách hàng",
        }
    },
    "gold_agent_performance": {
        "description": "Hiệu suất nhà môi giới BĐS. ĐƠN VỊ REVENUE: VND (đồng).",
        "columns": {
            "agent_id":   "VARCHAR — mã môi giới",
            "agent_name": "VARCHAR — tên môi giới",
            "region":     "VARCHAR — khu vực phụ trách",
            "listings":   "INTEGER — tổng sản phẩm đang bán",
            "closed":     "INTEGER — số giao dịch đã chốt",
            "revenue":    "DOUBLE — doanh thu VND. Ví dụ: 27000000000 = 27 tỷ đồng",
            "rating":     "DOUBLE — điểm đánh giá 1–5",
        }
    },
}


def get_kpi_summary() -> dict:
    conn = get_connection()
    kpis = {}
    for key, sql in [
        ("total_revenue", "SELECT SUM(revenue) FROM gold_daily_revenue"),
        ("total_leads",   "SELECT SUM(leads)   FROM gold_marketing_funnel"),
        ("total_agents",  "SELECT COUNT(*)      FROM gold_agent_performance"),
        ("total_products","SELECT COUNT(*)      FROM gold_product_performance"),
    ]:
        try:
            kpis[key] = conn.execute(sql).fetchone()[0] or 0
        except Exception:
            kpis[key] = 0
    return kpis