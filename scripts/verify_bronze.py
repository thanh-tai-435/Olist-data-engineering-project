"""
Kiểm tra Bronze layer sau khi chạy ingestion.
Kết nối Iceberg REST catalog → list tables → đếm rows.

Cách dùng:
  python scripts/verify_bronze.py
  python scripts/verify_bronze.py --table ecommerce_orders   # xem sample rows
"""
import argparse
import os
import sys

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog

load_dotenv()

ICEBERG_URI = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", help="Tên bảng để xem sample (vd: ecommerce_orders)")
    args = parser.parse_args()

    try:
        catalog = load_catalog("rest", **{
            "uri": ICEBERG_URI,
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.region": os.environ.get("S3_REGION", "auto"),
            "s3.path-style-access": "false",
        })
    except Exception as e:
        print(f"[ERROR] Không kết nối được Iceberg REST tại {ICEBERG_URI}")
        print("        Chạy: docker compose up -d")
        print(f"        {e}")
        sys.exit(1)

    tables = catalog.list_tables("bronze")
    if not tables:
        print("Namespace 'bronze' chưa có bảng nào. Chạy ingestion trước.")
        sys.exit(0)

    print(f"{'─' * 55}")
    print(f"  BRONZE LAYER — {len(tables)} bảng")
    print(f"{'─' * 55}")

    total = 0
    for ns, tbl_name in tables:
        tbl = catalog.load_table(f"{ns}.{tbl_name}")
        scan = tbl.scan()
        df = scan.to_arrow().to_pydict()
        rows = len(next(iter(df.values()))) if df else 0
        total += rows
        print(f"  bronze.{tbl_name:<35} {rows:>8,} rows")

    print(f"{'─' * 55}")
    print(f"  TOTAL{'':<37} {total:>8,} rows")
    print(f"{'─' * 55}")

    if args.table:
        full = f"bronze.{args.table}"
        print(f"\n=== Sample: {full} (5 rows) ===")
        tbl = catalog.load_table(full)
        df = tbl.scan(limit=5).to_arrow().to_pandas()
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
