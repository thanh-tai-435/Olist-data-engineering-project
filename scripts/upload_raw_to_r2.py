"""
Upload toàn bộ raw CSV lên R2 làm archive bất biến.
Đích: s3://olist-lakehouse/raw/ecommerce/ và .../raw/marketing/

Cách dùng:
  python scripts/upload_raw_to_r2.py
"""
import os
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    region_name=os.environ.get("S3_REGION", "auto"),
)

BUCKET = "retail-data-lake"
LOCAL_DATA = Path("data/raw")


def upload_directory(local_dir: Path, s3_prefix: str):
    files = sorted(local_dir.glob("*.csv"))
    if not files:
        print(f"  [WARN] No CSV files found in {local_dir}")
        return

    print(f"\nUploading {len(files)} files: {local_dir} → s3://{BUCKET}/{s3_prefix}")

    for f in files:
        key = f"{s3_prefix}/{f.name}"
        size_mb = f.stat().st_size / 1_048_576
        print(f"  {size_mb:6.2f} MB  {f.name}")
        s3.upload_file(
            Filename=str(f),
            Bucket=BUCKET,
            Key=key,
        )

    print(f"  Done ({len(files)} files).")


upload_directory(LOCAL_DATA / "ecommerce", "raw/ecommerce")
upload_directory(LOCAL_DATA / "marketing", "raw/marketing")

print("\n=== Files on R2 (raw/) ===")
resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/")
for obj in resp.get("Contents", []):
    print(f"  {obj['Key']:70s}  {obj['Size'] / 1024:8.1f} KB")
