#!/usr/bin/env bash
# Post-create setup for Olist Data Lakehouse Codespace
set -e

echo "=== [1/4] Upgrading pip ==="
python3 -m pip install --upgrade pip --quiet

echo "=== [2/4] Installing Python dependencies ==="
pip install --quiet \
  pandas==2.2.0 \
  pyarrow==15.0.0 \
  "pyiceberg[s3fs,pandas]==0.6.0" \
  confluent-kafka==2.3.0 \
  dbt-core==1.8.0 \
  dbt-duckdb==1.8.0 \
  duckdb==0.10.0 \
  mlflow==2.13.0 \
  scikit-learn==1.4.0 \
  xgboost==2.0.3 \
  boto3==1.34.0 \
  prefect==3.0.0 \
  requests==2.31.0

echo "=== [3/4] Creating .env from .env.example ==="
if [ ! -f .env ]; then
  if [ -f .env.example ]; then
    cp .env.example .env
    echo "  → .env created. Edit it with your R2 credentials before running compose."
  else
    echo "  → No .env.example found, skipping."
  fi
else
  echo "  → .env already exists, skipping."
fi

echo "=== [4/4] Done! ==="
echo ""
echo "Next steps:"
echo "  1. Fill in .env with your Cloudflare R2 credentials"
echo "  2. Run: docker compose up -d"
echo "  3. Optional Spark:  docker compose --profile spark up -d"
echo "  4. Optional Trino:  docker compose --profile query up -d"
echo "  5. Optional BI:     docker compose --profile bi    up -d"
echo ""
echo "Service URLs (via Codespace port forwarding):"
echo "  Prefect UI       → PORT 4200"
echo "  MLflow UI        → PORT 5000"
echo "  Redpanda Console → PORT 8080"
echo "  Iceberg REST     → PORT 8181"
echo "  Streamlit BI     → PORT 8501"
