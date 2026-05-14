#!/usr/bin/env sh
# Post-create setup for Olist Data Lakehouse Codespace
set -e

VENV_PATH="/workspaces/.venv"

echo "=== [1/4] Setting up Python virtual environment ==="
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache python3 py3-pip py3-virtualenv gcc musl-dev libffi-dev
elif command -v apt-get >/dev/null 2>&1; then
  # Xóa các repo lỗi GPG (yarn) để apt-get update không fail
  sudo rm -f /etc/apt/sources.list.d/yarn.list
  sudo apt-get update -qq || true
  sudo apt-get install -y python3-pip python3-venv build-essential libffi-dev || true
fi

python3 -m venv "$VENV_PATH"
. "$VENV_PATH/bin/activate"
pip install --upgrade pip --quiet

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

echo "=== [3/4] Auto-activating venv in shell profiles ==="
for RC in ~/.bashrc ~/.zshrc ~/.profile; do
  if [ -f "$RC" ] && ! grep -q "workspaces/.venv" "$RC" 2>/dev/null; then
    echo ". $VENV_PATH/bin/activate" >> "$RC"
  fi
done

echo "=== [4/4] Creating .env from .env.example ==="
if [ ! -f .env ]; then
  if [ -f .env.example ]; then
    cp .env.example .env
    echo "  → .env created. Edit with your R2 credentials."
  fi
else
  echo "  → .env already exists, skipping."
fi

echo ""
echo "=== Setup complete! ==="
echo "  venv: $VENV_PATH (auto-activated in new terminals)"
echo ""
echo "Next steps:"
echo "  1. Fill in .env with your Cloudflare R2 credentials"
echo "  2. docker compose up -d"
