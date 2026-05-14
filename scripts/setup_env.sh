#!/usr/bin/env bash
# =============================================================
# setup_env.sh — Tự động cấu hình .env cho môi trường hiện tại
#
# Dùng khi:
#   - Lần đầu clone repo
#   - Chạy trên GitHub Codespaces
#   - Chuyển đổi giữa local ↔ Codespaces
#
# Cách dùng:
#   chmod +x scripts/setup_env.sh
#   ./scripts/setup_env.sh
# =============================================================

set -euo pipefail

ENV_FILE=".env"

# ── Kiểm tra .env tồn tại ────────────────────────────────────
if [[ ! -f "$ENV_FILE" ]]; then
  cp .env.example "$ENV_FILE"
  echo "[INFO] .env chưa có → tạo từ .env.example"
fi

# ── Phát hiện môi trường ─────────────────────────────────────
if [[ -n "${CODESPACE_NAME:-}" ]]; then
  ENV_TYPE="codespaces"
  BASE="https://${CODESPACE_NAME}"
  echo "══════════════════════════════════════════════════════"
  echo "  Môi trường: GitHub Codespaces"
  echo "  CODESPACE_NAME: $CODESPACE_NAME"
  echo "══════════════════════════════════════════════════════"
else
  ENV_TYPE="local"
  BASE="http://localhost"
  echo "══════════════════════════════════════════════════════"
  echo "  Môi trường: Local"
  echo "══════════════════════════════════════════════════════"
fi

# ── Tính URLs theo môi trường ────────────────────────────────
if [[ "$ENV_TYPE" == "codespaces" ]]; then
  PREFECT_UI_API_URL="${BASE}-4200.app.github.dev/api"
  ICEBERG_REST_URI="${BASE}-8181.app.github.dev"
  MLFLOW_TRACKING_URI="${BASE}-5000.app.github.dev"
  REDPANDA_BROKERS="${BASE}-19092.app.github.dev"
else
  PREFECT_UI_API_URL="http://localhost:4200/api"
  ICEBERG_REST_URI="http://localhost:8181"
  MLFLOW_TRACKING_URI="http://localhost:5000"
  REDPANDA_BROKERS="localhost:19092"
fi

# ── Hàm ghi/cập nhật biến trong .env ────────────────────────
set_env_var() {
  local key="$1"
  local value="$2"

  if grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
    # Cập nhật dòng hiện có
    sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
  elif grep -q "^#\s*${key}=" "$ENV_FILE" 2>/dev/null; then
    # Bỏ comment và set giá trị
    sed -i "s|^#\s*${key}=.*|${key}=${value}|" "$ENV_FILE"
  else
    # Thêm mới vào cuối section Service URLs
    echo "${key}=${value}" >> "$ENV_FILE"
  fi
}

# ── Ghi các URL vào .env ─────────────────────────────────────
set_env_var "PREFECT_UI_API_URL"  "$PREFECT_UI_API_URL"
set_env_var "ICEBERG_REST_URI"    "$ICEBERG_REST_URI"
set_env_var "MLFLOW_TRACKING_URI" "$MLFLOW_TRACKING_URI"
set_env_var "REDPANDA_BROKERS"    "$REDPANDA_BROKERS"

# ── In kết quả ───────────────────────────────────────────────
echo ""
echo "  Service URLs đã cấu hình:"
echo ""
printf "  %-22s %s\n" "Prefect UI:"       "$PREFECT_UI_API_URL"
printf "  %-22s %s\n" "Iceberg REST:"     "$ICEBERG_REST_URI"
printf "  %-22s %s\n" "MLflow:"           "$MLFLOW_TRACKING_URI"
printf "  %-22s %s\n" "Redpanda Brokers:" "$REDPANDA_BROKERS"
echo ""
echo "  UI endpoints (mở trong trình duyệt):"
if [[ "$ENV_TYPE" == "codespaces" ]]; then
  printf "  %-22s %s\n" "Prefect UI:"       "https://${CODESPACE_NAME}-4200.app.github.dev"
  printf "  %-22s %s\n" "MLflow UI:"        "https://${CODESPACE_NAME}-5000.app.github.dev"
  printf "  %-22s %s\n" "Redpanda Console:" "https://${CODESPACE_NAME}-8080.app.github.dev"
  printf "  %-22s %s\n" "Trino UI:"         "https://${CODESPACE_NAME}-8082.app.github.dev"
  printf "  %-22s %s\n" "Streamlit:"        "https://${CODESPACE_NAME}-8501.app.github.dev"
else
  printf "  %-22s %s\n" "Prefect UI:"       "http://localhost:4200"
  printf "  %-22s %s\n" "MLflow UI:"        "http://localhost:5000"
  printf "  %-22s %s\n" "Redpanda Console:" "http://localhost:8080"
  printf "  %-22s %s\n" "Trino UI:"         "http://localhost:8082"
  printf "  %-22s %s\n" "Streamlit:"        "http://localhost:8501"
fi
echo ""
echo "══════════════════════════════════════════════════════"
echo "  Bước tiếp theo: docker compose up -d --force-recreate"
echo "══════════════════════════════════════════════════════"
