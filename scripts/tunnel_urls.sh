#!/bin/sh
# Auto-fetch trycloudflare URLs from running cf-* containers.
# Writes to /tunnel-urls.txt, stdout, and patches Coder workspace app URLs in Postgres.

CONTAINERS="olist-cf-prefect olist-cf-mlflow olist-cf-redpanda olist-cf-coder olist-cf-streamlit"
OUT=/tunnel-urls.txt
PGCONN="postgresql://olist:${POSTGRES_PASSWORD:-olistpass}@postgres:5432/coder"

get_url() {
  docker logs "$1" 2>&1 | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1
}

patch_coder_db() {
  PREFECT_URL="$1"
  MLFLOW_URL="$2"
  REDPANDA_URL="$3"
  [ -z "$PREFECT_URL" ] && [ -z "$MLFLOW_URL" ] && [ -z "$REDPANDA_URL" ] && return

  psql "$PGCONN" -q <<SQL 2>/dev/null
UPDATE workspace_apps SET url = '$MLFLOW_URL'   WHERE slug = 'mlflow'   AND url LIKE 'https://%.trycloudflare.com';
UPDATE workspace_apps SET url = '$PREFECT_URL'  WHERE slug = 'prefect'  AND url LIKE 'https://%.trycloudflare.com';
UPDATE workspace_apps SET url = '$REDPANDA_URL' WHERE slug = 'redpanda' AND url LIKE 'https://%.trycloudflare.com';
SQL
  echo "[url-reporter] Coder DB patched: prefect=$PREFECT_URL mlflow=$MLFLOW_URL redpanda=$REDPANDA_URL"
}

patch_prefect_env() {
  NEW_URL="$1"
  [ -z "$NEW_URL" ] && return

  EXPECTED="${NEW_URL}/api"
  CURRENT=$(grep 'PREFECT_UI_API_URL' /project.env 2>/dev/null | cut -d= -f2-)
  [ "$CURRENT" = "$EXPECTED" ] && return

  { grep -v 'PREFECT_UI_API_URL' /project.env; echo "PREFECT_UI_API_URL=${EXPECTED}"; } > /tmp/env.tmp
  cat /tmp/env.tmp > /project.env
  echo "[url-reporter] PREFECT_UI_API_URL updated → $EXPECTED"
  docker restart olist-prefect-server > /dev/null 2>&1
  echo "[url-reporter] prefect-server restarted"
}

patch_mlflow_env() {
  NEW_URL="$1"
  [ -z "$NEW_URL" ] && return

  CURRENT=$(grep 'MLFLOW_TRACKING_URI' /project.env 2>/dev/null | cut -d= -f2-)
  [ "$CURRENT" = "$NEW_URL" ] && return

  { grep -v 'MLFLOW_TRACKING_URI' /project.env; echo "MLFLOW_TRACKING_URI=${NEW_URL}"; } > /tmp/env.tmp
  cat /tmp/env.tmp > /project.env
  echo "[url-reporter] MLFLOW_TRACKING_URI updated → $NEW_URL"
}

update_urls() {
  PREFECT_URL=$(get_url olist-cf-prefect)
  MLFLOW_URL=$(get_url olist-cf-mlflow)
  REDPANDA_URL=$(get_url olist-cf-redpanda)
  CODER_URL=$(get_url olist-cf-coder)
  STREAMLIT_URL=$(get_url olist-cf-streamlit)

  {
    printf "=== Cloudflare Tunnel URLs ===\n"
    printf "Updated: %s\n\n" "$(date)"
    printf "%-20s %s\n" "prefect:"  "${PREFECT_URL:-not running}"
    printf "%-20s %s\n" "mlflow:"   "${MLFLOW_URL:-not running}"
    printf "%-20s %s\n" "redpanda:" "${REDPANDA_URL:-not running}"
    printf "%-20s %s\n" "coder:"    "${CODER_URL:-not running}"
    printf "%-20s %s\n" "streamlit:""${STREAMLIT_URL:-not running}"
  } | tee "$OUT"
  echo ""

  patch_coder_db "$PREFECT_URL" "$MLFLOW_URL" "$REDPANDA_URL"
  patch_prefect_env "$PREFECT_URL"
  patch_mlflow_env "$MLFLOW_URL"
}

sleep 8
update_urls

docker events \
  --filter 'event=start' \
  --filter 'name=olist-cf-' \
  --format '{{.Actor.Attributes.name}}' | while read -r container; do
    echo "[url-reporter] $container restarted, refreshing URLs in 8s..."
    sleep 8
    update_urls
done
