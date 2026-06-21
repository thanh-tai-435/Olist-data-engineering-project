#!/usr/bin/env bash
# Download OpenLineage Spark JAR into spark/jars/
# Run once before docker compose up --profile lineage
set -euo pipefail

JARS_DIR="$(cd "$(dirname "$0")/../spark/jars" && pwd)"
OL_VERSION="1.50.0"
JAR="openlineage-spark_2.12-${OL_VERSION}.jar"
URL="https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/${OL_VERSION}/${JAR}"

if [[ -f "${JARS_DIR}/${JAR}" ]]; then
    echo "Already exists: ${JARS_DIR}/${JAR}"
    exit 0
fi

echo "Downloading OpenLineage Spark JAR v${OL_VERSION}..."
curl -fL --progress-bar -o "${JARS_DIR}/${JAR}" "${URL}"
echo "Saved to ${JARS_DIR}/${JAR}"
