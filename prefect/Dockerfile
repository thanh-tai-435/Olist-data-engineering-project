FROM prefecthq/prefect:3-latest

# System deps for dbt-duckdb and confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev git curl openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app

COPY requirements-worker.txt .
RUN pip install --no-cache-dir -r requirements-worker.txt

# Copy flow code (ingestion/ and dbt/ are mounted via volumes in compose)
COPY prefect/flows ./flows
COPY quality ./quality
