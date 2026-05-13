FROM prefecthq/prefect:3-latest

# System deps for dbt-duckdb and confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev git curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-worker.txt .
RUN pip install --no-cache-dir -r requirements-worker.txt

# Copy flow code (ingestion/ and dbt/ are mounted via volumes in compose)
COPY prefect/flows ./flows
