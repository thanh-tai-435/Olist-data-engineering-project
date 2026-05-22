FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget git vim build-essential \
    ca-certificates gnupg \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-duckdb dbt-spark \
    "pyiceberg[s3,pyarrow]" \
    prefect pandas pyarrow duckdb \
    confluent-kafka mlflow \
    streamlit anthropic \
    boto3 s3fs fsspec

RUN curl -fsSL https://code-server.dev/install.sh | sh

EXPOSE 13337

CMD ["sleep", "infinity"]
