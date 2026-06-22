FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.35.0 \
    anthropic==0.49.0 \
    duckdb==0.10.0 \
    pyiceberg[s3fs,pandas]==0.6.0 \
    plotly==5.20.0 \
    mlflow==2.13.0 \
    pandas==2.2.0 \
    requests==2.31.0 \
    boto3==1.34.0 \
    confluent-kafka==2.3.0

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "agentic_bi.py", "--server.port=8501", "--server.address=0.0.0.0"]
