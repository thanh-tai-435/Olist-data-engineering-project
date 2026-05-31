# ============================================================
# Dockerfile - Agentic BI production container
# Base: python:3.10-slim (minimal footprint, stable LTS)
# PYTHONUNBUFFERED ensures logs stream to VSCode terminal in
# real time without buffering delays.
# ============================================================

FROM python:3.10-slim

# Sync stdout/stderr immediately to terminal (required for VSCode SSH log streaming)
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install system dependencies first (cached layer unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY . .

# Create data directory for DuckDB persistence
RUN mkdir -p data

# Streamlit default port - configure Port Forwarding in VSCode Remote SSH
EXPOSE 8501

# Disable Streamlit's browser auto-open (headless server mode)
CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
