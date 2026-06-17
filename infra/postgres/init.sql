-- ============================================================
-- PostgreSQL initialization for Olist Data Lakehouse
-- Runs automatically on first container start.
-- Creates dedicated databases for each service.
-- ============================================================

-- Iceberg REST catalog metadata
CREATE DATABASE iceberg;
GRANT ALL PRIVILEGES ON DATABASE iceberg TO olist;

-- Prefect workflow metadata
CREATE DATABASE prefect;
GRANT ALL PRIVILEGES ON DATABASE prefect TO olist;

-- MLflow experiment tracking
CREATE DATABASE mlflow;
GRANT ALL PRIVILEGES ON DATABASE mlflow TO olist;

-- Coder cloud IDE metadata (workspaces, users, templates)
CREATE DATABASE coder;
GRANT ALL PRIVILEGES ON DATABASE coder TO olist;

-- Marquez data lineage (OpenLineage reference implementation)
CREATE DATABASE marquez;
GRANT ALL PRIVILEGES ON DATABASE marquez TO olist;

-- External sample data (used by Trino postgres catalog for federation demos)
CREATE DATABASE olist_meta;
GRANT ALL PRIVILEGES ON DATABASE olist_meta TO olist;

\connect olist_meta

-- Example external table: marketing campaigns
-- Trino can JOIN this with Iceberg Gold tables via query federation
CREATE TABLE IF NOT EXISTS marketing_campaigns (
    campaign_id     VARCHAR(64) PRIMARY KEY,
    seller_id       VARCHAR(64),
    campaign_name   VARCHAR(255),
    channel         VARCHAR(64),
    cost_per_click  NUMERIC(10, 2),
    start_date      DATE,
    end_date        DATE
);
