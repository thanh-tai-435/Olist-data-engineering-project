"""
Prefect flows: Silver + Gold transforms via PySpark.

Mỗi flow import PySpark job từ spark/jobs/ và chạy in-process.
SPARK_MASTER (env var) điều khiển mode:
  local[2]                  – mặc định, không cần cluster
  spark://spark-master:7077 – dùng Spark cluster (cần --profile spark)
"""
import logging
import os
import sys
from pathlib import Path

from prefect.artifacts import create_markdown_artifact

from prefect import flow, get_run_logger, task

# spark/jobs/ → /app/spark/jobs trong Docker, <repo>/spark/jobs khi dev local
for _candidate in [
    Path(__file__).parents[2] / "spark" / "jobs",   # /app/spark/jobs (Docker)
    Path(__file__).parents[3] / "spark" / "jobs",   # <repo>/spark/jobs (local dev)
]:
    if _candidate.is_dir() and str(_candidate) not in sys.path:
        sys.path.insert(0, str(_candidate))


# ── Tasks ─────────────────────────────────────────────────────────────────────

@task(
    name="pyspark-silver",
    retries=1,
    retry_delay_seconds=30,
    description="Bronze → Silver: cleaning, typing, dedup, intermediate join.",
)
def run_silver_task() -> dict:
    log = get_run_logger()
    master = os.environ.get("SPARK_MASTER", "local[2]")
    log.info(f"Silver | master={master}")

    from silver_transform import get_spark, run_silver  # noqa: PLC0415

    spark = get_spark("olist-silver-transform")
    try:
        run_silver(spark)
        tables = [t.name for t in spark.catalog.listTables("olist.silver")]
        return {"status": "ok", "tables": tables, "master": master}
    finally:
        spark.stop()


@task(
    name="pyspark-gold",
    retries=1,
    retry_delay_seconds=30,
    description="Silver → Gold: fct_orders, fct_funnel, dim_sellers, dim_customers.",
)
def run_gold_task() -> dict:
    log = get_run_logger()
    master = os.environ.get("SPARK_MASTER", "local[2]")
    log.info(f"Gold | master={master}")

    from gold_transform import get_spark, run_gold  # noqa: PLC0415

    spark = get_spark("olist-gold-transform")
    try:
        run_gold(spark)
        tables = [t.name for t in spark.catalog.listTables("olist.gold")]
        return {"status": "ok", "tables": tables, "master": master}
    finally:
        spark.stop()


# ── Flows ─────────────────────────────────────────────────────────────────────

@flow(
    name="silver-transform",
    description="Bronze → Silver via PySpark + Iceberg REST catalog.",
    log_prints=True,
)
def silver_transform_flow() -> dict:
    result = run_silver_task()
    create_markdown_artifact(
        key="silver-spark-result",
        markdown=(
            f"## Silver Transform\n"
            f"- **master**: `{result['master']}`\n"
            f"- **tables**: {', '.join(f'`{t}`' for t in result.get('tables', []))}\n"
        ),
        description="PySpark Silver layer result",
    )
    return result


@flow(
    name="gold-transform",
    description="Silver → Gold via PySpark + Iceberg REST catalog (partitioned marts).",
    log_prints=True,
)
def gold_transform_flow() -> dict:
    result = run_gold_task()
    create_markdown_artifact(
        key="gold-spark-result",
        markdown=(
            f"## Gold Transform\n"
            f"- **master**: `{result['master']}`\n"
            f"- **tables**: {', '.join(f'`{t}`' for t in result.get('tables', []))}\n"
        ),
        description="PySpark Gold layer result",
    )
    return result


if __name__ == "__main__":
    silver_transform_flow()
    gold_transform_flow()
