"""
Flow: Full Pipeline
Orchestrator chạy toàn bộ pipeline: Bronze → Silver → Gold.
Đây là flow chính để schedule hàng ngày.

Silver + Gold dùng PySpark + Iceberg (spark_transforms.py).
SPARK_MASTER env var điều khiển chế độ:
  local[2]                  – mặc định, không cần Spark cluster
  spark://spark-master:7077 – dùng Spark cluster (--profile spark)
"""
from prefect import flow, get_run_logger
from bronze_ingestion import bronze_ingestion_flow
from spark_transforms import silver_transform_flow, gold_transform_flow


@flow(
    name="full-pipeline",
    description="End-to-end: Bronze ingestion → Silver dbt → Gold dbt.",
    log_prints=True,
)
def full_pipeline_flow(
    full_refresh: bool = False,
    skip_bronze: bool = False,
):
    log = get_run_logger()
    log.info("=== FULL PIPELINE START ===")

    if not skip_bronze:
        log.info("Step 1/3: Bronze ingestion")
        bronze_ingestion_flow()
    else:
        log.info("Step 1/3: Bronze ingestion SKIPPED")

    log.info("Step 2/3: Silver transform")
    silver_transform_flow(full_refresh=full_refresh)

    log.info("Step 3/3: Gold transform")
    gold_transform_flow(full_refresh=full_refresh)

    log.info("=== FULL PIPELINE COMPLETE ===")


if __name__ == "__main__":
    full_pipeline_flow()
