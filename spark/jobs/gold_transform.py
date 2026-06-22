"""
Gold Layer Transform – incremental partition overwrite + Iceberg maintenance.

fct_orders   : chỉ overwrite các tháng có đơn hàng mới/cập nhật trong run này
               (xác định qua _silver_updated_at > gold watermark)
fct_funnel   : full refresh (8K rows, lead/deal state thay đổi cả row)
dim_sellers  : full refresh (3K rows, aggregation over all history)
dim_customers: full refresh (96K rows, CLV aggregation over all history)

Maintenance (cuối mỗi run):
  - expire_snapshots: xóa snapshots cũ hơn 7 ngày (giữ tối đa 3 gần nhất)
  - rewrite_data_files: compact small files → query nhanh hơn
"""
import logging
import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(__file__))
import watermark as wm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SPARK_MASTER    = os.environ.get("SPARK_MASTER",     "local[2]")
ICEBERG_URI     = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
S3_ENDPOINT     = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY   = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY   = os.environ["S3_SECRET_KEY"]
BUCKET          = "retail-data-lake"
JARS_DIR        = os.environ.get("SPARK_JARS_DIR",   "/app/spark-jars")
OPENLINEAGE_URL = os.environ.get("OPENLINEAGE_URL",  "http://marquez:5002")

_JARS = ",".join([
    f"{JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.5.0.jar",
    f"{JARS_DIR}/iceberg-aws-bundle-1.5.0.jar",
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
    f"{JARS_DIR}/openlineage-spark_2.12-1.50.0.jar",
])

# Tables that get maintenance after every Gold run
_MAINTENANCE_TABLES = [
    "olist.silver.stg_orders",
    "olist.silver.stg_order_items",
    "olist.silver.stg_order_reviews",
    "olist.silver.int_orders_enriched",
    "olist.gold.fct_orders",
    "olist.gold.fct_funnel",
    "olist.gold.dim_sellers",
    "olist.gold.dim_customers",
]


# ── SparkSession ──────────────────────────────────────────────────────────────

def get_spark(app_name: str = "olist-gold-transform") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.jars", _JARS)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.olist",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.olist.type",      "rest")
        .config("spark.sql.catalog.olist.uri",       ICEBERG_URI)
        .config("spark.sql.catalog.olist.warehouse", f"s3://{BUCKET}")
        .config("spark.sql.catalog.olist.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.olist.s3.endpoint",          S3_ENDPOINT)
        .config("spark.sql.catalog.olist.s3.access-key-id",     S3_ACCESS_KEY)
        .config("spark.sql.catalog.olist.s3.secret-access-key", S3_SECRET_KEY)
        .config("spark.sql.catalog.olist.s3.path-style-access", "false")
        .config("spark.sql.catalog.olist.client.region",        "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint",      S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",    S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",    S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism",    "4")
        .config("spark.extraListeners",
                "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url",  OPENLINEAGE_URL)
        .config("spark.openlineage.namespace",      "olist")
        .getOrCreate()
    )


# ── Gold transforms ───────────────────────────────────────────────────────────

def transform_fct_orders(spark: SparkSession, last_gold_wm: str) -> None:
    """
    Incremental: tìm các tháng có _silver_updated_at > last_gold_wm,
    recompute toàn bộ orders của tháng đó, overwrite partition.

    overwritePartitions() là atomic per-partition: các tháng cũ không bị đụng.
    """
    log.info("  → fct_orders  (gold wm: %s)", last_gold_wm)

    enriched = spark.table("olist.silver.int_orders_enriched")

    # Tháng nào có Silver data mới kể từ lần Gold chạy cuối?
    affected_rows = (
        enriched
        .filter(F.col("_silver_updated_at") > F.lit(last_gold_wm).cast("timestamp"))
        .select(F.date_format("purchased_at", "yyyy-MM").alias("month"))
        .filter(F.col("month").isNotNull())
        .distinct()
        .collect()
    )
    months = [r["month"] for r in affected_rows]

    if not months:
        log.info("    → fct_orders: no new months, skipping")
        return

    log.info("    affected months: %s", months)

    # Recompute ALL orders for affected months (không dùng incremental filter
    # để đảm bảo aggregation chính xác khi có late-arriving data)
    result = (
        enriched
        .filter(F.date_format("purchased_at", "yyyy-MM").isin(months))
        .filter(F.col("order_id").isNotNull())
        .select(
            "order_id", "customer_id", "customer_unique_id", "order_status",
            F.to_date("purchased_at").alias("order_date"),
            F.date_format("purchased_at", "yyyy-MM").alias("order_month"),
            "purchased_at", "approved_at", "shipped_at", "delivered_at",
            "actual_delivery_days", "estimated_delivery_days", "delivery_delay_days",
            "customer_state", "customer_city",
            F.coalesce(F.col("order_revenue"),  F.lit(0.0)).alias("order_revenue"),
            F.coalesce(F.col("order_freight"),  F.lit(0.0)).alias("order_freight"),
            F.coalesce(F.col("item_count"),     F.lit(0)).alias("item_count"),
            F.coalesce(F.col("payment_value"),  F.lit(0.0)).alias("payment_value"),
            F.coalesce(F.col("payment_type"),   F.lit("unknown")).alias("payment_type"),
            F.coalesce(F.col("max_installments"), F.lit(1)).alias("max_installments"),
            "review_score",
            F.coalesce(F.col("total_weight_g"), F.lit(0.0)).alias("total_weight_g"),
            F.when(F.col("delivery_delay_days") > 0,    "late")
             .when(F.col("delivery_delay_days") < 0,    "early")
             .when(F.col("delivery_delay_days").isNull(), "unknown")
             .otherwise("on_time")
             .alias("delivery_status"),
        )
    )

    result.writeTo("olist.gold.fct_orders") \
          .partitionedBy(F.months("purchased_at")) \
          .overwritePartitions()
    log.info("    ✓ fct_orders  %d months overwritten", len(months))


def transform_fct_funnel(spark: SparkSession) -> None:
    """
    Full refresh: 8K rows. Lead-to-deal state thay đổi cả row (is_converted,
    won_date), không phù hợp với partition overwrite → createOrReplace.
    """
    log.info("  → fct_funnel  (full refresh)")
    leads = spark.table("olist.silver.stg_marketing_leads")
    deals = spark.table("olist.silver.stg_marketing_deals")

    result = (
        leads
        .join(
            deals.select("mql_id", "seller_id", "won_date",
                         "business_segment", "lead_type", "business_type",
                         "declared_monthly_revenue"),
            "mql_id", "left",
        )
        .select(
            "mql_id", "first_contact_date", "origin", "landing_page_id",
            "seller_id", "won_date",
            F.coalesce(F.col("business_segment"), F.lit("unknown")).alias("business_segment"),
            F.coalesce(F.col("lead_type"),         F.lit("unknown")).alias("lead_type"),
            F.coalesce(F.col("business_type"),     F.lit("unknown")).alias("business_type"),
            "declared_monthly_revenue",
            F.datediff(
                F.col("won_date").cast("date"),
                F.col("first_contact_date").cast("date"),
            ).alias("days_to_close"),
            F.when(F.col("seller_id").isNotNull(), F.lit(1))
             .otherwise(F.lit(0))
             .alias("is_converted"),
        )
    )

    result.writeTo("olist.gold.fct_funnel") \
          .partitionedBy(F.years("first_contact_date")) \
          .createOrReplace()
    log.info("    ✓ fct_funnel  (%d rows)", result.count())


def transform_dim_sellers(spark: SparkSession) -> None:
    """Full refresh: seller metrics aggregate over all order history."""
    log.info("  → dim_sellers  (full refresh)")
    sellers = spark.table("olist.silver.stg_sellers")
    items   = spark.table("olist.silver.stg_order_items")
    orders  = spark.table("olist.silver.stg_orders").select(
        "order_id", "purchased_at", "order_status"
    )
    reviews = spark.table("olist.silver.stg_order_reviews").select(
        "order_id", "review_score"
    )

    seller_metrics = (
        items
        .join(orders,  "order_id", "inner")
        .join(reviews, "order_id", "left")
        .groupBy("seller_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum(F.col("price").cast("double")).alias("total_revenue"),
            F.round(F.avg(F.col("review_score").cast("double")), 2)
             .alias("avg_review_score"),
            F.min("purchased_at").alias("first_sale_date"),
            F.max("purchased_at").alias("last_sale_date"),
            F.countDistinct(
                F.when(F.col("order_status") == "delivered", F.col("order_id"))
            ).alias("delivered_orders"),
        )
    )

    result = (
        sellers
        .join(seller_metrics, "seller_id", "left")
        .select(
            "seller_id", "seller_city", "seller_state",
            F.coalesce(F.col("total_orders"),    F.lit(0)).alias("total_orders"),
            F.coalesce(F.col("total_revenue"),   F.lit(0.0)).alias("total_revenue"),
            "avg_review_score", "first_sale_date", "last_sale_date",
            F.coalesce(F.col("delivered_orders"), F.lit(0)).alias("delivered_orders"),
        )
    )

    result.writeTo("olist.gold.dim_sellers").createOrReplace()
    log.info("    ✓ dim_sellers  (%d rows)", result.count())


def transform_dim_customers(spark: SparkSession) -> None:
    """Full refresh: CLV metrics require all-time aggregation."""
    log.info("  → dim_customers  (full refresh)")
    customers = spark.table("olist.silver.stg_customers")
    orders    = spark.table("olist.silver.stg_orders")
    payments  = spark.table("olist.silver.stg_order_payments").select(
        "order_id", "payment_value"
    )

    order_metrics = (
        orders
        .join(customers.select("customer_id", "customer_unique_id"), "customer_id", "left")
        .join(payments, "order_id", "left")
        .groupBy("customer_unique_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum(F.col("payment_value").cast("double")).alias("total_spend"),
            F.avg(F.col("payment_value").cast("double")).alias("avg_order_value"),
            F.min("purchased_at").alias("first_order_date"),
            F.max("purchased_at").alias("last_order_date"),
            F.datediff(
                F.current_date(),
                F.max(F.to_date("purchased_at")),
            ).alias("days_since_last_order"),
        )
    )

    # Dedup: 1 row per customer_unique_id (pick most recent order's customer_id)
    w = Window.partitionBy("customer_unique_id").orderBy(
        F.col("last_order_date").desc()
    )
    customers_dedup = (
        customers
        .join(
            order_metrics.select("customer_unique_id", "last_order_date"),
            "customer_unique_id", "left",
        )
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "last_order_date")
    )

    result = (
        customers_dedup
        .join(order_metrics, "customer_unique_id", "left")
        .select(
            "customer_id", "customer_unique_id", "customer_city", "customer_state",
            F.coalesce(F.col("total_orders"),  F.lit(0)).alias("total_orders"),
            F.coalesce(F.col("total_spend"),   F.lit(0.0)).alias("total_spend"),
            F.round("avg_order_value", 2).alias("avg_order_value"),
            "first_order_date", "last_order_date", "days_since_last_order",
            F.when(F.col("days_since_last_order") > 90, F.lit(1))
             .otherwise(F.lit(0))
             .alias("is_churned"),
            F.when(F.col("total_orders") > 1, F.lit(1))
             .otherwise(F.lit(0))
             .alias("is_repeat_customer"),
        )
    )

    result.writeTo("olist.gold.dim_customers").createOrReplace()
    log.info("    ✓ dim_customers  (%d rows)", result.count())


# ── Iceberg maintenance ───────────────────────────────────────────────────────

def run_maintenance(spark: SparkSession) -> None:
    """
    Chạy sau mỗi Gold run:
    1. expire_snapshots  – xóa snapshots cũ hơn 7 ngày (giữ 3 gần nhất)
       → giảm metadata size, tăng tốc time-travel query
    2. rewrite_data_files – compact nhiều file nhỏ thành ít file lớn
       → giảm số files R2 cần đọc, tăng tốc full-table scan
    """
    log.info("=== ICEBERG MAINTENANCE ===")
    cutoff = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")

    for table in _MAINTENANCE_TABLES:
        # expire_snapshots
        try:
            result = spark.sql(f"""
                CALL olist.system.expire_snapshots(
                    table        => '{table}',
                    older_than   => TIMESTAMP '{cutoff}',
                    retain_last  => 3
                )
            """)
            row = result.collect()[0]
            log.info(
                "    ✓ expire_snapshots  %s  "
                "(deleted: %d snapshots, %d data files, %d manifest files)",
                table,
                row["deleted_snapshots_count"],
                row["deleted_data_files_count"],
                row["deleted_manifest_files_count"],
            )
        except Exception as e:
            log.warning("    ! expire_snapshots failed  %s: %s", table, e)

        # rewrite_data_files
        try:
            result = spark.sql(f"""
                CALL olist.system.rewrite_data_files(table => '{table}')
            """)
            row = result.collect()[0]
            log.info(
                "    ✓ rewrite_data_files  %s  "
                "(rewritten: %d files → %d files)",
                table,
                row["rewritten_files_count"],
                row["added_files_count"],
            )
        except Exception as e:
            log.warning("    ! rewrite_data_files failed  %s: %s", table, e)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_gold(spark: SparkSession, skip_maintenance: bool = False) -> None:
    log.info("=== GOLD TRANSFORM START ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.gold")

    last_gold_wm = wm.get("gold.run")
    log.info("gold watermark = %s", last_gold_wm)

    transform_fct_orders(spark, last_gold_wm)
    transform_fct_funnel(spark)
    transform_dim_sellers(spark)
    transform_dim_customers(spark)

    wm.save("gold.run", wm.now_utc())
    log.info("gold watermark updated")

    if not skip_maintenance:
        run_maintenance(spark)

    log.info("=== GOLD TRANSFORM COMPLETE ===")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-maintenance", action="store_true",
                        help="Skip Iceberg expire_snapshots / rewrite_data_files")
    args = parser.parse_args()

    spark = get_spark()
    try:
        run_gold(spark, skip_maintenance=args.skip_maintenance)
    finally:
        spark.stop()
