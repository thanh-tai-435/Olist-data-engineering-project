"""
Gold Layer Transform – Silver Iceberg → Gold Iceberg on R2.

Tables tạo ra trong namespace olist.gold:
  fct_orders      – order fact, partitioned by month
  fct_funnel      – marketing funnel, partitioned by year
  dim_sellers     – seller dimension với aggregated metrics
  dim_customers   – customer dimension với CLV metrics
"""
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SPARK_MASTER  = os.environ.get("SPARK_MASTER", "local[2]")
ICEBERG_URI   = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
S3_ENDPOINT   = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
BUCKET        = "retail-data-lake"
JARS_DIR      = os.environ.get("SPARK_JARS_DIR", "/app/spark-jars")

_JARS = ",".join([
    f"{JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.5.0.jar",
    f"{JARS_DIR}/iceberg-aws-bundle-1.5.0.jar",   # AWS SDK v2 cho S3FileIO
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
])


def get_spark(app_name: str = "olist-gold-transform") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.jars", _JARS)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.olist",
                "org.apache.iceberg.spark.SparkCatalog")
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
        .getOrCreate()
    )


def _write_partitioned(df, table: str, *partition_cols) -> None:
    writer = df.writeTo(table)
    if partition_cols:
        writer = writer.partitionedBy(*partition_cols)
    writer.createOrReplace()
    log.info("    ✓ %s  (%d rows)", table, df.count())


# ── Gold transforms ───────────────────────────────────────────────────────────

def transform_fct_orders(spark: SparkSession) -> None:
    """
    Fact table chính: mỗi row là 1 order đã delivered.
    Partitioned by months("purchased_at") để query theo tháng nhanh.
    """
    log.info("  → fct_orders")
    df = spark.table("olist.silver.int_orders_enriched")

    result = (
        df
        .filter(F.col("order_id").isNotNull())
        .select(
            "order_id",
            "customer_id",
            "customer_unique_id",
            "order_status",
            F.to_date("purchased_at").alias("order_date"),
            F.date_format("purchased_at", "yyyy-MM").alias("order_month"),
            "purchased_at",
            "approved_at",
            "shipped_at",
            "delivered_at",
            "actual_delivery_days",
            "estimated_delivery_days",
            "delivery_delay_days",
            "customer_state",
            "customer_city",
            F.coalesce(F.col("order_revenue"),  F.lit(0.0)).alias("order_revenue"),
            F.coalesce(F.col("order_freight"),  F.lit(0.0)).alias("order_freight"),
            F.coalesce(F.col("item_count"),     F.lit(0)).alias("item_count"),
            F.coalesce(F.col("payment_value"),  F.lit(0.0)).alias("payment_value"),
            F.coalesce(F.col("payment_type"),   F.lit("unknown")).alias("payment_type"),
            "max_installments",
            "review_score",
            "total_weight_g",
            # Delivery SLA label
            F.when(F.col("delivery_delay_days") > 0,  "late")
             .when(F.col("delivery_delay_days") < 0,  "early")
             .when(F.col("delivery_delay_days").isNull(), "unknown")
             .otherwise("on_time")
             .alias("delivery_status"),
        )
    )
    _write_partitioned(result, "olist.gold.fct_orders", F.months("purchased_at"))


def transform_fct_funnel(spark: SparkSession) -> None:
    """
    Marketing funnel: từ MQL (lead) → deal.
    Partitioned by years("first_contact_date").
    """
    log.info("  → fct_funnel")
    leads = spark.table("olist.silver.stg_marketing_leads")
    deals = spark.table("olist.silver.stg_marketing_deals")

    result = (
        leads
        .join(
            deals.select(
                "mql_id", "seller_id", "won_date",
                "business_segment", "lead_type", "business_type",
                "declared_monthly_revenue",
            ),
            "mql_id", "left",
        )
        .select(
            "mql_id",
            "first_contact_date",
            "origin",
            "landing_page_id",
            "seller_id",
            "won_date",
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
    _write_partitioned(result, "olist.gold.fct_funnel", F.years("first_contact_date"))


def transform_dim_sellers(spark: SparkSession) -> None:
    """
    Seller dimension: profile + aggregated performance metrics.
    Full refresh (không partition – bảng nhỏ).
    """
    log.info("  → dim_sellers")
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
            "seller_id",
            "seller_city",
            "seller_state",
            F.coalesce(F.col("total_orders"),    F.lit(0)).alias("total_orders"),
            F.coalesce(F.col("total_revenue"),   F.lit(0.0)).alias("total_revenue"),
            "avg_review_score",
            "first_sale_date",
            "last_sale_date",
            F.coalesce(F.col("delivered_orders"), F.lit(0)).alias("delivered_orders"),
        )
    )
    _write_partitioned(result, "olist.gold.dim_sellers")


def transform_dim_customers(spark: SparkSession) -> None:
    """
    Customer dimension: profile + CLV / churn metrics.
    Dedup by customer_unique_id (1 unique customer = nhiều customer_id).
    """
    log.info("  → dim_customers")
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

    # Dedup customers: 1 row per customer_unique_id (giữ customer_id mới nhất)
    w = Window.partitionBy("customer_unique_id").orderBy(F.col("customer_id").desc())
    customers_dedup = (
        customers
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    result = (
        customers_dedup
        .join(order_metrics, "customer_unique_id", "left")
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state",
            F.coalesce(F.col("total_orders"),         F.lit(0)).alias("total_orders"),
            F.coalesce(F.col("total_spend"),          F.lit(0.0)).alias("total_spend"),
            F.round("avg_order_value", 2).alias("avg_order_value"),
            "first_order_date",
            "last_order_date",
            "days_since_last_order",
            # Churn feature: chưa mua trong 90 ngày
            F.when(F.col("days_since_last_order") > 90, F.lit(1))
             .otherwise(F.lit(0))
             .alias("is_churned"),
            # Repeat customer feature cho ML
            F.when(F.col("total_orders") > 1, F.lit(1))
             .otherwise(F.lit(0))
             .alias("is_repeat_customer"),
        )
    )
    _write_partitioned(result, "olist.gold.dim_customers")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_gold(spark: SparkSession) -> None:
    log.info("=== GOLD TRANSFORM START ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.gold")

    transform_fct_orders(spark)
    transform_fct_funnel(spark)
    transform_dim_sellers(spark)
    transform_dim_customers(spark)

    log.info("=== GOLD TRANSFORM COMPLETE ===")


if __name__ == "__main__":
    spark = get_spark()
    try:
        run_gold(spark)
    finally:
        spark.stop()
