"""
Silver Layer Transform – Bronze Iceberg → Silver Iceberg on R2.

Chạy theo 2 chế độ:
  local[2]  (mặc định) – prefect-worker, không cần Spark cluster
  cluster   – set SPARK_MASTER=spark://spark-master:7077 + profile spark

Tables tạo ra trong namespace olist.silver:
  stg_orders, stg_order_items, stg_order_payments, stg_order_reviews,
  stg_products, stg_sellers, stg_customers,
  stg_marketing_leads, stg_marketing_deals,
  int_orders_enriched (intermediate join, dùng bởi Gold layer)
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

# ── Config từ environment variables ──────────────────────────────────────────
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


# ── SparkSession factory ──────────────────────────────────────────────────────

def get_spark(app_name: str = "olist-silver-transform") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.jars", _JARS)
        # Iceberg SQL extensions
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Iceberg REST catalog – tên catalog: "olist"
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
        # Hadoop S3A (cho direct file reads nếu cần)
        .config("spark.hadoop.fs.s3a.endpoint",      S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",    S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",    S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Tune cho dataset nhỏ (~200 MB)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism",    "4")
        .getOrCreate()
    )


# ── Helper ────────────────────────────────────────────────────────────────────

def _write(df, table: str) -> None:
    """createOrReplace: idempotent, tạo bảng lần đầu hoặc refresh toàn bộ."""
    df.writeTo(table).createOrReplace()
    log.info("    ✓ %s  (%d rows)", table, df.count())


# ── Staging transforms ────────────────────────────────────────────────────────

def transform_stg_orders(spark: SparkSession) -> None:
    log.info("  → stg_orders")
    df = spark.table("olist.bronze.ecommerce_orders")

    result = (
        df
        .select(
            "order_id",
            "customer_id",
            "order_status",
            F.col("order_purchase_timestamp").cast("timestamp").alias("purchased_at"),
            F.col("order_approved_at").cast("timestamp").alias("approved_at"),
            F.col("order_delivered_carrier_date").cast("timestamp").alias("shipped_at"),
            F.col("order_delivered_customer_date").cast("timestamp").alias("delivered_at"),
            F.col("order_estimated_delivery_date").cast("timestamp").alias("estimated_delivery_at"),
            F.datediff(
                F.col("order_delivered_customer_date").cast("timestamp"),
                F.col("order_purchase_timestamp").cast("timestamp"),
            ).alias("actual_delivery_days"),
            F.datediff(
                F.col("order_estimated_delivery_date").cast("timestamp"),
                F.col("order_purchase_timestamp").cast("timestamp"),
            ).alias("estimated_delivery_days"),
            F.col("_ingested_at"),
        )
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
    )
    _write(result, "olist.silver.stg_orders")


def transform_stg_order_items(spark: SparkSession) -> None:
    log.info("  → stg_order_items")
    df = spark.table("olist.bronze.ecommerce_order_items")

    result = (
        df
        .select(
            "order_id",
            F.col("order_item_id").cast("int"),
            "product_id",
            "seller_id",
            F.col("shipping_limit_date").cast("timestamp"),
            F.col("price").cast("double"),
            F.col("freight_value").cast("double"),
        )
        .filter(F.col("order_id").isNotNull())
    )
    _write(result, "olist.silver.stg_order_items")


def transform_stg_order_payments(spark: SparkSession) -> None:
    """Aggregate: 1 row per order (total value + dominant payment type)."""
    log.info("  → stg_order_payments")
    df = spark.table("olist.bronze.ecommerce_order_payments")

    result = (
        df
        .filter(F.col("order_id").isNotNull())
        .groupBy("order_id")
        .agg(
            F.sum(F.col("payment_value").cast("double")).alias("payment_value"),
            # payment type với payment_sequential = 1 là chính
            F.first(
                F.when(F.col("payment_sequential").cast("int") == 1,
                       F.col("payment_type"))
            ).alias("payment_type"),
            F.max(F.col("payment_installments").cast("int")).alias("max_installments"),
            F.count("*").alias("payment_count"),
        )
    )
    _write(result, "olist.silver.stg_order_payments")


def transform_stg_order_reviews(spark: SparkSession) -> None:
    """Dedup: 1 review per order (lấy review mới nhất)."""
    log.info("  → stg_order_reviews")
    df = spark.table("olist.bronze.ecommerce_order_reviews")

    w = Window.partitionBy("order_id").orderBy(
        F.col("review_answer_timestamp").cast("timestamp").desc()
    )

    result = (
        df
        .filter(F.col("order_id").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "order_id",
            "review_id",
            F.col("review_score").cast("int"),
            "review_comment_title",
            "review_comment_message",
            F.col("review_creation_date").cast("timestamp"),
            F.col("review_answer_timestamp").cast("timestamp"),
        )
    )
    _write(result, "olist.silver.stg_order_reviews")


def transform_stg_products(spark: SparkSession) -> None:
    log.info("  → stg_products")
    df = spark.table("olist.bronze.ecommerce_products")

    result = (
        df
        .filter(F.col("product_id").isNotNull())
        .select(
            "product_id",
            F.coalesce(F.col("product_category_name"), F.lit("unknown"))
             .alias("product_category_name"),
            F.col("product_weight_g").cast("double"),
            F.col("product_length_cm").cast("double"),
            F.col("product_height_cm").cast("double"),
            F.col("product_width_cm").cast("double"),
            F.col("product_photos_qty").cast("int"),
            (
                F.col("product_length_cm").cast("double")
                * F.col("product_height_cm").cast("double")
                * F.col("product_width_cm").cast("double")
            ).alias("product_volume_cm3"),
        )
        .dropDuplicates(["product_id"])
    )
    _write(result, "olist.silver.stg_products")


def transform_stg_sellers(spark: SparkSession) -> None:
    log.info("  → stg_sellers")
    df = spark.table("olist.bronze.ecommerce_sellers")

    result = (
        df
        .filter(F.col("seller_id").isNotNull())
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            F.trim(F.lower(F.col("seller_city"))).alias("seller_city"),
            F.upper(F.col("seller_state")).alias("seller_state"),
        )
        .dropDuplicates(["seller_id"])
    )
    _write(result, "olist.silver.stg_sellers")


def transform_stg_customers(spark: SparkSession) -> None:
    log.info("  → stg_customers")
    df = spark.table("olist.bronze.ecommerce_customers")

    result = (
        df
        .filter(F.col("customer_id").isNotNull())
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            F.trim(F.lower(F.col("customer_city"))).alias("customer_city"),
            F.upper(F.col("customer_state")).alias("customer_state"),
        )
        .dropDuplicates(["customer_id"])
    )
    _write(result, "olist.silver.stg_customers")


def transform_stg_marketing_leads(spark: SparkSession) -> None:
    log.info("  → stg_marketing_leads")
    df = spark.table("olist.bronze.marketing_leads")

    result = (
        df
        .filter(F.col("mql_id").isNotNull())
        .select(
            "mql_id",
            F.col("first_contact_date").cast("date"),
            "landing_page_id",
            F.coalesce(F.col("origin"), F.lit("unknown")).alias("origin"),
        )
        .dropDuplicates(["mql_id"])
    )
    _write(result, "olist.silver.stg_marketing_leads")


def transform_stg_marketing_deals(spark: SparkSession) -> None:
    log.info("  → stg_marketing_deals")
    df = spark.table("olist.bronze.marketing_deals")

    result = (
        df
        .filter(F.col("mql_id").isNotNull())
        .select(
            "mql_id",
            "seller_id",
            F.col("won_date").cast("date"),
            F.coalesce(F.col("business_segment"), F.lit("unknown")).alias("business_segment"),
            F.coalesce(F.col("lead_type"),         F.lit("unknown")).alias("lead_type"),
            F.coalesce(F.col("business_type"),     F.lit("unknown")).alias("business_type"),
            F.col("declared_monthly_revenue").cast("double"),
        )
        .dropDuplicates(["mql_id"])
    )
    _write(result, "olist.silver.stg_marketing_deals")


# ── Intermediate join (dùng bởi Gold) ────────────────────────────────────────

def transform_int_orders_enriched(spark: SparkSession) -> None:
    """
    Denormalized order table: orders + customers + items agg + payments + reviews.
    Gold layer đọc bảng này thay vì join nhiều bảng Silver.
    """
    log.info("  → int_orders_enriched")

    orders    = spark.table("olist.silver.stg_orders")
    customers = spark.table("olist.silver.stg_customers").select(
        "customer_id", "customer_unique_id", "customer_city", "customer_state"
    )
    items     = spark.table("olist.silver.stg_order_items")
    products  = spark.table("olist.silver.stg_products").select(
        "product_id", "product_weight_g", "product_category_name"
    )
    payments  = spark.table("olist.silver.stg_order_payments").select(
        "order_id", "payment_value", "payment_type", "max_installments"
    )
    reviews   = spark.table("olist.silver.stg_order_reviews").select(
        "order_id", "review_score"
    )

    # Aggregate items per order
    items_agg = (
        items
        .join(products, "product_id", "left")
        .groupBy("order_id")
        .agg(
            F.sum("price").alias("order_revenue"),
            F.sum("freight_value").alias("order_freight"),
            F.count("order_item_id").alias("item_count"),
            F.sum("product_weight_g").alias("total_weight_g"),
        )
    )

    result = (
        orders
        .join(customers,  "customer_id", "left")
        .join(items_agg,  "order_id",    "left")
        .join(payments,   "order_id",    "left")
        .join(reviews,    "order_id",    "left")
        .withColumn(
            "delivery_delay_days",
            F.col("actual_delivery_days") - F.col("estimated_delivery_days"),
        )
    )
    _write(result, "olist.silver.int_orders_enriched")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_silver(spark: SparkSession) -> None:
    log.info("=== SILVER TRANSFORM START ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.silver")

    transform_stg_orders(spark)
    transform_stg_order_items(spark)
    transform_stg_order_payments(spark)
    transform_stg_order_reviews(spark)
    transform_stg_products(spark)
    transform_stg_sellers(spark)
    transform_stg_customers(spark)
    transform_stg_marketing_leads(spark)
    transform_stg_marketing_deals(spark)
    transform_int_orders_enriched(spark)

    log.info("=== SILVER TRANSFORM COMPLETE ===")


if __name__ == "__main__":
    spark = get_spark()
    try:
        run_silver(spark)
    finally:
        spark.stop()
