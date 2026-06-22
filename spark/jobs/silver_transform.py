"""
Silver Layer Transform – incremental MERGE INTO with watermark tracking.

Chế độ hoạt động:
  - Lần đầu chạy (Silver tables chưa tồn tại): full load (createOrReplace)
  - Lần sau: chỉ đọc Bronze records MỚI (filter _ingested_at > watermark)
             rồi MERGE INTO Silver (upsert: update matched + insert new)

Watermark mỗi bảng lưu riêng trong PostgreSQL (bảng pipeline_watermarks).
int_orders_enriched union affected order_ids từ TẤT CẢ Bronze sources để
catch cả orders có review/payment mới (không chỉ đơn hàng mới).
"""
import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
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


# ── SparkSession ──────────────────────────────────────────────────────────────

def get_spark(app_name: str = "olist-silver-transform") -> SparkSession:
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


# ── Core helpers ──────────────────────────────────────────────────────────────

def _table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.table(table)
        return True
    except Exception:
        return False


def _merge_or_create(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    pk: "str | list[str]",
) -> int:
    """
    Initial run  → createOrReplace (builds table schema + data).
    Subsequent   → MERGE INTO on pk (upsert: update existing, insert new).
    Returns number of rows in the source batch (0 = skipped).
    """
    n = df.count()
    if n == 0:
        log.info("    → %s: no new rows, skipping", table)
        return 0

    if not _table_exists(spark, table):
        df.writeTo(table).createOrReplace()
        log.info("    ✓ %s  initial load  (%d rows)", table, n)
        return n

    pks = [pk] if isinstance(pk, str) else list(pk)
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in pks)

    df.createOrReplaceTempView("_merge_src")
    spark.sql(f"""
        MERGE INTO {table} t
        USING (SELECT * FROM _merge_src) s ON ({on_clause})
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    log.info("    ✓ %s  merged  (%d rows)", table, n)
    return n


def _max_ingested_at(df: DataFrame) -> str:
    """Max _ingested_at from the filtered Bronze batch."""
    return str(df.agg(F.max("_ingested_at")).collect()[0][0])


def _wm_filter(df: DataFrame, last_wm: str) -> DataFrame:
    return df.filter(F.col("_ingested_at") > F.lit(last_wm).cast("timestamp"))


# ── Staging transforms ────────────────────────────────────────────────────────

def transform_stg_orders(spark: SparkSession, last_wm: str) -> None:
    log.info("  → stg_orders  (since %s)", last_wm)
    bronze = _wm_filter(spark.table("olist.bronze.ecommerce_orders"), last_wm)

    result = (
        bronze
        .select(
            "order_id", "customer_id", "order_status",
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

    n = _merge_or_create(spark, result, "olist.silver.stg_orders", "order_id")
    if n > 0:
        wm.save("silver.stg_orders", _max_ingested_at(bronze))


def transform_stg_order_items(spark: SparkSession, last_wm: str) -> None:
    log.info("  → stg_order_items  (since %s)", last_wm)
    bronze = _wm_filter(spark.table("olist.bronze.ecommerce_order_items"), last_wm)

    result = (
        bronze
        .select(
            "order_id",
            F.col("order_item_id").cast("int"),
            "product_id", "seller_id",
            F.col("shipping_limit_date").cast("timestamp"),
            F.col("price").cast("double"),
            F.col("freight_value").cast("double"),
        )
        .filter(F.col("order_id").isNotNull())
    )

    n = _merge_or_create(spark, result, "olist.silver.stg_order_items",
                         ["order_id", "order_item_id"])
    if n > 0:
        wm.save("silver.stg_order_items", _max_ingested_at(bronze))


def transform_stg_order_payments(spark: SparkSession, last_wm: str) -> None:
    log.info("  → stg_order_payments  (since %s)", last_wm)
    bronze = _wm_filter(spark.table("olist.bronze.ecommerce_order_payments"), last_wm)

    result = (
        bronze
        .filter(F.col("order_id").isNotNull())
        .groupBy("order_id")
        .agg(
            F.sum(F.col("payment_value").cast("double")).alias("payment_value"),
            F.first(
                F.when(F.col("payment_sequential").cast("int") == 1,
                       F.col("payment_type")),
                ignorenulls=True,
            ).alias("payment_type"),
            F.greatest(
                F.max(F.col("payment_installments").cast("int")), F.lit(1)
            ).alias("max_installments"),
            F.count("*").alias("payment_count"),
        )
    )

    n = _merge_or_create(spark, result, "olist.silver.stg_order_payments", "order_id")
    if n > 0:
        wm.save("silver.stg_order_payments", _max_ingested_at(bronze))


def transform_stg_order_reviews(spark: SparkSession, last_wm: str) -> None:
    log.info("  → stg_order_reviews  (since %s)", last_wm)
    bronze = _wm_filter(spark.table("olist.bronze.ecommerce_order_reviews"), last_wm)

    w = Window.partitionBy("order_id").orderBy(
        F.col("review_answer_timestamp").cast("timestamp").desc()
    )

    result = (
        bronze
        .filter(F.col("order_id").isNotNull())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "order_id", "review_id",
            F.col("review_score").cast("int"),
            "review_comment_title", "review_comment_message",
            F.col("review_creation_date").cast("timestamp"),
            F.col("review_answer_timestamp").cast("timestamp"),
        )
    )

    n = _merge_or_create(spark, result, "olist.silver.stg_order_reviews", "order_id")
    if n > 0:
        wm.save("silver.stg_order_reviews", _max_ingested_at(bronze))


def transform_stg_products(spark: SparkSession, last_wm: str) -> None:
    log.info("  → stg_products  (since %s)", last_wm)
    bronze = _wm_filter(spark.table("olist.bronze.ecommerce_products"), last_wm)

    result = (
        bronze
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

    n = _merge_or_create(spark, result, "olist.silver.stg_products", "product_id")
    if n > 0:
        wm.save("silver.stg_products", _max_ingested_at(bronze))


def transform_stg_sellers(spark: SparkSession, last_wm: str) -> None:
    log.info("  → stg_sellers  (since %s)", last_wm)
    bronze = _wm_filter(spark.table("olist.bronze.ecommerce_sellers"), last_wm)

    result = (
        bronze
        .filter(F.col("seller_id").isNotNull())
        .select(
            "seller_id", "seller_zip_code_prefix",
            F.trim(F.lower(F.col("seller_city"))).alias("seller_city"),
            F.upper(F.col("seller_state")).alias("seller_state"),
        )
        .dropDuplicates(["seller_id"])
    )

    n = _merge_or_create(spark, result, "olist.silver.stg_sellers", "seller_id")
    if n > 0:
        wm.save("silver.stg_sellers", _max_ingested_at(bronze))


def transform_stg_customers(spark: SparkSession, last_wm: str) -> None:
    log.info("  → stg_customers  (since %s)", last_wm)
    bronze = _wm_filter(spark.table("olist.bronze.ecommerce_customers"), last_wm)

    result = (
        bronze
        .filter(F.col("customer_id").isNotNull())
        .select(
            "customer_id", "customer_unique_id", "customer_zip_code_prefix",
            F.trim(F.lower(F.col("customer_city"))).alias("customer_city"),
            F.upper(F.col("customer_state")).alias("customer_state"),
        )
        .dropDuplicates(["customer_id"])
    )

    n = _merge_or_create(spark, result, "olist.silver.stg_customers", "customer_id")
    if n > 0:
        wm.save("silver.stg_customers", _max_ingested_at(bronze))


def transform_stg_marketing_leads(spark: SparkSession, last_wm: str) -> None:
    log.info("  → stg_marketing_leads  (since %s)", last_wm)
    bronze = _wm_filter(spark.table("olist.bronze.marketing_leads"), last_wm)

    result = (
        bronze
        .filter(F.col("mql_id").isNotNull())
        .select(
            "mql_id",
            F.col("first_contact_date").cast("date"),
            "landing_page_id",
            F.coalesce(F.col("origin"), F.lit("unknown")).alias("origin"),
        )
        .dropDuplicates(["mql_id"])
    )

    n = _merge_or_create(spark, result, "olist.silver.stg_marketing_leads", "mql_id")
    if n > 0:
        wm.save("silver.stg_marketing_leads", _max_ingested_at(bronze))


def transform_stg_marketing_deals(spark: SparkSession, last_wm: str) -> None:
    log.info("  → stg_marketing_deals  (since %s)", last_wm)
    bronze = _wm_filter(spark.table("olist.bronze.marketing_deals"), last_wm)

    result = (
        bronze
        .filter(F.col("mql_id").isNotNull())
        .select(
            "mql_id", "seller_id",
            F.col("won_date").cast("date"),
            F.coalesce(F.col("business_segment"), F.lit("unknown")).alias("business_segment"),
            F.coalesce(F.col("lead_type"),         F.lit("unknown")).alias("lead_type"),
            F.coalesce(F.col("business_type"),     F.lit("unknown")).alias("business_type"),
            F.col("declared_monthly_revenue").cast("double"),
        )
        .dropDuplicates(["mql_id"])
    )

    n = _merge_or_create(spark, result, "olist.silver.stg_marketing_deals", "mql_id")
    if n > 0:
        wm.save("silver.stg_marketing_deals", _max_ingested_at(bronze))


# ── Intermediate join ─────────────────────────────────────────────────────────

def transform_int_orders_enriched(spark: SparkSession, prev_wm: dict) -> None:
    """
    MERGE INTO int_orders_enriched cho các orders bị ảnh hưởng trong run này.

    Union order_ids từ TẤT CẢ Bronze sources để catch:
      - Đơn hàng mới (Bronze orders)
      - Review mới cho đơn cũ (Bronze reviews)
      - Payment mới/cập nhật (Bronze payments)
      - Item mới (Bronze order_items)
    """
    log.info("  → int_orders_enriched")

    def _new_ids(bronze_table: str, wm_key: str) -> DataFrame:
        return (
            _wm_filter(spark.table(bronze_table), prev_wm[wm_key])
            .select("order_id")
            .filter(F.col("order_id").isNotNull())
        )

    affected_order_ids = (
        _new_ids("olist.bronze.ecommerce_orders",       "orders")
        .union(_new_ids("olist.bronze.ecommerce_order_items",    "items"))
        .union(_new_ids("olist.bronze.ecommerce_order_payments", "payments"))
        .union(_new_ids("olist.bronze.ecommerce_order_reviews",  "reviews"))
        .distinct()
        .cache()
    )

    n_affected = affected_order_ids.count()
    if n_affected == 0:
        log.info("    → int_orders_enriched: no affected orders, skipping")
        affected_order_ids.unpersist()
        return

    log.info("    %d affected orders", n_affected)

    orders    = spark.table("olist.silver.stg_orders") \
                     .join(affected_order_ids, "order_id", "inner")
    customers = spark.table("olist.silver.stg_customers").select(
        "customer_id", "customer_unique_id", "customer_city", "customer_state"
    )
    items     = spark.table("olist.silver.stg_order_items") \
                     .join(affected_order_ids, "order_id", "inner")
    products  = spark.table("olist.silver.stg_products").select(
        "product_id", "product_weight_g", "product_category_name"
    )
    payments  = spark.table("olist.silver.stg_order_payments").select(
        "order_id", "payment_value", "payment_type", "max_installments"
    )
    reviews   = spark.table("olist.silver.stg_order_reviews").select(
        "order_id", "review_score"
    )

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
        .join(customers, "customer_id", "left")
        .join(items_agg, "order_id",    "left")
        .join(payments,  "order_id",    "left")
        .join(reviews,   "order_id",    "left")
        .withColumn(
            "delivery_delay_days",
            F.col("actual_delivery_days") - F.col("estimated_delivery_days"),
        )
        # Timestamp cho Gold layer biết row này được refresh lúc nào
        .withColumn("_silver_updated_at", F.current_timestamp())
    )

    _merge_or_create(spark, result, "olist.silver.int_orders_enriched", "order_id")
    affected_order_ids.unpersist()


# ── Main ──────────────────────────────────────────────────────────────────────

def run_silver(spark: SparkSession) -> None:
    log.info("=== SILVER TRANSFORM START ===")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.silver")

    # Capture watermarks TRƯỚC khi chạy bất kỳ transform nào.
    # Dùng để lọc Bronze → chỉ lấy records mới trong run này.
    prev_wm = {
        "orders":    wm.get("silver.stg_orders"),
        "items":     wm.get("silver.stg_order_items"),
        "payments":  wm.get("silver.stg_order_payments"),
        "reviews":   wm.get("silver.stg_order_reviews"),
        "products":  wm.get("silver.stg_products"),
        "sellers":   wm.get("silver.stg_sellers"),
        "customers": wm.get("silver.stg_customers"),
        "leads":     wm.get("silver.stg_marketing_leads"),
        "deals":     wm.get("silver.stg_marketing_deals"),
    }

    transform_stg_orders(spark,            prev_wm["orders"])
    transform_stg_order_items(spark,       prev_wm["items"])
    transform_stg_order_payments(spark,    prev_wm["payments"])
    transform_stg_order_reviews(spark,     prev_wm["reviews"])
    transform_stg_products(spark,          prev_wm["products"])
    transform_stg_sellers(spark,           prev_wm["sellers"])
    transform_stg_customers(spark,         prev_wm["customers"])
    transform_stg_marketing_leads(spark,   prev_wm["leads"])
    transform_stg_marketing_deals(spark,   prev_wm["deals"])
    transform_int_orders_enriched(spark,   prev_wm)

    log.info("=== SILVER TRANSFORM COMPLETE ===")


if __name__ == "__main__":
    spark = get_spark()
    try:
        run_silver(spark)
    finally:
        spark.stop()
