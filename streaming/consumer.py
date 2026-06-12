"""
Olist Streaming Consumer
Đọc từ Redpanda → batch → append vào Bronze Iceberg trên R2.

Cách dùng:
  docker exec olist-prefect-worker python /app/streaming/consumer.py

  # Batch size lớn hơn (default 200)
  docker exec -e BATCH_SIZE=500 olist-prefect-worker python /app/streaming/consumer.py

  # Flush sau tối đa 30 giây dù chưa đủ batch
  docker exec -e FLUSH_INTERVAL=30 olist-prefect-worker python /app/streaming/consumer.py

Topics consumed → Bronze tables:
  olist.orders   → olist.bronze.ecommerce_orders
  olist.reviews  → olist.bronze.ecommerce_order_reviews
  olist.payments → olist.bronze.ecommerce_order_payments
"""
import os
import json
import time
import logging
import pandas as pd
import pyarrow as pa
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError, KafkaException
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import schema_to_pyarrow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

try:
    from river_model import DeliveryDelayPredictor, SAVE_EVERY
    _RIVER_ENABLED = True
except ImportError:
    _RIVER_ENABLED = False
    log.warning("river not installed — online learning disabled. pip install river")

BROKERS        = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
ICEBERG_URI    = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
S3_ENDPOINT    = os.environ["S3_ENDPOINT"]
S3_ACCESS_KEY  = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY  = os.environ["S3_SECRET_KEY"]
S3_REGION      = os.environ.get("S3_REGION", "auto")
BUCKET         = "retail-data-lake"

BATCH_SIZE     = int(os.environ.get("BATCH_SIZE", "200"))
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "15"))   # giây
CONSUMER_GROUP = "iceberg-bronze-writer"

# topic → (iceberg table name, timestamp columns)
TOPIC_CONFIG = {
    "olist.orders": (
        "ecommerce_orders",
        ["order_purchase_timestamp", "order_approved_at",
         "order_delivered_carrier_date", "order_delivered_customer_date",
         "order_estimated_delivery_date"],
    ),
    "olist.reviews": (
        "ecommerce_order_reviews",
        ["review_creation_date", "review_answer_timestamp"],
    ),
    "olist.payments": (
        "ecommerce_order_payments",
        [],
    ),
}


# ── Iceberg catalog ───────────────────────────────────────────────────────────

def get_catalog():
    return load_catalog(
        "rest",
        **{
            "uri": ICEBERG_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": S3_REGION,
            "s3.path-style-access": "false",
        },
    )


# ── Flush buffer → Iceberg ────────────────────────────────────────────────────

def flush_buffer(
    catalog,
    topic: str,
    rows: list[dict],
) -> int:
    if not rows:
        return 0

    table_name, ts_cols = TOPIC_CONFIG[topic]
    full_name = f"bronze.{table_name}"

    df = pd.DataFrame(rows)

    # Bỏ metadata của producer (không thuộc schema Bronze)
    df.drop(columns=[c for c in ["_produced_at"] if c in df.columns], inplace=True)

    # Cast timestamp columns — normalize to UTC then strip tz (timezone-aware ISO strings
    # from the synthetic generator would fail a naive astype otherwise)
    for col in ts_cols:
        if col in df.columns:
            df[col] = (
                pd.to_datetime(df[col], errors="coerce", utc=True)
                .dt.tz_localize(None)
                .astype("datetime64[us]")
            )

    # Metadata columns
    now = datetime.now(timezone.utc).isoformat()
    df["_ingested_at"]  = now
    df["_source_file"]  = f"stream:{topic}"
    df["_source_path"]  = f"kafka://{BROKERS}/{topic}"

    try:
        iceberg_tbl = catalog.load_table(full_name)

        # Align DataFrame: thêm cột thiếu (null), bỏ cột thừa, đúng thứ tự
        iceberg_schema = iceberg_tbl.schema()
        existing_cols = [f.name for f in iceberg_schema.fields]
        for col in existing_cols:
            if col not in df.columns:
                df[col] = None
        df = df[existing_cols]

        # Dùng schema_to_pyarrow để ép đúng kiểu (int64, string, timestamp...)
        arrow_schema = schema_to_pyarrow(iceberg_schema)
        arrow_tbl = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)
        iceberg_tbl.append(arrow_tbl)
        log.info("  ✓ %s  +%d rows", full_name, len(df))

    except NoSuchTableError:
        # Bảng chưa tồn tại → tạo mới
        from pyiceberg.partitioning import PartitionSpec
        arrow_tbl = pa.Table.from_pandas(df, preserve_index=False)
        iceberg_tbl = catalog.create_table(
            identifier=full_name,
            schema=arrow_tbl.schema,
            location=f"s3://{BUCKET}/bronze/{table_name}",
            partition_spec=PartitionSpec(),
        )
        iceberg_tbl.append(arrow_tbl)
        log.info("  ✓ %s  CREATED +%d rows", full_name, len(df))

    return len(df)


# ── Main consumer loop ────────────────────────────────────────────────────────

def run() -> None:
    catalog  = get_catalog()
    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(TOPIC_CONFIG.keys()))

    # Buffer: topic → list of dicts
    buffers:    dict[str, list] = defaultdict(list)
    last_flush: dict[str, float] = {t: time.monotonic() for t in TOPIC_CONFIG}
    total_written = 0

    river_model = DeliveryDelayPredictor.load() if _RIVER_ENABLED else None

    log.info("=== CONSUMER START ===")
    log.info("  Group:          %s", CONSUMER_GROUP)
    log.info("  Topics:         %s", ", ".join(TOPIC_CONFIG))
    log.info("  Batch size:     %d", BATCH_SIZE)
    log.info("  Flush interval: %ds", FLUSH_INTERVAL)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Không có message mới → kiểm tra xem có buffer nào quá hạn flush không
                for topic in TOPIC_CONFIG:
                    age = time.monotonic() - last_flush[topic]
                    if buffers[topic] and age >= FLUSH_INTERVAL:
                        log.info("  [timeout flush] %s  (%d rows, %.0fs idle)",
                                 topic, len(buffers[topic]), age)
                        written = flush_buffer(catalog, topic, buffers[topic])
                        total_written += written
                        if river_model is not None and topic == "olist.orders":
                            river_model.process_batch(buffers[topic])
                            if river_model.n_seen % SAVE_EVERY < len(buffers[topic]):
                                river_model.save()
                        try:
                            consumer.commit(asynchronous=False)
                        except KafkaException:
                            pass
                        buffers[topic].clear()
                        last_flush[topic] = time.monotonic()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            topic = msg.topic()
            try:
                payload = json.loads(msg.value().decode())
                buffers[topic].append(payload)
            except Exception as e:
                log.warning("Skip malformed message on %s: %s", topic, e)
                continue

            # Flush khi đủ batch size
            if len(buffers[topic]) >= BATCH_SIZE:
                log.info("  [batch flush]   %s  (%d rows)", topic, len(buffers[topic]))
                written = flush_buffer(catalog, topic, buffers[topic])
                total_written += written
                if river_model is not None and topic == "olist.orders":
                    river_model.process_batch(buffers[topic])
                    if river_model.n_seen % SAVE_EVERY < len(buffers[topic]):
                        river_model.save()
                consumer.commit(asynchronous=False)
                buffers[topic].clear()
                last_flush[topic] = time.monotonic()

    except KeyboardInterrupt:
        log.info("Interrupted — flushing remaining buffers ...")
        for topic, rows in buffers.items():
            if rows:
                flush_buffer(catalog, topic, rows)
        if river_model is not None:
            river_model.save()
        consumer.commit(asynchronous=False)
        log.info("=== CONSUMER STOPPED  total written: %d rows ===", total_written)
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
