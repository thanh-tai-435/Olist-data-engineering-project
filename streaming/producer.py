"""
Olist Streaming Producer
Replay CSV events vào Redpanda theo thứ tự thời gian thực (time-compressed).

Cách dùng (trong prefect-worker):
  # Mặc định: 1 ngày thực = 1 giây (86400x)
  docker exec olist-prefect-worker python /app/streaming/producer.py

  # Nhanh hơn: 1 giờ = 1 giây
  docker exec -e SPEED_FACTOR=3600 olist-prefect-worker python /app/streaming/producer.py

  # Instant (không sleep, flood tất cả events)
  docker exec -e SPEED_FACTOR=0 olist-prefect-worker python /app/streaming/producer.py

  # Chỉ produce N events đầu (test)
  docker exec -e MAX_EVENTS=100 olist-prefect-worker python /app/streaming/producer.py

Topics:
  olist.orders    ← olist_orders_dataset.csv
  olist.reviews   ← olist_order_reviews_dataset.csv
  olist.payments  ← olist_order_payments_dataset.csv
"""
import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BROKERS      = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
DATA_DIR     = os.environ.get("DATA_DIR", "/app/data/raw/ecommerce")

# Chế độ chạy:
#   rate   (mặc định) – gửi đúng RATE events/giây, lặp vô hạn qua dataset
#   replay             – nén thời gian thực theo SPEED_FACTOR
MODE         = os.environ.get("MODE", "replay")
RATE         = float(os.environ.get("RATE", "2"))          # events/giây (MODE=rate)
SPEED_FACTOR = int(os.environ.get("SPEED_FACTOR", "86400")) # (MODE=replay)
MAX_EVENTS   = int(os.environ.get("MAX_EVENTS", "0"))        # 0 = vô hạn


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def _delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)


def get_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BROKERS,
        "acks": "all",
        "linger.ms": 20,          # micro-batching để tăng throughput
        "compression.type": "lz4",
    })


def produce(producer: Producer, topic: str, key: str, payload: dict) -> None:
    producer.produce(
        topic=topic,
        key=key.encode(),
        value=json.dumps(payload, default=str).encode(),
        on_delivery=_delivery_report,
    )
    producer.poll(0)   # non-blocking flush của internal queue


# ── Load và merge event streams ───────────────────────────────────────────────

def load_events(data_dir: str) -> pd.DataFrame:
    """
    Đọc CSV, enrich orders với customer_state + payment_value, sắp xếp theo timestamp.
    """
    events = []

    # ── Load orders ───────────────────────────────────────────────────────────
    orders_path = os.path.join(data_dir, "olist_orders_dataset.csv")
    if not os.path.exists(orders_path):
        raise FileNotFoundError(
            f"Không tìm thấy {orders_path}. "
            "Đảm bảo volume ./data:/app/data được mount đúng."
        )
    orders = pd.read_csv(orders_path, low_memory=False)
    orders["_event_ts"] = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce"
    )
    log.info("  Loaded orders:    %d rows", len(orders))

    # ── Join customers → thêm customer_state, customer_city ──────────────────
    customers_path = os.path.join(data_dir, "olist_customers_dataset.csv")
    if os.path.exists(customers_path):
        customers = pd.read_csv(
            customers_path,
            usecols=["customer_id", "customer_state", "customer_city"],
            low_memory=False,
        )
        orders = orders.merge(customers, on="customer_id", how="left")
        log.info("  Joined customers: customer_state OK")

    # ── Join payments → thêm payment_value, payment_type ─────────────────────
    payments_path = os.path.join(data_dir, "olist_order_payments_dataset.csv")
    if os.path.exists(payments_path):
        payments_raw = pd.read_csv(payments_path, low_memory=False)
        # payment_type: lấy của sequential=1, fallback về sequential nhỏ nhất
        primary = (
            payments_raw[payments_raw["payment_sequential"] == 1]
            [["order_id", "payment_type"]]
            .drop_duplicates("order_id")
        )
        fallback = (
            payments_raw.sort_values("payment_sequential")
            .drop_duplicates("order_id")[["order_id", "payment_type"]]
            .rename(columns={"payment_type": "_pt_fallback"})
        )
        payments_agg = (
            payments_raw
            .groupby("order_id", as_index=False)
            .agg(payment_value=("payment_value", "sum"))
            .merge(primary,   on="order_id", how="left")
            .merge(fallback,  on="order_id", how="left")
        )
        payments_agg["payment_type"] = (
            payments_agg["payment_type"].fillna(payments_agg["_pt_fallback"])
        )
        payments_agg.drop(columns=["_pt_fallback"], inplace=True)
        orders = orders.merge(payments_agg, on="order_id", how="left")
        log.info("  Joined payments:  payment_value OK")

    orders["_topic"]     = "olist.orders"
    orders["_kafka_key"] = orders["order_id"].astype(str)
    events.append(orders)

    # ── olist.reviews ─────────────────────────────────────────────────────────
    reviews_path = os.path.join(data_dir, "olist_order_reviews_dataset.csv")
    if os.path.exists(reviews_path):
        reviews = pd.read_csv(reviews_path, low_memory=False)
        reviews["_topic"]     = "olist.reviews"
        reviews["_kafka_key"] = reviews["order_id"].astype(str)
        reviews["_event_ts"]  = pd.to_datetime(
            reviews["review_answer_timestamp"], errors="coerce"
        )
        events.append(reviews)
        log.info("  Loaded reviews:   %d rows", len(reviews))

    combined = (
        pd.concat(events, ignore_index=True)
        .dropna(subset=["_event_ts"])
        .sort_values("_event_ts")
        .reset_index(drop=True)
    )
    log.info("  Total events with timestamp: %d", len(combined))
    return combined


# ── Helpers ───────────────────────────────────────────────────────────────────

def _send_row(producer: Producer, row: pd.Series, sent: int) -> None:
    payload = {
        k: v for k, v in row.items()
        if not k.startswith("_") and pd.notna(v)
    }
    payload["_produced_at"] = datetime.now(timezone.utc).isoformat()
    produce(producer, row["_topic"], row["_kafka_key"], payload)
    if sent % 50 == 0:
        log.info("  Sent %d  topic=%-20s  data_ts=%s",
                 sent, row["_topic"], str(row["_event_ts"])[:19])


# ── Mode: rate (default) ──────────────────────────────────────────────────────

def run_rate(producer: Producer, events: pd.DataFrame) -> None:
    """
    Gửi đúng RATE events/giây, lặp vô hạn qua dataset (shuffle mỗi vòng).
    Mỗi event được gán timestamp = NOW để dashboard thấy data "mới".
    """
    interval = 1.0 / RATE if RATE > 0 else 0

    log.info("=== PRODUCER START  [mode=rate] ===")
    log.info("  Rate:   %.1f events/s  (~%.0f orders/min)", RATE, RATE * 60)
    log.info("  Topics: olist.orders | olist.reviews | olist.payments")
    log.info("  Loop:   vô hạn qua %d events (Ctrl+C để dừng)", len(events))

    sent = 0
    while True:
        shuffled = events.sample(frac=1).reset_index(drop=True)
        for _, row in shuffled.iterrows():
            if MAX_EVENTS > 0 and sent >= MAX_EVENTS:
                log.info("=== PRODUCER STOPPED  MAX_EVENTS=%d ===", MAX_EVENTS)
                producer.flush()
                return

            t0 = time.monotonic()
            _send_row(producer, row, sent)
            sent += 1

            elapsed = time.monotonic() - t0
            sleep = interval - elapsed
            if sleep > 0:
                time.sleep(sleep)


# ── Mode: replay ──────────────────────────────────────────────────────────────

def run_replay(producer: Producer, events: pd.DataFrame) -> None:
    """Nén thời gian thực theo SPEED_FACTOR (hành vi cũ)."""
    total      = MAX_EVENTS if MAX_EVENTS > 0 else len(events)
    subset     = events.iloc[:total]
    first_ts   = subset["_event_ts"].iloc[0]
    wall_start = time.monotonic()

    log.info("=== PRODUCER START  [mode=replay] ===")
    log.info("  Speed factor: %sx", SPEED_FACTOR)
    log.info("  Events: %d  (%s → %s)",
             total, str(first_ts)[:19], str(subset["_event_ts"].iloc[-1])[:19])

    sent = 0
    for _, row in subset.iterrows():
        if SPEED_FACTOR > 0:
            data_elapsed = (row["_event_ts"] - first_ts).total_seconds()
            wall_elapsed = time.monotonic() - wall_start
            sleep_sec    = data_elapsed / SPEED_FACTOR - wall_elapsed
            if sleep_sec > 0:
                time.sleep(sleep_sec)
        _send_row(producer, row, sent)
        sent += 1

    producer.flush()
    elapsed = time.monotonic() - wall_start
    log.info("=== PRODUCER DONE  %d events in %.1fs ===", sent, elapsed)


if __name__ == "__main__":
    log.info("Loading CSV data from %s ...", DATA_DIR)
    events   = load_events(DATA_DIR)
    producer = get_producer()
    try:
        if MODE == "replay":
            run_replay(producer, events)
        else:
            run_rate(producer, events)
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
        producer.flush()
