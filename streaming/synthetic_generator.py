"""
Olist Synthetic Event Generator
Non-stationary e-commerce stream → Redpanda (same topics as producer.py)

Mechanisms:
  - HMM regime:  NORMAL / SPIKE / RECOVERY / OUTAGE
  - OU drift:    price level, geographic mix, payment mix drift over time
  - Seasonality: time-of-day and day-of-week multipliers
  - Entity lifecycle: sellers onboard/churn, new customers appear

Usage:
  docker exec -e RATE=3 olist-prefect-worker python /app/streaming/synthetic_generator.py
  docker exec -e RATE=10 -e MAX_EVENTS=5000 olist-prefect-worker python /app/streaming/synthetic_generator.py
"""
import os
import json
import time
import uuid
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

BROKERS    = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
DATA_DIR   = os.environ.get("DATA_DIR", "/app/data/raw/ecommerce")
RATE       = float(os.environ.get("RATE", "2"))       # events/second baseline
MAX_EVENTS = int(os.environ.get("MAX_EVENTS", "0"))   # 0 = infinite


# ── Ornstein-Uhlenbeck: slowly drifting scalar ────────────────────────────────

class OU:
    def __init__(self, mu: float, theta: float = 0.02, sigma: float = 0.01):
        self.mu, self.theta, self.sigma, self.x = mu, theta, sigma, mu

    def step(self) -> float:
        self.x += self.theta * (self.mu - self.x) + self.sigma * np.random.randn()
        return self.x


# ── HMM Regime ────────────────────────────────────────────────────────────────

# Transition matrix: P[from, to]
_TRANS = np.array([
    [0.9970, 0.0025, 0.0003, 0.0002],   # NORMAL
    [0.0500, 0.8500, 0.1000, 0.0000],   # SPIKE  → mostly recovery
    [0.2000, 0.0100, 0.7900, 0.0000],   # RECOVERY
    [0.3000, 0.0000, 0.0000, 0.7000],   # OUTAGE
])
# (rate_mult, price_mult, delivery_mult)
_EFFECTS = [(1.0, 1.0, 1.0), (8.0, 0.7, 1.4), (2.5, 0.9, 1.1), (0.05, 1.0, 2.0)]
_NAMES   = ["NORMAL", "SPIKE", "RECOVERY", "OUTAGE"]

class Regime:
    def __init__(self):
        self.state = 0

    def step(self) -> int:
        self.state = int(np.random.choice(4, p=_TRANS[self.state]))
        return self.state

    @property
    def name(self) -> str:
        return _NAMES[self.state]

    @property
    def rate_mult(self) -> float:
        return _EFFECTS[self.state][0]

    @property
    def price_mult(self) -> float:
        return _EFFECTS[self.state][1]

    @property
    def delivery_mult(self) -> float:
        return _EFFECTS[self.state][2]


# ── Entity Pool ───────────────────────────────────────────────────────────────

class EntityPool:
    def __init__(self, sellers_df: pd.DataFrame, customers_df: pd.DataFrame):
        self.sellers      = sellers_df["seller_id"].tolist() or [str(uuid.uuid4().hex) for _ in range(50)]
        self.seller_state = dict(zip(sellers_df.get("seller_id", pd.Series()), sellers_df.get("seller_state", pd.Series())))
        self.customers    = customers_df["customer_unique_id"].tolist() if len(customers_df) else []
        self.cust_state   = dict(zip(customers_df.get("customer_unique_id", pd.Series()), customers_df.get("customer_state", pd.Series())))
        self.cust_city    = dict(zip(customers_df.get("customer_unique_id", pd.Series()), customers_df.get("customer_city", pd.Series())))
        self._tick = 0

    def tick(self, state_labels: list, state_probs: np.ndarray):
        self._tick += 1
        if self._tick % 100 != 0:
            return
        # Onboard new seller
        if np.random.random() < 0.3:
            sid = uuid.uuid4().hex[:32]
            self.sellers.append(sid)
            self.seller_state[sid] = np.random.choice(state_labels, p=state_probs)
        # Churn random seller
        if len(self.sellers) > 20 and np.random.random() < 0.1:
            idx = np.random.randint(len(self.sellers))
            self.seller_state.pop(self.sellers.pop(idx), None)

    def sample_seller(self) -> tuple:
        sid = np.random.choice(self.sellers)
        return sid, self.seller_state.get(sid, "SP")

    def sample_customer(self, p_new: float, state_labels: list, state_probs: np.ndarray) -> tuple:
        if np.random.random() < p_new or not self.customers:
            cid   = uuid.uuid4().hex[:32]
            state = np.random.choice(state_labels, p=state_probs)
            city  = f"{state.lower()}_city_{np.random.randint(1,30)}"
            self.customers.append(cid)
            self.cust_state[cid] = state
            self.cust_city[cid]  = city
            return cid, state, city
        cid = np.random.choice(self.customers)
        return cid, self.cust_state.get(cid, "SP"), self.cust_city.get(cid, "unknown")


# ── Distributions (fit from CSV once) ─────────────────────────────────────────

class Distributions:
    def __init__(self, data_dir: str):
        log.info("Fitting distributions from CSV …")
        orders = pd.read_csv(os.path.join(data_dir, "olist_orders_dataset.csv"), low_memory=False)

        pay_path = os.path.join(data_dir, "olist_order_payments_dataset.csv")
        if os.path.exists(pay_path):
            pay = pd.read_csv(pay_path, low_memory=False)
            orders = orders.merge(pay.groupby("order_id")["payment_value"].sum().reset_index(), on="order_id", how="left")
            orders = orders.merge(
                pay[pay["payment_sequential"] == 1][["order_id", "payment_type"]].drop_duplicates("order_id"),
                on="order_id", how="left",
            )

        items_path = os.path.join(data_dir, "olist_order_items_dataset.csv")
        if os.path.exists(items_path):
            items = pd.read_csv(items_path, low_memory=False)
            orders = orders.merge(items.groupby("order_id").size().reset_index(name="item_count"), on="order_id", how="left")

        cust_path = os.path.join(data_dir, "olist_customers_dataset.csv")
        custs = pd.read_csv(cust_path, low_memory=False) if os.path.exists(cust_path) else pd.DataFrame()
        if len(custs):
            orders = orders.merge(custs[["customer_id", "customer_state", "customer_city", "customer_unique_id"]], on="customer_id", how="left")

        sell_path = os.path.join(data_dir, "olist_sellers_dataset.csv")
        sellers = pd.read_csv(sell_path, low_memory=False) if os.path.exists(sell_path) else pd.DataFrame(columns=["seller_id", "seller_state"])

        rev_path = os.path.join(data_dir, "olist_order_reviews_dataset.csv")
        if os.path.exists(rev_path):
            rev = pd.read_csv(rev_path, low_memory=False)
            rs  = rev["review_score"].value_counts(normalize=True).sort_index()
            self.review_scores = rs.index.tolist()
            self.review_probs  = rs.values.tolist()
        else:
            self.review_scores = [1, 2, 3, 4, 5]
            self.review_probs  = [0.11, 0.08, 0.08, 0.16, 0.57]

        # Price log-normal
        vals = orders["payment_value"].dropna()
        vals = vals[vals > 0]
        self.price_mu    = float(np.log(vals).mean())
        self.price_sigma = float(np.log(vals).std())

        # Geographic
        sc = orders["customer_state"].value_counts(normalize=True)
        self.state_labels = sc.index.tolist()
        self.state_probs  = sc.values.tolist()

        # Payment type
        pc = orders["payment_type"].value_counts(normalize=True).dropna()
        self.pt_labels = pc.index.tolist()
        self.pt_probs  = pc.values.tolist()


        # Item count
        self.item_lambda = float(orders["item_count"].dropna().mean()) if "item_count" in orders else 1.3

        # Purchase hour distribution
        orders["_ts"] = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce")
        hd = orders["_ts"].dt.hour.value_counts(normalize=True).sort_index()
        self.hour_probs = dict(zip(hd.index.tolist(), hd.values.tolist()))

        self.sellers_df  = sellers
        self.customers_df = custs if len(custs) else pd.DataFrame(columns=["customer_unique_id", "customer_state", "customer_city"])

        log.info("  price μ=%.2f σ=%.2f | states=%d | pt=%d | sellers=%d",
                 self.price_mu, self.price_sigma, len(self.state_labels), len(self.pt_labels), len(sellers))


# ── Generator ─────────────────────────────────────────────────────────────────

class SyntheticGenerator:
    def __init__(self, dist: Distributions):
        self.dist   = dist
        self.regime = Regime()
        self.pool   = EntityPool(dist.sellers_df, dist.customers_df)

        # OU processes per drifting parameter
        self.ou_price    = OU(mu=dist.price_mu,   theta=0.01, sigma=0.015)
        self.ou_states   = [OU(mu=p, theta=0.005, sigma=0.002) for p in dist.state_probs]
        self.ou_pt       = [OU(mu=p, theta=0.01,  sigma=0.003) for p in dist.pt_probs]

        self._pending_reviews: list[dict] = []

    def _state_probs(self) -> np.ndarray:
        raw = np.array([max(ou.step(), 1e-6) for ou in self.ou_states])
        return raw / raw.sum()

    def _pt_probs(self) -> np.ndarray:
        raw = np.array([max(ou.step(), 1e-6) for ou in self.ou_pt])
        return raw / raw.sum()

    def _tod_mult(self) -> float:
        h = datetime.now().hour
        if 11 <= h <= 14 or 20 <= h <= 22:
            return 1.8
        if 0 <= h <= 6:
            return 0.3
        return 1.0

    def _dow_mult(self) -> float:
        return [1.2, 1.15, 1.1, 1.05, 1.0, 0.8, 0.75][datetime.now().weekday()]

    def effective_rate_mult(self) -> float:
        self.regime.step()
        return self.regime.rate_mult * self._tod_mult() * self._dow_mult()

    def generate_order(self) -> dict:
        sp    = self._state_probs()
        pp    = self._pt_probs()
        price = round(float(np.exp(np.random.normal(self.ou_price.step(), self.dist.price_sigma)) * self.regime.price_mult), 2)
        price = max(price, 5.0)

        cid, c_state, c_city = self.pool.sample_customer(0.05, self.dist.state_labels, sp)
        sid, _               = self.pool.sample_seller()

        base_del  = float(np.random.gamma(shape=3, scale=4))
        act_del   = max(1, int(base_del * self.regime.delivery_mult))
        est_del   = max(act_del - 2, act_del + int(np.random.randint(-3, 7)))

        now = datetime.now(timezone.utc)
        oid = uuid.uuid4().hex

        order = {
            "order_id":                      oid,
            "customer_id":                   uuid.uuid4().hex,
            "customer_unique_id":            cid,
            "order_status":                  "created",
            "order_purchase_timestamp":      now.isoformat(),
            "order_approved_at":             now.isoformat(),
            "order_estimated_delivery_date": (now + timedelta(days=est_del)).isoformat(),
            "customer_state":                c_state,
            "customer_city":                 c_city,
            "seller_id":                     sid,
            "payment_value":                 price,
            "payment_type":                  np.random.choice(self.dist.pt_labels, p=pp),
            "order_freight":                 round(price * float(np.random.uniform(0.05, 0.25)), 2),
            "item_count":                    max(1, int(np.random.poisson(self.dist.item_lambda))),
            "actual_delivery_days":          act_del,
            "estimated_delivery_days":       est_del,
            "_regime":                       self.regime.name,
            "_synthetic":                    True,
        }

        if np.random.random() < 0.7:
            self._pending_reviews.append({
                "_send_at":               time.monotonic() + np.random.uniform(3, 60),
                "order_id":               oid,
                "review_id":              uuid.uuid4().hex,
                "review_score":           int(np.random.choice(self.dist.review_scores, p=self.dist.review_probs)),
                "review_creation_date":   now.isoformat(),
                "review_answer_timestamp": now.isoformat(),
                "_synthetic":             True,
            })

        self.pool.tick(self.dist.state_labels, sp)
        return order

    def pop_due_reviews(self) -> list[dict]:
        now  = time.monotonic()
        due  = [r for r in self._pending_reviews if r["_send_at"] <= now]
        self._pending_reviews = [r for r in self._pending_reviews if r["_send_at"] > now]
        return due


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def _on_delivery(err, msg):
    if err:
        log.error("Delivery failed: %s", err)

def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BROKERS, "acks": "all", "linger.ms": 20, "compression.type": "lz4"})

def produce(p: Producer, topic: str, key: str, payload: dict):
    p.produce(topic=topic, key=key.encode(), value=json.dumps(payload, default=str).encode(), on_delivery=_on_delivery)
    p.poll(0)


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    dist = Distributions(DATA_DIR)
    gen  = SyntheticGenerator(dist)
    p    = make_producer()

    log.info("=== SYNTHETIC GENERATOR START  baseline=%.1f events/s ===", RATE)
    log.info("    Ctrl+C to stop  |  MAX_EVENTS=%s", MAX_EVENTS or "∞")

    interval = 1.0 / RATE if RATE > 0 else 0
    sent = 0

    while True:
        if MAX_EVENTS > 0 and sent >= MAX_EVENTS:
            break

        t0         = time.monotonic()
        rate_mult  = gen.effective_rate_mult()
        skip_prob  = max(0.0, 1.0 - rate_mult / max(rate_mult, 10.0))

        if np.random.random() < skip_prob:
            time.sleep(interval)
            continue

        order = gen.generate_order()
        produce(p, "olist.orders", order["order_id"], order)
        sent += 1

        for rev in gen.pop_due_reviews():
            produce(p, "olist.reviews", rev["order_id"], rev)

        if sent % 100 == 0:
            log.info("sent=%5d  regime=%-10s  state=%s  price=R$%6.0f  sellers=%d",
                     sent, gen.regime.name, order["customer_state"],
                     order["payment_value"], len(gen.pool.sellers))

        sleep = interval - (time.monotonic() - t0)
        if sleep > 0:
            time.sleep(sleep)

    log.info("=== DONE  %d events ===", sent)
    p.flush()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log.info("Stopped by user.")
