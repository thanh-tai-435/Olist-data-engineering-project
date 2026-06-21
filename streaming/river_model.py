"""
River Online Learning — Delivery Delay Classifier
Học từng order event realtime từ Kafka stream (prequential: predict → learn).

Standalone evaluation trên CSV:
  python streaming/river_model.py --csv /app/data/raw/ecommerce

Được import và gọi bởi consumer.py sau mỗi batch flush của olist.orders.
"""
import argparse
import logging
import os
import pickle
from pathlib import Path

import pandas as pd
from river import compose, linear_model, metrics, preprocessing

log = logging.getLogger(__name__)

MODEL_PATH = Path(os.environ.get("RIVER_MODEL_PATH", "/tmp/olist_river_model.pkl"))
SAVE_EVERY = int(os.environ.get("RIVER_SAVE_EVERY", "500"))
LOG_EVERY  = int(os.environ.get("RIVER_LOG_EVERY", "200"))


def _build_pipeline():
    num = compose.Select("freight_value", "payment_value") | preprocessing.StandardScaler()
    cat = compose.Select("customer_state", "payment_type") | preprocessing.OneHotEncoder()
    return (num + cat) | linear_model.LogisticRegression(l2=0.01)


class DeliveryDelayPredictor:
    """
    Online binary classifier: P(order delivered late).
    Label = 1 nếu order_delivered_customer_date > order_estimated_delivery_date.
    - Predict cho mọi order (kể cả chưa delivered).
    - Chỉ learn khi có ground truth (order đã delivered).
    """

    def __init__(self):
        self.model     = _build_pipeline()
        self.roc       = metrics.ROCAUC()
        self.acc       = metrics.Accuracy()
        self.n_seen    = 0
        self.n_learned = 0

    # ── Feature / label extraction ────────────────────────────────────────────

    @staticmethod
    def _features(row: dict) -> dict | None:
        try:
            return {
                "freight_value":  float(row.get("freight_value") or 0),
                "payment_value":  float(row.get("payment_value") or 0),
                "customer_state": str(row.get("customer_state") or "XX"),
                "payment_type":   str(row.get("payment_type") or "unknown"),
            }
        except Exception:
            return None

    @staticmethod
    def _label(row: dict) -> int | None:
        delivered = pd.to_datetime(row.get("order_delivered_customer_date"), errors="coerce")
        estimated = pd.to_datetime(row.get("order_estimated_delivery_date"), errors="coerce")
        if pd.isna(delivered) or pd.isna(estimated):
            return None
        return int(delivered > estimated)

    # ── Core API ──────────────────────────────────────────────────────────────

    def process_one(self, row: dict) -> dict:
        """Predict trước, rồi learn nếu có label (prequential evaluation)."""
        x = self._features(row)
        if x is None:
            return {"order_id": row.get("order_id"), "p_late": None, "learned": False}

        proba  = self.model.predict_proba_one(x)
        p_late = round(proba.get(True, 0.5), 4)

        y = self._label(row)
        learned = False
        if y is not None:
            self.model.learn_one(x, y)
            self.roc.update(y, p_late)
            self.acc.update(y, int(p_late >= 0.5))
            self.n_learned += 1
            learned = True

        self.n_seen += 1
        return {"order_id": row.get("order_id"), "p_late": p_late, "learned": learned}

    def process_batch(self, rows: list[dict]) -> list[dict]:
        results = [self.process_one(r) for r in rows]
        prev = (self.n_seen - len(rows)) // LOG_EVERY
        if self.n_seen // LOG_EVERY > prev:
            self._log_metrics()
        return results

    def _log_metrics(self):
        roc = float(self.roc.get()) if self.n_learned > 20 else float("nan")
        acc = float(self.acc.get()) if self.n_learned > 20 else float("nan")
        log.info(
            "  [River] n_seen=%d  n_learned=%d  ROC-AUC=%.4f  Acc=%.4f",
            self.n_seen, self.n_learned, roc, acc,
        )

    # ── Persistence ───────────────────────────────────────────────────────────

    def save(self, path: Path = MODEL_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f)
        log.info("  [River] Saved → %s  (n_seen=%d)", path, self.n_seen)

    @classmethod
    def load(cls, path: Path = MODEL_PATH) -> "DeliveryDelayPredictor":
        if path.exists():
            try:
                with open(path, "rb") as f:
                    obj = pickle.load(f)
                log.info("  [River] Loaded ← %s  (n_seen=%d)", path, obj.n_seen)
                return obj
            except Exception as e:
                log.warning("  [River] Load failed (%s) — starting fresh.", e)
        return cls()


# ── Standalone: prequential evaluation trên CSV ───────────────────────────────

def evaluate_on_csv(data_dir: str) -> None:
    """Feed toàn bộ CSV row-by-row vào River model theo thứ tự thời gian."""
    orders_path    = os.path.join(data_dir, "olist_orders_dataset.csv")
    payments_path  = os.path.join(data_dir, "olist_order_payments_dataset.csv")
    customers_path = os.path.join(data_dir, "olist_customers_dataset.csv")

    orders = pd.read_csv(orders_path, low_memory=False)

    if os.path.exists(payments_path):
        pay = (
            pd.read_csv(payments_path, low_memory=False)
            .groupby("order_id", as_index=False)
            .agg(payment_value=("payment_value", "sum"),
                 payment_type=("payment_type", "first"))
        )
        orders = orders.merge(pay, on="order_id", how="left")

    if os.path.exists(customers_path):
        cust = pd.read_csv(
            customers_path,
            usecols=["customer_id", "customer_state"],
            low_memory=False,
        )
        orders = orders.merge(cust, on="customer_id", how="left")

    orders = orders.sort_values("order_purchase_timestamp").reset_index(drop=True)

    model = DeliveryDelayPredictor()
    print(f"Evaluating on {len(orders):,} orders (prequential / test-then-train) ...")

    for _, row in orders.iterrows():
        model.process_one(row.to_dict())

    roc = float(model.roc.get()) if model.n_learned > 20 else float("nan")
    acc = float(model.acc.get()) if model.n_learned > 20 else float("nan")
    print("\n=== River Evaluation Results ===")
    print(f"  Total seen:    {model.n_seen:,}")
    print(f"  With label:    {model.n_learned:,}  (delivered orders)")
    print(f"  ROC-AUC:       {roc:.4f}")
    print(f"  Accuracy:      {acc:.4f}")
    model.save()
    print(f"  Model saved →  {MODEL_PATH}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    parser = argparse.ArgumentParser(description="River prequential evaluation")
    parser.add_argument("--csv", default="/app/data/raw/ecommerce", metavar="DIR",
                        help="Directory containing Olist ecommerce CSV files")
    args = parser.parse_args()
    evaluate_on_csv(args.csv)
