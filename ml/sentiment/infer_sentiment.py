"""
Batch inference — score all Olist reviews with the Production ABSA model.

Flow:
  1. Load Production model from MLflow Registry
  2. Load stg_order_reviews from Silver Iceberg
  3. Run batch inference (GPU if available, fallback CPU)
  4. Write gold.review_sentiment to Iceberg (createOrReplace)

Usage (host):
  python ml/sentiment/infer_sentiment.py [--model-version 3]

Incremental mode (skip already-scored reviews):
  python ml/sentiment/infer_sentiment.py --incremental
"""
import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import torch
from pyiceberg.exceptions import NoSuchTableError
from transformers import AutoTokenizer

sys.path.insert(0, str(Path(__file__).parents[1]))
sys.path.insert(0, str(Path(__file__).parent))

import mlflow
from model import ALL_HEADS, ASPECTS, LABEL_NAMES, ABSAModel, get_tokenizer
from utils import get_catalog, setup_mlflow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MLFLOW_MODEL_NAME = "review-sentiment-bertimbau"
INFER_BATCH_SIZE  = 64
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

GOLD_TABLE     = "gold.review_sentiment"
GOLD_LOCATION  = "s3://retail-data-lake/gold/review_sentiment"


# ── Gold table schema ─────────────────────────────────────────────────────────

def _gold_schema() -> pa.Schema:
    fields = [
        pa.field("order_id",     pa.string(), nullable=False),
        pa.field("review_id",    pa.string()),
        pa.field("review_text",  pa.string()),
        pa.field("overall_sentiment",    pa.string()),
        pa.field("overall_confidence",   pa.float64()),
    ]
    for aspect in ASPECTS:
        fields += [
            pa.field(f"{aspect}_sentiment",  pa.string()),
            pa.field(f"{aspect}_confidence", pa.float64()),
        ]
    fields += [
        pa.field("model_version", pa.string()),
        pa.field("scored_at",     pa.timestamp("us", tz="UTC")),
    ]
    return pa.schema(fields)


# ── Model loading ─────────────────────────────────────────────────────────────

CHECKPOINTS_DIR = Path(__file__).parent / "checkpoints"
REGISTRY_FILE   = CHECKPOINTS_DIR / "registry.json"


def load_production_model(version: int | None = None) -> tuple[ABSAModel, AutoTokenizer, str]:
    """Load ABSAModel from local checkpoints (tracked in registry.json)."""
    import json

    if not REGISTRY_FILE.exists():
        raise FileNotFoundError(
            f"Registry not found: {REGISTRY_FILE}\n"
            "Run train_sentiment.py first."
        )

    with open(REGISTRY_FILE) as f:
        registry = json.load(f)

    if version:
        version_str = str(version)
    else:
        version_str = registry.get("production")
        if not version_str:
            raise RuntimeError("No production model in registry. Run train_sentiment.py first.")

    entry = registry["versions"].get(version_str)
    if not entry:
        raise RuntimeError(f"Version {version_str} not found in registry.")

    weights_path  = entry["weights_path"]
    config_path   = entry["config_path"]
    tokenizer_dir = entry["tokenizer_dir"]

    if not os.path.exists(weights_path):
        raise FileNotFoundError(f"Checkpoint not found: {weights_path}")

    log.info("Loading model v%s (acc=%.3f) from %s", version_str, entry.get("accuracy", 0), entry["checkpoint_dir"])
    model     = ABSAModel.load(weights_path, config_path)
    tokenizer = get_tokenizer(tokenizer_dir)

    model.eval()
    model.to(DEVICE)
    log.info("Model loaded on %s", DEVICE)
    return model, tokenizer, version_str


# ── Silver data loading ───────────────────────────────────────────────────────

def load_silver_reviews(incremental: bool = False) -> pd.DataFrame:
    catalog = get_catalog()
    log.info("Loading stg_order_reviews from Silver...")
    reviews = catalog.load_table("silver.stg_order_reviews").scan().to_pandas()
    log.info("  Total reviews: %d", len(reviews))

    if incremental:
        try:
            scored_ids = (
                catalog.load_table(GOLD_TABLE)
                .scan(selected_fields=("order_id",))
                .to_pandas()["order_id"]
                .tolist()
            )
            reviews = reviews[~reviews["order_id"].isin(scored_ids)]
            log.info("  Unscored reviews: %d (incremental mode)", len(reviews))
        except NoSuchTableError:
            log.info("  No existing Gold table — scoring all reviews")

    return reviews


# ── Inference ─────────────────────────────────────────────────────────────────

@torch.no_grad()
def run_inference(
    model: ABSAModel,
    tokenizer: AutoTokenizer,
    df: pd.DataFrame,
    version_str: str,
) -> pd.DataFrame:
    texts    = df["review_comment_message"].fillna("").tolist()
    order_ids  = df["order_id"].tolist()
    review_ids = df.get("review_id", pd.Series([None] * len(df))).tolist()
    scored_at  = datetime.now(tz=timezone.utc)

    results = []
    n_batches = (len(texts) + INFER_BATCH_SIZE - 1) // INFER_BATCH_SIZE

    for i in range(0, len(texts), INFER_BATCH_SIZE):
        batch_idx   = i // INFER_BATCH_SIZE + 1
        batch_texts = texts[i : i + INFER_BATCH_SIZE]

        if batch_idx % 20 == 0 or batch_idx == 1:
            log.info("  Batch %d/%d  (%d rows)", batch_idx, n_batches, len(batch_texts))

        enc = tokenizer(
            batch_texts,
            max_length=128,
            padding=True,
            truncation=True,
            return_tensors="pt",
        )
        ids  = enc["input_ids"].to(DEVICE)
        mask = enc["attention_mask"].to(DEVICE)

        logits_dict = model(ids, mask)
        probs_dict  = {h: torch.softmax(v, dim=-1).cpu().numpy() for h, v in logits_dict.items()}

        for j in range(len(batch_texts)):
            row = {
                "order_id":   order_ids[i + j],
                "review_id":  review_ids[i + j],
                "review_text": batch_texts[j],
            }
            for head in ALL_HEADS:
                probs = probs_dict[head][j]
                idx   = int(probs.argmax())
                row[f"{head}_sentiment"]  = LABEL_NAMES[idx]
                row[f"{head}_confidence"] = float(probs[idx])

            row["model_version"] = f"v{version_str}"
            row["scored_at"]     = scored_at
            results.append(row)

    pred_df = pd.DataFrame(results)
    log.info("Inference complete: %d reviews scored", len(pred_df))
    return pred_df


# ── Gold write ────────────────────────────────────────────────────────────────

def write_gold(df: pd.DataFrame, incremental: bool = False) -> None:
    catalog  = get_catalog()
    schema   = _gold_schema()
    df["scored_at"] = pd.to_datetime(df["scored_at"], utc=True)
    arrow_tbl = pa.Table.from_pandas(df, schema=schema, safe=False)

    try:
        existing = catalog.load_table(GOLD_TABLE)
        if incremental:
            existing.append(arrow_tbl)
            log.info("Appended %d rows to %s", len(df), GOLD_TABLE)
        else:
            catalog.drop_table(GOLD_TABLE)
            raise NoSuchTableError(GOLD_TABLE)
    except NoSuchTableError:
        tbl = catalog.create_table(
            identifier=GOLD_TABLE,
            schema=schema,
            location=GOLD_LOCATION,
        )
        tbl.append(arrow_tbl)
        log.info("Created %s with %d rows", GOLD_TABLE, len(df))


# ── Main ──────────────────────────────────────────────────────────────────────

def infer(version: int | None = None, incremental: bool = False) -> int:
    log.info("=== SENTIMENT INFERENCE START ===")
    log.info("Device: %s | incremental=%s", DEVICE, incremental)

    model, tokenizer, version_str = load_production_model(version)
    df_reviews = load_silver_reviews(incremental=incremental)

    if df_reviews.empty:
        log.info("No reviews to score. Exiting.")
        return 0

    df_preds = run_inference(model, tokenizer, df_reviews, version_str)
    write_gold(df_preds, incremental=incremental)

    log.info("=== SENTIMENT INFERENCE COMPLETE: %d rows written ===", len(df_preds))
    return len(df_preds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-version", type=int, default=None)
    parser.add_argument("--incremental",   action="store_true",
                        help="Only score reviews not yet in Gold")
    args = parser.parse_args()
    infer(version=args.model_version, incremental=args.incremental)
