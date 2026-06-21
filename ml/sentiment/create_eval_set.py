"""
ABSA Eval Set — Groq LLM as silver-standard annotator.

Two modes:
  --label (default) : call Groq to label N reviews → save to eval_set.csv
  --eval            : load trained BERTimbau, run inference, print F1/accuracy vs Groq labels

Usage:
  python ml/sentiment/create_eval_set.py              # label 300 reviews (~10 min)
  python ml/sentiment/create_eval_set.py --n 100      # label 100 reviews (~3.5 min)
  python ml/sentiment/create_eval_set.py --eval       # eval trained model on existing eval_set.csv
  python ml/sentiment/create_eval_set.py --no-label --eval  # eval only

Rate limit: Groq free tier = 30 RPM → sleeps 2.1s between requests.
Resume-safe: saves every 10 rows, skips already-labeled order_ids.
"""
import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1]))
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

EVAL_SET_PATH  = Path(__file__).parent / "eval_set.csv"
ASPECTS        = ["product_quality", "delivery_speed", "seller_service", "price_value"]
SENTIMENTS     = ["negative", "neutral", "positive"]
GROQ_MODEL     = "llama-3.3-70b-versatile"
RATE_SLEEP     = 2.1   # 30 RPM limit → 2s gap is safe

_LABEL_PROMPT = """\
You are an expert sentiment analyst for Brazilian e-commerce reviews.

Analyze this Portuguese review and return ONLY a JSON object — no explanation, no markdown.

Review text: "{text}"
Star rating: {score}/5

Return exactly this JSON structure:
{{
  "overall": "negative" | "neutral" | "positive",
  "product_quality": "negative" | "neutral" | "positive" | "not_mentioned",
  "delivery_speed":  "negative" | "neutral" | "positive" | "not_mentioned",
  "seller_service":  "negative" | "neutral" | "positive" | "not_mentioned",
  "price_value":     "negative" | "neutral" | "positive" | "not_mentioned"
}}

Rules:
- overall: combine text tone AND star rating
- Each aspect: label only if the review explicitly discusses it; otherwise "not_mentioned"
"""


# ── JSON helpers ──────────────────────────────────────────────────────────────

def _parse_json(text: str) -> dict:
    text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`")
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object in Groq response")
    obj, _ = json.JSONDecoder().raw_decode(text, start)
    return obj


def _validate(labels: dict) -> dict:
    valid_overall = set(SENTIMENTS)
    valid_aspect  = set(SENTIMENTS) | {"not_mentioned"}
    if labels.get("overall") not in valid_overall:
        labels["overall"] = "neutral"
    for a in ASPECTS:
        if labels.get(a) not in valid_aspect:
            labels[a] = "not_mentioned"
    return labels


def _score_to_label(score: int) -> str:
    if score <= 2:
        return "negative"
    if score == 3:
        return "neutral"
    return "positive"


# ── Data loading ──────────────────────────────────────────────────────────────

def load_reviews(n: int) -> pd.DataFrame:
    """Load reviews from Silver Iceberg; fall back to raw CSV if unavailable."""
    df = None
    try:
        from utils import get_catalog
        catalog = get_catalog()
        df = catalog.load_table("silver.stg_order_reviews").scan().to_pandas()
        log.info("Silver Iceberg: %d reviews loaded", len(df))
    except Exception as e:
        log.warning("Silver Iceberg unavailable (%s) — trying raw CSV...", e)

    if df is None:
        data_dir = Path(__file__).parents[2] / "data" / "raw" / "ecommerce"
        csv_path = data_dir / "olist_order_reviews_dataset.csv"
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Neither Silver Iceberg nor {csv_path} is available.\n"
                "Run Bronze ingestion + Silver transform first, or place the raw CSV."
            )
        df = pd.read_csv(csv_path)
        log.info("Raw CSV fallback: %d reviews loaded", len(df))

    # Keep only reviews with text
    df = df[
        df["review_comment_message"].notna() &
        (df["review_comment_message"].str.strip() != "")
    ].copy()
    log.info("Reviews with text: %d", len(df))

    # Stratified sample — roughly equal per star rating for a balanced eval set
    if len(df) > n:
        per_score = max(1, n // 5)
        parts = [
            df[df["review_score"] == s].sample(n=min(per_score, (df["review_score"] == s).sum()), random_state=42)
            for s in range(1, 6)
        ]
        df = pd.concat(parts).sample(frac=1, random_state=42).head(n)
        log.info("Stratified sample: %d reviews (%d per star)", len(df), per_score)

    cols = [c for c in ["order_id", "review_id", "review_comment_message", "review_score"] if c in df.columns]
    return df[cols].reset_index(drop=True)


# ── Groq labeling ─────────────────────────────────────────────────────────────

def label_with_groq(df: pd.DataFrame) -> pd.DataFrame:
    """Call Groq to label each review for ABSA. Saves every 10 rows for resume."""
    from groq import Groq

    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        raise EnvironmentError("GROQ_API_KEY is not set")

    client = Groq(api_key=groq_key)

    # Resume: skip already-labeled order_ids
    existing_rows: list[dict] = []
    already_labeled: set = set()
    if EVAL_SET_PATH.exists():
        existing_df = pd.read_csv(EVAL_SET_PATH)
        already_labeled = set(existing_df["order_id"].tolist())
        existing_rows   = existing_df.to_dict("records")
        log.info("Resuming: %d already labeled, %d remaining",
                 len(already_labeled), len(df) - len(already_labeled))

    to_label = df[~df["order_id"].isin(already_labeled)]
    new_rows: list[dict] = []
    total = len(to_label)
    log.info("Will label %d reviews — estimated time: ~%.0f min", total, total * RATE_SLEEP / 60)

    for i, (_, row) in enumerate(to_label.iterrows()):
        text  = str(row["review_comment_message"])[:800]   # truncate very long reviews
        score = int(row.get("review_score", 3))

        try:
            resp = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": _LABEL_PROMPT.format(text=text, score=score)}],
                temperature=0.0,
                max_tokens=200,
            )
            labels = _validate(_parse_json(resp.choices[0].message.content))
        except Exception as e:
            log.warning("Row %d (%s) failed: %s — using score-based fallback", i, row["order_id"], e)
            labels = {"overall": _score_to_label(score), **{a: "not_mentioned" for a in ASPECTS}}

        new_rows.append({
            "order_id":    row["order_id"],
            "review_id":   row.get("review_id", ""),
            "review_text": str(row["review_comment_message"]),
            "review_score": score,
            "overall":     labels["overall"],
            **{a: labels[a] for a in ASPECTS},
            "labeled_by":  GROQ_MODEL,
            "labeled_at":  datetime.now(tz=timezone.utc).isoformat(),
        })

        # Save checkpoint every 10 rows so progress isn't lost
        if (i + 1) % 10 == 0:
            pd.DataFrame(existing_rows + new_rows).to_csv(EVAL_SET_PATH, index=False)
            log.info("  Checkpoint saved: %d/%d labeled", i + 1, total)

        if i < total - 1:
            time.sleep(RATE_SLEEP)

    result_df = pd.DataFrame(existing_rows + new_rows)
    result_df.to_csv(EVAL_SET_PATH, index=False)
    log.info("Done. %d labeled reviews saved to %s", len(result_df), EVAL_SET_PATH)
    return result_df


# ── BERTimbau evaluation ──────────────────────────────────────────────────────

def evaluate_model(eval_df: pd.DataFrame) -> None:
    """Load trained BERTimbau and compute F1/accuracy vs Groq labels."""
    import torch
    from infer_sentiment import DEVICE, load_production_model
    from model import ALL_HEADS, LABEL_NAMES
    from sklearn.metrics import accuracy_score, classification_report, f1_score

    try:
        model, tokenizer, version_str = load_production_model()
    except (FileNotFoundError, RuntimeError) as e:
        log.error("Model not available: %s", e)
        log.error("Run train_sentiment.py first, then re-run with --eval.")
        return

    log.info("Running inference on %d eval reviews (device=%s)...", len(eval_df), DEVICE)

    texts = eval_df["review_text"].fillna("").tolist()
    preds: dict[str, list] = {h: [] for h in ALL_HEADS}

    model.eval()
    with torch.no_grad():
        for i in range(0, len(texts), 32):
            batch = texts[i : i + 32]
            enc = tokenizer(batch, max_length=128, padding=True, truncation=True, return_tensors="pt")
            logits = model(enc["input_ids"].to(DEVICE), enc["attention_mask"].to(DEVICE))
            for h in ALL_HEADS:
                preds[h].extend(LABEL_NAMES[idx] for idx in logits[h].argmax(dim=-1).cpu().tolist())

    for h in ALL_HEADS:
        eval_df[f"pred_{h}"] = preds[h]

    # ── Print report ──────────────────────────────────────────────────────────
    sep = "=" * 62
    print(f"\n{sep}")
    print(f"  BERTimbau v{version_str}  vs  Groq {GROQ_MODEL}")
    print(f"  Eval set: {len(eval_df)} reviews")
    print(sep)

    # Overall sentiment
    print("\n── Overall Sentiment ─────────────────────────────────────")
    print(classification_report(
        eval_df["overall"], eval_df["pred_overall"],
        labels=SENTIMENTS, zero_division=0,
    ))

    # Per-aspect F1 (exclude rows where Groq labeled "not_mentioned")
    print("── Per-Aspect F1 (not_mentioned rows excluded) ───────────")
    print(f"  {'Aspect':<25} {'F1-macro':>10}  {'Accuracy':>10}  {'n':>6}")
    print("  " + "-" * 55)
    for aspect in ASPECTS:
        mask   = eval_df[aspect] != "not_mentioned"
        n_eval = mask.sum()
        if n_eval < 5:
            print(f"  {aspect:<25} {'(< 5 labeled)':>10}")
            continue
        y_true = eval_df.loc[mask, aspect]
        y_pred = eval_df.loc[mask, f"pred_{aspect}"]
        f1  = f1_score(y_true, y_pred, labels=SENTIMENTS, average="macro", zero_division=0)
        acc = accuracy_score(y_true, y_pred)
        print(f"  {aspect:<25} {f1:>10.3f}  {acc:>10.1%}  {n_eval:>6,}")

    # Aspect coverage from Groq labels
    print("\n── Aspect Mention Coverage (Groq labels) ─────────────────")
    for aspect in ASPECTS:
        cov = (eval_df[aspect] != "not_mentioned").mean()
        print(f"  {aspect:<25} {cov:.1%}")
    print(sep + "\n")

    # Save predictions alongside labels for manual inspection
    out_path = EVAL_SET_PATH.with_name("eval_results.csv")
    eval_df.to_csv(out_path, index=False)
    log.info("Full results saved to %s", out_path)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create ABSA eval set via Groq labeling and/or evaluate BERTimbau."
    )
    parser.add_argument("--n",        type=int, default=300,
                        help="Number of reviews to label (default: 300)")
    parser.add_argument("--eval",     action="store_true",
                        help="Evaluate trained BERTimbau on eval_set.csv after labeling")
    parser.add_argument("--no-label", action="store_true",
                        help="Skip Groq labeling, only run --eval on existing eval_set.csv")
    args = parser.parse_args()

    run_label = not args.no_label
    eval_df   = None

    if run_label:
        log.info("=== Step 1/2: Load %d reviews ===", args.n)
        df      = load_reviews(args.n)
        log.info("=== Step 2/2: Label with Groq (%s) ===", GROQ_MODEL)
        eval_df = label_with_groq(df)

    if args.eval:
        if eval_df is None:
            if not EVAL_SET_PATH.exists():
                log.error("eval_set.csv not found — run labeling first (omit --no-label).")
                return
            eval_df = pd.read_csv(EVAL_SET_PATH)
            log.info("Loaded eval_set.csv: %d rows", len(eval_df))
        evaluate_model(eval_df)


if __name__ == "__main__":
    # Auto-load .env for local runs outside Docker
    _env = Path(__file__).parents[2] / ".env"
    if _env.exists():
        for _line in _env.read_text(encoding="utf-8", errors="ignore").splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

    main()
