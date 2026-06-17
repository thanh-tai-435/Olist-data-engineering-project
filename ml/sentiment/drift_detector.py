"""
Sentiment Drift Detector — monitors distribution shift in review sentiments over time.

Metrics:
  - KL divergence between baseline and current overall_sentiment distribution
  - Chi-square test for statistical significance
  - Per-aspect drift scores
  - New unscored review count

Triggers:
  - RETRAIN_THRESHOLD: 500+ new unscored reviews
  - DRIFT_THRESHOLD:   KL divergence > 0.05 (≈5% distributional shift)

Usage:
  python ml/sentiment/drift_detector.py
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats
from pyiceberg.exceptions import NoSuchTableError

sys.path.insert(0, str(Path(__file__).parents[1]))
sys.path.insert(0, str(Path(__file__).parent))

from utils import get_catalog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

RETRAIN_THRESHOLD = int(os.environ.get("SENTIMENT_RETRAIN_THRESHOLD", "500"))
DRIFT_THRESHOLD   = float(os.environ.get("SENTIMENT_DRIFT_THRESHOLD",  "0.05"))
ASPECTS = ["product_quality", "delivery_speed", "seller_service", "price_value"]
SENTIMENTS = ["negative", "neutral", "positive"]


@dataclass
class DriftReport:
    new_unscored_count:   int
    kl_divergence_overall: float
    chi2_pvalue:          float
    per_aspect_kl:        dict[str, float]
    should_retrain:       bool
    should_infer:         bool
    reason:               str
    baseline_dist:        dict[str, float]
    current_dist:         dict[str, float]
    checked_at:           datetime


def _distribution(series: pd.Series) -> np.ndarray:
    """Normalised count vector [neg, neu, pos] with Laplace smoothing."""
    counts = np.array([
        (series == s).sum() + 1   # +1 Laplace smoothing
        for s in SENTIMENTS
    ], dtype=float)
    return counts / counts.sum()


def _kl_divergence(p: np.ndarray, q: np.ndarray) -> float:
    """KL(p || q) — small values mean distributions are similar."""
    p = np.clip(p, 1e-10, 1)
    q = np.clip(q, 1e-10, 1)
    return float(np.sum(p * np.log(p / q)))


def detect(lookback_days: int = 30) -> DriftReport:
    """
    Compare sentiment distribution of recent reviews vs the scoring baseline.

    lookback_days: compare last N days vs all-time baseline.
    """
    catalog = get_catalog()
    checked_at = datetime.now(tz=timezone.utc)

    # ── Load Gold sentiment table ─────────────────────────────────────────────
    try:
        gold_df = catalog.load_table("gold.review_sentiment").scan().to_pandas()
        log.info("Loaded gold.review_sentiment: %d rows", len(gold_df))
    except NoSuchTableError:
        log.warning("gold.review_sentiment not found — no inference run yet")
        return DriftReport(
            new_unscored_count=_count_unscored(catalog),
            kl_divergence_overall=0.0,
            chi2_pvalue=1.0,
            per_aspect_kl={a: 0.0 for a in ASPECTS},
            should_retrain=False,
            should_infer=True,
            reason="No Gold table yet — first inference needed",
            baseline_dist={s: 1 / 3 for s in SENTIMENTS},
            current_dist={s: 1 / 3 for s in SENTIMENTS},
            checked_at=checked_at,
        )

    # ── Baseline distribution (all-time) ─────────────────────────────────────
    baseline_dist = _distribution(gold_df["overall_sentiment"])
    baseline_dict = dict(zip(SENTIMENTS, baseline_dist.tolist()))

    # ── Recent distribution (last N days) ────────────────────────────────────
    cutoff = pd.Timestamp(checked_at - timedelta(days=lookback_days), tz="UTC")
    if "scored_at" in gold_df.columns:
        gold_df["scored_at"] = pd.to_datetime(gold_df["scored_at"], utc=True)
        recent_df = gold_df[gold_df["scored_at"] >= cutoff]
    else:
        recent_df = gold_df.tail(min(500, len(gold_df)))

    if len(recent_df) < 20:
        log.info("Too few recent rows (%d) for drift analysis", len(recent_df))
        recent_dist   = baseline_dist
        current_dict  = baseline_dict
        kl_overall    = 0.0
        chi2_pvalue   = 1.0
    else:
        recent_dist  = _distribution(recent_df["overall_sentiment"])
        current_dict = dict(zip(SENTIMENTS, recent_dist.tolist()))
        kl_overall   = _kl_divergence(recent_dist, baseline_dist)

        # Chi-square test
        baseline_counts = np.array([(gold_df["overall_sentiment"] == s).sum() for s in SENTIMENTS])
        recent_counts   = np.array([(recent_df["overall_sentiment"] == s).sum() for s in SENTIMENTS])
        expected        = baseline_counts / baseline_counts.sum() * recent_counts.sum()
        _, chi2_pvalue  = stats.chisquare(f_obs=recent_counts, f_exp=np.clip(expected, 1, None))

    # ── Per-aspect KL divergence ──────────────────────────────────────────────
    per_aspect_kl: dict[str, float] = {}
    for aspect in ASPECTS:
        col = f"{aspect}_sentiment"
        if col not in gold_df.columns:
            per_aspect_kl[aspect] = 0.0
            continue
        base  = _distribution(gold_df[col].dropna())
        recnt = _distribution(recent_df[col].dropna()) if len(recent_df) >= 20 else base
        per_aspect_kl[aspect] = _kl_divergence(recnt, base)

    # ── New unscored reviews ──────────────────────────────────────────────────
    new_unscored = _count_unscored(catalog, already_scored_ids=set(gold_df["order_id"].tolist()))

    # ── Decision logic ────────────────────────────────────────────────────────
    drift_detected    = kl_overall > DRIFT_THRESHOLD
    enough_new        = new_unscored >= RETRAIN_THRESHOLD
    should_retrain    = drift_detected or enough_new
    should_infer      = new_unscored > 0

    reasons = []
    if drift_detected:
        reasons.append(f"KL={kl_overall:.4f} > threshold={DRIFT_THRESHOLD}")
    if enough_new:
        reasons.append(f"{new_unscored} new reviews >= retrain threshold={RETRAIN_THRESHOLD}")
    if not reasons:
        reasons = ["No drift detected, no new reviews above threshold"]

    reason = " | ".join(reasons)

    report = DriftReport(
        new_unscored_count=new_unscored,
        kl_divergence_overall=kl_overall,
        chi2_pvalue=chi2_pvalue,
        per_aspect_kl=per_aspect_kl,
        should_retrain=should_retrain,
        should_infer=should_infer,
        reason=reason,
        baseline_dist=baseline_dict,
        current_dist=current_dict,
        checked_at=checked_at,
    )

    log.info(
        "Drift report: KL=%.4f  chi2_pvalue=%.3f  new_unscored=%d  "
        "should_retrain=%s  should_infer=%s",
        kl_overall, chi2_pvalue, new_unscored, should_retrain, should_infer,
    )
    log.info("Reason: %s", reason)
    return report


def _count_unscored(catalog, already_scored_ids: set | None = None) -> int:
    """Count Silver reviews not yet in Gold."""
    try:
        silver_df = (
            catalog.load_table("silver.stg_order_reviews")
            .scan(selected_fields=("order_id",))
            .to_pandas()
        )
        silver_ids = set(silver_df["order_id"].tolist())
    except NoSuchTableError:
        return 0

    if already_scored_ids is None:
        try:
            gold_df = (
                catalog.load_table("gold.review_sentiment")
                .scan(selected_fields=("order_id",))
                .to_pandas()
            )
            already_scored_ids = set(gold_df["order_id"].tolist())
        except NoSuchTableError:
            already_scored_ids = set()

    return len(silver_ids - already_scored_ids)


if __name__ == "__main__":
    report = detect()
    print(f"\n{'='*50}")
    print(f"Drift Report — {report.checked_at.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}")
    print(f"  KL divergence (overall):  {report.kl_divergence_overall:.4f}  (threshold={DRIFT_THRESHOLD})")
    print(f"  Chi-square p-value:       {report.chi2_pvalue:.4f}")
    print(f"  New unscored reviews:     {report.new_unscored_count}  (threshold={RETRAIN_THRESHOLD})")
    print(f"  Per-aspect KL:")
    for aspect, kl in report.per_aspect_kl.items():
        print(f"    {aspect:25s}  {kl:.4f}")
    print(f"  Baseline distribution:  {report.baseline_dist}")
    print(f"  Current distribution:   {report.current_dist}")
    print(f"  → Should retrain: {report.should_retrain}")
    print(f"  → Should infer:   {report.should_infer}")
    print(f"  Reason: {report.reason}")
