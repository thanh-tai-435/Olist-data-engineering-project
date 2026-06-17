"""
Prefect Flow: Sentiment Pipeline — drift detection + conditional retrain + inference.

Runs on the HOST machine (not in the prefect-worker container) because:
  - Training requires GPU (RTX 4060)
  - PyTorch/transformers are not installed in the worker image

Usage:
  # Run once manually:
  python prefect/flows/sentiment_flow.py

  # Force retrain + full inference:
  python prefect/flows/sentiment_flow.py --force-retrain

  # Only inference (skip retrain):
  python prefect/flows/sentiment_flow.py --skip-retrain

  # Incremental inference (new reviews only):
  python prefect/flows/sentiment_flow.py --incremental

Decision logic:
  drift_detector → should_retrain? → train_sentiment → infer_sentiment
  drift_detector → should_infer?   → infer_sentiment (skip retrain)
"""
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from notifications import notify_failure

sys.path.insert(0, str(Path(__file__).parents[2] / "ml"))
sys.path.insert(0, str(Path(__file__).parents[2] / "ml" / "sentiment"))

logging.basicConfig(level=logging.INFO)


# ── Tasks ─────────────────────────────────────────────────────────────────────

@task(name="sentiment-drift-check", retries=1, retry_delay_seconds=30)
def drift_check_task(lookback_days: int = 30) -> dict:
    from drift_detector import detect
    log = get_run_logger()
    report = detect(lookback_days=lookback_days)
    log.info(
        "Drift check: KL=%.4f  new_unscored=%d  should_retrain=%s",
        report.kl_divergence_overall, report.new_unscored_count, report.should_retrain,
    )
    return {
        "kl_divergence":     report.kl_divergence_overall,
        "chi2_pvalue":       report.chi2_pvalue,
        "new_unscored":      report.new_unscored_count,
        "should_retrain":    report.should_retrain,
        "should_infer":      report.should_infer,
        "reason":            report.reason,
        "per_aspect_kl":     report.per_aspect_kl,
        "baseline_dist":     report.baseline_dist,
        "current_dist":      report.current_dist,
    }


@task(name="sentiment-train", retries=0, timeout_seconds=3600)
def train_task() -> str:
    from train_sentiment import train
    log = get_run_logger()
    log.info("Starting ABSA training...")
    run_id = train()
    log.info("Training complete. MLflow run: %s", run_id)
    return run_id


@task(name="sentiment-infer", retries=1, retry_delay_seconds=60)
def infer_task(incremental: bool = False) -> int:
    from infer_sentiment import infer
    log = get_run_logger()
    log.info("Starting batch inference (incremental=%s)...", incremental)
    n_scored = infer(incremental=incremental)
    log.info("Inference complete: %d reviews scored", n_scored)
    return n_scored


@task(name="sentiment-publish-report")
def publish_report_task(
    drift: dict,
    n_scored: int,
    did_retrain: bool,
    train_run_id: str | None,
) -> None:
    """Write a Prefect Artifact summarising the sentiment pipeline run."""
    kl    = drift["kl_divergence"]
    emoji = "🔄" if did_retrain else "✅"

    aspect_rows = "\n".join(
        f"| {aspect:25s} | {kl_val:.4f} |"
        for aspect, kl_val in drift.get("per_aspect_kl", {}).items()
    )
    baseline_row = " | ".join(f"{k}: {v:.1%}" for k, v in drift["baseline_dist"].items())
    current_row  = " | ".join(f"{k}: {v:.1%}" for k, v in drift["current_dist"].items())

    md = f"""## {emoji} Sentiment Pipeline Report

**Run time**: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

### Drift Analysis
| Metric | Value |
|--------|-------|
| KL Divergence (overall) | `{kl:.4f}` |
| Chi-square p-value | `{drift['chi2_pvalue']:.4f}` |
| New unscored reviews | `{drift['new_unscored']}` |
| Trigger reason | {drift['reason']} |

### Per-Aspect KL Divergence
| Aspect | KL |
|--------|----|
{aspect_rows}

### Distribution Comparison
- **Baseline**: {baseline_row}
- **Recent**:   {current_row}

### Actions Taken
- **Retrained**: {'Yes — MLflow run `' + str(train_run_id) + '`' if did_retrain else 'No'}
- **Reviews scored**: `{n_scored}`
"""
    create_markdown_artifact(
        key="sentiment-pipeline",
        markdown=md,
        description="Sentiment pipeline drift + retrain + inference report",
    )


# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(
    name="sentiment-pipeline",
    description=(
        "Drift detection → conditional BERTimbau retrain → batch ABSA inference. "
        "Runs on host GPU. Writes gold.review_sentiment to Iceberg."
    ),
    log_prints=True,
    on_failure=[notify_failure],
)
def sentiment_pipeline_flow(
    force_retrain: bool = False,
    skip_retrain:  bool = False,
    incremental:   bool = False,
    lookback_days: int  = 30,
) -> dict:
    log = get_run_logger()
    log.info("=== SENTIMENT PIPELINE START ===")
    log.info("force_retrain=%s  skip_retrain=%s  incremental=%s", force_retrain, skip_retrain, incremental)

    # Step 1: Drift detection
    drift = drift_check_task(lookback_days=lookback_days)

    # Step 2: Conditional retrain
    did_retrain  = False
    train_run_id = None
    if not skip_retrain and (force_retrain or drift["should_retrain"]):
        log.info("Step 2: Retraining model (reason: %s)", drift["reason"])
        train_run_id = train_task()
        did_retrain  = True
    else:
        log.info("Step 2: Skipping retrain (should_retrain=%s, skip_retrain=%s)", drift["should_retrain"], skip_retrain)

    # Step 3: Inference
    n_scored = 0
    if drift["should_infer"] or force_retrain or did_retrain:
        log.info("Step 3: Running batch inference...")
        n_scored = infer_task(incremental=incremental)
    else:
        log.info("Step 3: Skipping inference (no new reviews)")

    # Step 4: Publish report artifact
    publish_report_task(drift, n_scored, did_retrain, train_run_id)

    log.info("=== SENTIMENT PIPELINE COMPLETE ===")
    return {
        "did_retrain":  did_retrain,
        "train_run_id": train_run_id,
        "n_scored":     n_scored,
        "kl":           drift["kl_divergence"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run or serve the Sentiment Pipeline flow")
    parser.add_argument("--serve",         action="store_true",
                        help="Serve with weekly Sunday 03:00 UTC schedule")
    parser.add_argument("--force-retrain", action="store_true",
                        help="Force retrain regardless of drift")
    parser.add_argument("--skip-retrain",  action="store_true",
                        help="Skip retrain, only run inference")
    parser.add_argument("--incremental",   action="store_true",
                        help="Only score reviews not yet in Gold")
    parser.add_argument("--lookback-days", type=int, default=30,
                        help="Days to look back for drift detection")
    args = parser.parse_args()

    if args.serve:
        sentiment_pipeline_flow.serve(
            name="weekly-sentiment",
            cron="0 3 * * 0",
            parameters={"incremental": True, "lookback_days": 30},
        )
    else:
        result = sentiment_pipeline_flow(
            force_retrain=args.force_retrain,
            skip_retrain=args.skip_retrain,
            incremental=args.incremental,
            lookback_days=args.lookback_days,
        )
        print(f"\nResult: {result}")
