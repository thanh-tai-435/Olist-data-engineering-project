"""
Serve all Olist Prefect flows with their production schedules in one process.

Usage:
    export PREFECT_API_URL=http://localhost:4200/api
    python prefect/deployments/serve_all.py

Ctrl-C to stop. All flow runs will continue queued in the Prefect server.

Schedules (UTC):
    daily-medallion      02:00 daily    Bronze → Silver → Gold + quality gates
    weekly-sentiment     03:00 Sunday   ABSA drift check + retrain + inference
    weekly-ml-training   04:00 Sunday   delivery-delay / churn / lead models
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "flows"))

from full_pipeline import full_pipeline_flow
from ml_training import ml_training_flow
from sentiment_flow import sentiment_pipeline_flow

from prefect import serve

if __name__ == "__main__":
    print("Registering deployments and starting workers…")
    serve(
        full_pipeline_flow.to_deployment(
            name="daily-medallion",
            cron="0 2 * * *",
            parameters={
                "skip_quality":          False,
                "pause_on_quality_fail": True,
            },
            description="Daily: Bronze → Silver → Gold with Soda quality gates.",
            tags=["medallion", "daily"],
        ),
        sentiment_pipeline_flow.to_deployment(
            name="weekly-sentiment",
            cron="0 3 * * 0",
            parameters={"incremental": True, "lookback_days": 30},
            description="Weekly: ABSA drift check → conditional retrain → batch inference.",
            tags=["ml", "sentiment", "weekly"],
        ),
        ml_training_flow.to_deployment(
            name="weekly-ml-training",
            cron="0 4 * * 0",
            description="Weekly: retrain delivery-delay, churn, and lead-scoring models.",
            tags=["ml", "training", "weekly"],
        ),
    )
