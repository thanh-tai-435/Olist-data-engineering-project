"""
Prefect flow: train all 3 P0 ML models and register to MLflow.

Flows:
  train_delivery_flow   — XGBoost regression: delivery delay
  train_churn_flow      — XGBoost classifier: customer churn
  train_lead_flow       — XGBoost classifier: lead scoring
  ml_training_flow      — runs all 3 in parallel (faster than sequential)

Usage:
  python prefect/flows/ml_training.py               # one-shot full training
  python prefect/flows/ml_training.py --serve       # weekly Sunday 04:00 UTC schedule
"""
import sys
import os
import argparse
sys.path.insert(0, "/app/ml/training")

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from notifications import notify_failure


@task(name="train-delivery-model", retries=1, retry_delay_seconds=30)
def train_delivery_task() -> str:
    from train_delivery_model import train
    return train()


@task(name="train-churn-model", retries=1, retry_delay_seconds=30)
def train_churn_task() -> str:
    from train_churn_model import train
    return train()


@task(name="train-lead-scoring", retries=1, retry_delay_seconds=30)
def train_lead_task() -> str:
    from train_lead_scoring import train
    return train()


@flow(
    name="ml-training-pipeline",
    description="Train delivery-delay, churn, and lead-scoring models in parallel.",
    log_prints=True,
    on_failure=[notify_failure],
)
def ml_training_flow() -> dict:
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    # Submit all 3 training jobs in parallel
    delivery_fut = train_delivery_task.submit()
    churn_fut    = train_churn_task.submit()
    lead_fut     = train_lead_task.submit()

    delivery_run = delivery_fut.result()
    churn_run    = churn_fut.result()
    lead_run     = lead_fut.result()

    summary = {
        "delivery_run_id": delivery_run,
        "churn_run_id":    churn_run,
        "lead_run_id":     lead_run,
    }

    create_markdown_artifact(
        key="ml-training-summary",
        markdown=f"""# ML Training Complete

| Model | Experiment | Run ID |
|---|---|---|
| Delivery Delay | delivery-delay-prediction | `{delivery_run}` |
| Customer Churn | customer-churn-prediction | `{churn_run}` |
| Lead Scoring   | lead-scoring              | `{lead_run}` |

View results at [{tracking_uri}]({tracking_uri})
""",
        description="MLflow run IDs for all 3 P0 models",
    )
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run or serve the ML Training pipeline.")
    parser.add_argument("--serve", action="store_true",
                        help="Serve with weekly Sunday 04:00 UTC schedule")
    args = parser.parse_args()

    if args.serve:
        ml_training_flow.serve(
            name="weekly-ml-training",
            cron="0 4 * * 0",
        )
    else:
        ml_training_flow()
