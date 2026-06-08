"""
Prefect flow: train all 3 P0 ML models and register to MLflow.

Flows:
  train_delivery_flow   — XGBoost regression: delivery delay
  train_churn_flow      — XGBoost classifier: customer churn
  train_lead_flow       — XGBoost classifier: lead scoring
  ml_training_flow      — runs all 3 sequentially

Usage:
  prefect run prefect/flows/ml_training.py               # full
  prefect run prefect/flows/ml_training.py --name delivery  # single
"""
import sys
import os
sys.path.insert(0, "/app/ml/training")

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact


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


@flow(name="ml-training-pipeline", log_prints=True)
def ml_training_flow() -> dict:
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

    delivery_run = train_delivery_task()
    churn_run    = train_churn_task()
    lead_run     = train_lead_task()

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
    ml_training_flow()
