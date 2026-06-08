"""
Customer Churn Prediction — XGBoost Binary Classifier
Reads:   olist.gold.dim_customers
Predicts: is_churned (1 = no purchase in 90 days)
Logs to: MLflow experiment "customer-churn-prediction"

Usage:
  python ml/training/train_churn_model.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import logging
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, f1_score, precision_score, recall_score, classification_report
)
from xgboost import XGBClassifier

from utils import load_gold_table, setup_mlflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

EXPERIMENT = "customer-churn-prediction"
MODEL_NAME = "customer-churn-xgb"

CAT_COLS = ["customer_state"]
NUM_COLS = ["days_since_last_order", "total_orders", "total_spend",
            "avg_order_value", "is_repeat_customer"]
TARGET   = "is_churned"

PARAMS = {
    "n_estimators":    150,
    "max_depth":       5,
    "learning_rate":   0.05,
    "subsample":       0.8,
    "colsample_bytree": 0.8,
    "scale_pos_weight": 3,   # churn thường imbalanced: reward recall trên minority
    "objective":       "binary:logistic",
    "eval_metric":     "auc",
    "random_state":    42,
    "n_jobs":          -1,
}


def prepare(df: pd.DataFrame):
    df = df[df["total_orders"] > 0].copy()

    for col in NUM_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df[CAT_COLS] = df[CAT_COLS].fillna("unknown").astype(str)
    df[TARGET]   = df[TARGET].fillna(0).astype(int)

    X = df[CAT_COLS + NUM_COLS]
    y = df[TARGET]
    churn_rate = y.mean()
    log.info("Dataset: %d customers  churn_rate=%.1f%%", len(X), churn_rate * 100)
    return X, y


def train() -> str:
    setup_mlflow()
    df = load_gold_table("olist.gold.dim_customers")
    X, y = prepare(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    preprocessor = ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT_COLS),
        ("num", "passthrough", NUM_COLS),
    ])
    pipeline = Pipeline([
        ("prep",  preprocessor),
        ("model", XGBClassifier(**PARAMS)),
    ])

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run() as run:
        mlflow.log_params({**PARAMS, "train_rows": len(X_train), "test_rows": len(X_test)})
        mlflow.log_param("features", CAT_COLS + NUM_COLS)

        pipeline.fit(X_train, y_train)
        y_pred  = pipeline.predict(X_test)
        y_proba = pipeline.predict_proba(X_test)[:, 1]

        auc       = float(roc_auc_score(y_test, y_proba))
        f1        = float(f1_score(y_test, y_pred))
        precision = float(precision_score(y_test, y_pred, zero_division=0))
        recall    = float(recall_score(y_test, y_pred))
        mlflow.log_metrics({"auc": auc, "f1": f1, "precision": precision, "recall": recall})

        # Classification report as artifact
        report = classification_report(y_test, y_pred, target_names=["active", "churned"])
        report_path = "/tmp/churn_classification_report.txt"
        with open(report_path, "w") as f:
            f.write(report)
        mlflow.log_artifact(report_path)

        # Feature importance
        xgb_model = pipeline.named_steps["model"]
        fi = pd.Series(
            xgb_model.feature_importances_,
            index=CAT_COLS + NUM_COLS,
        ).sort_values(ascending=False)
        fi_path = "/tmp/churn_feature_importance.csv"
        fi.to_csv(fi_path, header=["importance"])
        mlflow.log_artifact(fi_path)

        mlflow.sklearn.log_model(
            pipeline, "model",
            registered_model_name=MODEL_NAME,
            input_example=X_test.head(5),
        )

        log.info("Run %s  AUC=%.3f  F1=%.3f  Recall=%.3f", run.info.run_id, auc, f1, recall)
        return run.info.run_id


if __name__ == "__main__":
    train()
