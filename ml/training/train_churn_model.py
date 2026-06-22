"""
Customer Churn Prediction — XGBoost Binary Classifier
Reads:   Silver (stg_orders + stg_customers + stg_order_payments)
Predicts: is_churned (1 = no purchase in the 90-day churn window)
Logs to: MLflow experiment "customer-churn-prediction"

Usage:
  python ml/training/train_churn_model.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import logging

import mlflow
import mlflow.sklearn
import pandas as pd
from features import build_churn_features
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OrdinalEncoder
from utils import get_catalog, setup_mlflow
from xgboost import XGBClassifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

EXPERIMENT = "customer-churn-prediction"
MODEL_NAME = "customer-churn-xgb"

CAT_COLS = ["customer_state"]
NUM_COLS = [
    "total_orders",
    "total_spend",
    "avg_order_value",
    "is_repeat_customer",
    "orders_last_30d",
    "orders_last_90d",
    "orders_last_180d",
    "avg_days_between_orders",
    "days_since_last_order",   # recency BEFORE snapshot — not leaky
]
TARGET = "is_churned"

PARAMS = {
    "n_estimators":     200,
    "max_depth":        5,
    "learning_rate":    0.05,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "scale_pos_weight": 1,     # SMOTE balances training set
    "objective":        "binary:logistic",
    "eval_metric":      "aucpr",
    "random_state":     42,
    "n_jobs":           -1,
}


def train() -> str:
    setup_mlflow()
    catalog = get_catalog()
    df = build_churn_features(catalog)

    X = df[CAT_COLS + NUM_COLS]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    preprocessor = ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT_COLS),
        ("num", "passthrough", NUM_COLS),
    ])

    pipeline = Pipeline([
        ("prep",  preprocessor),
        ("smote", SMOTE(random_state=42, k_neighbors=5)),
        ("model", XGBClassifier(**PARAMS)),
    ])

    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run() as run:
        mlflow.log_params({
            **PARAMS,
            "smote": True,
            "train_rows": len(X_train),
            "test_rows":  len(X_test),
            "source":     "silver",
        })
        mlflow.log_param("features", CAT_COLS + NUM_COLS)

        pipeline.fit(X_train, y_train)
        y_pred  = pipeline.predict(X_test)
        y_proba = pipeline.predict_proba(X_test)[:, 1]

        auc    = float(roc_auc_score(y_test, y_proba))
        pr_auc = float(average_precision_score(y_test, y_proba))
        f1     = float(f1_score(y_test, y_pred, zero_division=0))
        prec   = float(precision_score(y_test, y_pred, zero_division=0))
        rec    = float(recall_score(y_test, y_pred, zero_division=0))
        mlflow.log_metrics({"auc": auc, "pr_auc": pr_auc,
                            "f1": f1, "precision": prec, "recall": rec})

        report = classification_report(y_test, y_pred, target_names=["active", "churned"])
        log.info("\n%s", report)
        report_path = "/tmp/churn_report.txt"
        with open(report_path, "w") as fh:
            fh.write(report)
        mlflow.log_artifact(report_path)

        xgb = pipeline.named_steps["model"]
        fi  = pd.Series(xgb.feature_importances_,
                        index=CAT_COLS + NUM_COLS).sort_values(ascending=False)
        fi_path = "/tmp/churn_feature_importance.csv"
        fi.to_csv(fi_path, header=["importance"])
        mlflow.log_artifact(fi_path)
        log.info("Feature importance:\n%s", fi.to_string())

        mlflow.sklearn.log_model(
            pipeline, "model",
            registered_model_name=MODEL_NAME,
            input_example=X_test.head(5),
        )
        log.info("Run %s  AUC=%.3f  PR-AUC=%.3f  F1=%.3f  Prec=%.3f  Recall=%.3f",
                 run.info.run_id, auc, pr_auc, f1, prec, rec)
        return run.info.run_id


if __name__ == "__main__":
    train()
